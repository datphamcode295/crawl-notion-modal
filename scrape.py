from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import boto3
from botocore.exceptions import ClientError
import modal
from typing import Union
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from typing import Dict, Any, List
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

image = modal.Image.debian_slim().pip_install("boto3", "pandas", "pyarrow", "requests", "fastapi", "futures")
web_app = FastAPI()
app = modal.App(name="has-simple-web-endpoint", image=image)
vol = modal.Volume.from_name("output", create_if_missing=True)

@web_app.post("/api/upload")
async def uploadToVolumn(request: Request):
    body = await request.json()
    return process_page_data.remote(
        markdown_data=body['data'],
        url=body['url'],
        workspaceName=body['workspaceName']
    )

@app.function(volumes={"/output": vol}, secrets=[modal.Secret.from_name("s3-credential")])
def process_page_data(
    markdown_data: str,
    url: str,
    workspaceName: str,
) -> Dict[str, Any]:
    """Process markdown data and manage parquet file"""
    try:
        # Extract title and id from url
        path = url.split('/')[-1]
        title, id = '-'.join(path.split('-')[:-1]), path.split('-')[-1]
        
        # Create filename
        filename = get_file_name.remote(org_name=workspaceName)
        file_path = f"/output/{filename}"
        
        # Create new data row
        from datetime import datetime
        new_data = {
            'id': [id],
            'org_name': [workspaceName],
            'title': [title],
            'data': [markdown_data],
            'updated_at': [datetime.now().isoformat()],
            'url': [url]
        }
        
        # Check if file exists
        if os.path.exists(file_path):
            # Read existing parquet file
            existing_df = pd.read_parquet(file_path)
            
            # Check if id exists
            if id in existing_df['id'].values:
                # Update existing row
                existing_df.loc[existing_df['id'] == id, ['data', 'updated_at']] = [
                    markdown_data,
                    datetime.now().isoformat()
                ]
                final_df = existing_df
            else:
                # Append new row
                new_df = pd.DataFrame(new_data)
                final_df = pd.concat([existing_df, new_df], ignore_index=True)           
        else:
            # Create new DataFrame with the data
            final_df = pd.DataFrame(new_data)
        
        # Convert to parquet and save
        table = pa.Table.from_pandas(final_df)
        pq.write_table(
            table,
            file_path,
            compression='SNAPPY',
            use_dictionary=True,
            version='2.6'
        )
        
        vol.commit()
        
        # upload file to s3 if the number of row over 5 using upload_to_s3_api and then remove the file
        if len(final_df) > 50:
            # Convert to PyArrow Table
            table = pa.Table.from_pandas(final_df)
            
            # Write to bytes buffer
            buf = io.BytesIO()
            pq.write_table(
                table,
                buf,
                compression='SNAPPY',
                use_dictionary=True,
                version='2.6'
            )
            
            # Get bytes and upload
            parquet_bytes = buf.getvalue()
            buf.close()
            upload_to_s3_api.remote(
                parquet_bytes=parquet_bytes,
                file_name=filename,
                bucket=os.environ["BUCKET"],
                base_path=os.environ["BASE_PATH"],
                api_token=os.environ["API_TOKEN"],
                api_url=os.environ["API_URL"]
            )
            os.remove(file_path)
            print(f"Remove filee {file_path}")
            vol.commit()    
            return {
                "success": True,
                "message": "Data processed successfully",
                "filename": filename,
                "rows": len(final_df)
            }   
        
        return {
            "success": True,
            "message": "Data processed successfully",
            "filename": filename,
            "rows": len(final_df)
        }
        
    except Exception as e:
        return {"error": f"Error processing markdown data: {str(e)}"}
        

@app.function(volumes={"/output": vol}, secrets=[modal.Secret.from_name("s3-credential")])
def get_file_name(
    org_name: str,
) -> str:
    """Get filename for organization data"""
    try:
        # Check files in volume
        volume_path = "/output"
        org_prefix = org_name.lower().replace(' ', '_')
        
        existing_files = [f for f in os.listdir(volume_path) 
                         if f.startswith(org_prefix)]
        
        if existing_files:
            return existing_files[0]
        # If no files found, call S3 API
        api_url = os.environ["GET_FILE_URL"]
        headers = {
            "Authorization": f"Bearer {os.environ['GET_FILE_API_TOKEN']}"
        }
        params = {
            "bucket": os.environ["BUCKET"],
            "prefix": f"data/uploads/2024/{org_prefix}_chunk_"
        }
        
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        total_files = data.get("total_files", 0)
        
        # Create new filename with chunk number
        new_filename = f"{org_prefix}_chunk_{total_files + 1}.parquet"
        return new_filename
        
    except Exception as e:
        print(f"Error getting filename: {str(e)}")
        # Return default filename if error occurs
        return f"{org_prefix}_chunk_1.parquet"
    
# Schedule function to upload all files to S3 every Monday at 8am and timeout after 3000 seconds
@app.function(volumes={"/output": vol}, secrets=[modal.Secret.from_name("s3-credential")], schedule=modal.Cron("0 8 * * 1"), timeout=3000)
def send_all_file_to_s3() -> Dict[str, Any]:
    """
    Upload all parquet files from output volume to S3 in parallel
    
    Implements parallel file upload with error handling
    
    Returns:
        Dict containing upload results with success/error status
    """
    try:
        # Get environment variables
        api_url = os.environ["API_URL"]
        api_token = os.environ["API_TOKEN"]
        base_path = os.environ["BASE_PATH"]
        bucket = os.environ["BUCKET"]
        
        # Determine max workers based on CPU cores or set a reasonable default
        max_workers = 10
        
        volume_path = "/output"
        
        # List all parquet files
        files = [f for f in os.listdir(volume_path) if f.endswith('.parquet')]
        print(f"Found {len(files)} files to upload: {files}")
        
        results = []
        
        def upload_single_file(file_name):
            """
            Helper function to upload a single file with detailed error handling
            """
            try:
                file_path = os.path.join(volume_path, file_name)
                
                # Read file into bytes
                with open(file_path, 'rb') as f:
                    parquet_bytes = f.read()
                
                # Upload to S3
                result = upload_to_s3_api.remote(
                    parquet_bytes=parquet_bytes,
                    file_name=file_name,
                    bucket=bucket,
                    base_path=base_path,
                    api_token=api_token,
                    api_url=api_url
                )
                
                return {
                    "file": file_name,
                    "status": "success",
                    "result": result
                }
            
            except Exception as e:
                return {
                    "file": file_name,
                    "status": "error",
                    "error": str(e)
                }
        
        # Use ThreadPoolExecutor for parallel uploads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit upload tasks for all files
            future_to_file = {
                executor.submit(upload_single_file, file_name): file_name 
                for file_name in files
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    results.append({
                        "file": file_name,
                        "status": "unexpected_error",
                        "error": str(exc)
                    })
        
        # Analyze overall results
        successful_uploads = [r for r in results if r['status'] == 'success']
        failed_uploads = [r for r in results if r['status'] != 'success']
        
        print(f"Upload complete. Successful: {len(successful_uploads)}, Failed: {len(failed_uploads)}")
        
        return {
            "success": len(failed_uploads) == 0,
            "total_files": len(files),
            "successful_uploads": len(successful_uploads),
            "failed_uploads": len(failed_uploads),
            "results": results
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Critical error processing files: {str(e)}"
        }

@app.function(secrets=[modal.Secret.from_name("s3-credential")])
def upload_to_s3_api(
    parquet_bytes: bytes,
    file_name: str,
    bucket: str,
    base_path: str,
    api_token: str,
    api_url: str
) -> Dict[str, Any]:
    """
    Upload parquet bytes to S3 using the provided API
    """
    print(f"Uploading {file_name} to S3")
    try:
        headers = {
            "Authorization": f"Bearer {api_token}"
        }
        
        files = {
            'files': (file_name, io.BytesIO(parquet_bytes), 'application/octet-stream')
        }
        
        data = {
            'bucket': bucket,
            'base_path': base_path
        }
        
        response = requests.post(
            api_url,
            headers=headers,
            files=files,
            data=data,
            timeout=30
        )
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        error_msg = f"API request error: {str(e)}"
        print(error_msg)
        return {"error": error_msg}
    except Exception as e:
        error_msg = f"Error uploading to S3: {str(e)}"
        print(error_msg)
        return {"error": error_msg}

@app.function(image=image)
@modal.asgi_app()
def fastapi_app():
    return web_app