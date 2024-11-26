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


image = modal.Image.debian_slim().pip_install("boto3", "pandas", "pyarrow", "requests")
web_app = FastAPI()
app = modal.App(name="has-simple-web-endpoint", image=image)

@web_app.post("/api/upload")
async def setToS3(request: Request):
    body = await request.json()
    # get content element from body
    
    response = process_markdown_array_to_s3.remote(body['data'], body['url'])
    
    return response

@app.function(secrets=[modal.Secret.from_name("s3-credential")])
def process_markdown_array_to_s3(
    markdown_array: List[Dict[str, str]],
    url: str,
) -> Dict[str, Any]:
    
    print(markdown_array)
    print(url)

    """
    Convert array of markdown objects to parquet and upload to S3
    """
    api_url=os.environ["API_URL"]
    api_token=os.environ["API_TOKEN"]
    base_path=os.environ["BASE_PATH"]
    bucket=os.environ["BUCKET"]
    try:
        # Create DataFrame directly from the array
        df = pd.DataFrame(markdown_array)
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
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

        # create filename
        path = url.split('/')[-1]
        title, id = path.split('-', 1)
    
        filename = f"{title.lower().replace(' ', '_')}_{id}"
        
        return upload_to_s3_api.remote(
            parquet_bytes=parquet_bytes,
            file_name=filename,
            bucket=bucket,
            base_path=base_path,
            api_token=api_token,
            api_url=api_url
        )
        
    except Exception as e:
        error_msg = f"Error processing markdown array: {str(e)}"
        print(error_msg)
        return {"error": error_msg}

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