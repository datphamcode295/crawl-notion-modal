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


@web_app.post("/foo")
async def foo(request: Request):
    body = await request.json()
    return body

@web_app.get("/bar")
async def bar(arg="world"):
    key=getenv.remote()
    return HTMLResponse(f"<h1>Hello Fast {arg}!</h1>{key}")

@web_app.post("/sent-to-s3")
async def setToS3(request: Request):
    body = await request.json()
    # get content element from body
    
    response = process_markdown_array_to_s3.remote(body['contents'], body['file_name'])
    
    return response


@app.function(secrets=[modal.Secret.from_name("aws-secret")])
def getenv():
    return os.environ["AWS_ACCESS_KEY_ID"]
    
@app.function(secrets=[modal.Secret.from_name("s3-credential")])
def getAPIToken():
    return os.environ["API_TOKEN"]

# endpoint to get sign url S3
@web_app.get("/sign-url")
def generate_s3_presigned_url(
    bucket_name: str,
    object_key: str,
    operation: str = 'get',
    expiration: int = 3600,
) -> Union[str, None]:
    return handle_generate_s3_presigned_url.remote(bucket_name, object_key, operation, expiration)

@app.function(secrets=[modal.Secret.from_name("aws-secret")])
def handle_generate_s3_presigned_url(
    bucket_name: str,
    object_key: str,
    operation: str = 'get',
    expiration: int = 3600,
) -> Union[str, None]:
    """
    Generate a pre-signed URL for AWS S3 object operations.
    
    Args:
        bucket_name (str): Name of the S3 bucket
        object_key (str): Key/path of the S3 object
        operation (str): Operation type - 'get' or 'put' (default: 'get')
        expiration (int): URL expiration time in seconds (default: 3600)    
    Returns:
        Union[str, None]: Pre-signed URL if successful, None if error occurs
    """
    # Input validation
    if not bucket_name or not object_key:
        raise ValueError("Bucket name and object key must be provided")
    
    if operation.lower() not in ['get', 'put']:
        raise ValueError("Operation must be either 'get' or 'put'")

    try:
        # Initialize S3 client
        # ACCESS_KEY = "access_key"
        # SECRET_KEY = "secret_key"
        # location = boto3.client('s3', aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY).get_bucket_location(Bucket=bucket_name)['LocationConstraint']
        s3_client = boto3.client(
            's3',
            # region_name=location,
            # aws_access_key_id=ACCESS_KEY,
            # aws_secret_access_key=SECRET_KEY
        )
        
        # Map operation to S3 client method
        client_method = 'put_object'
        
        # Generate the presigned URL
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod=client_method,
            Params={
                'Bucket': bucket_name,
                'Key': object_key
            },
            ExpiresIn=expiration,
        )
        
        return presigned_url

    except ClientError as e:
        print(f"AWS Error: {str(e)}")
        return None
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return None

@app.function(secrets=[modal.Secret.from_name("s3-credential")])
def process_markdown_array_to_s3(
    markdown_array: List[Dict[str, str]],
    file_name: str,
) -> Dict[str, Any]:
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
        
        # save file parquet to local
        # local_path = f"./output/{file_name}"
        # os.makedirs(os.path.dirname(local_path), exist_ok=True)
        # with open(local_path, "wb") as f:
        #     f.write(parquet_bytes)
        # print(f"Saved parquet file locally to: {local_path}")
        
        return upload_to_s3_api.remote(
            parquet_bytes=parquet_bytes,
            file_name=file_name,
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