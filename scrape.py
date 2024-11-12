from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
import boto3
from botocore.exceptions import ClientError
import modal
from typing import Union
import os


image = modal.Image.debian_slim().pip_install("boto3")
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


@app.function(secrets=[modal.Secret.from_name("aws-secret")])
def getenv():
    return os.environ["AWS_ACCESS_KEY_ID"]

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


@app.function(image=image)
@modal.asgi_app()
def fastapi_app():
    return web_app