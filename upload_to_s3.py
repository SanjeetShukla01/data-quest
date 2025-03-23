import os

import boto3

from fetch_bls_data import FILES

BUCKET_NAME = "your-bucket-name"
s3_client = boto3.client("s3")


def upload_to_s3(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Uploaded {file_name} to s3://{bucket}/{object_name}")
    except Exception as e:
        print(f"Failed to upload {file_name}: {str(e)}")


# Upload all files to S3
for file in FILES:
    file_path = os.path.join("bls_data", file)
    upload_to_s3(file_path, BUCKET_NAME, f"bls/{file}")
