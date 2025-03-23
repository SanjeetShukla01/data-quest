# sync_to_s3.py
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Configuration
DOWNLOAD_DIR = "bls_data"  # Directory where files are downloaded
S3_BUCKET = "my-bls-data-bucket"  # Replace with your S3 bucket name
S3_PREFIX = "bls_data/"  # Optional: Prefix for S3 object keys
s3_client = boto3.client("s3")


def get_s3_files():
    """Fetch the list of files currently in the S3 bucket."""
    s3_files = set()
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
            if "Contents" in page:
                for obj in page["Contents"]:
                    # Remove the prefix from the object key
                    file_name = os.path.relpath(obj["Key"], S3_PREFIX)
                    s3_files.add(file_name)
    except ClientError as e:
        print(f"Error fetching S3 files: {e}")
    return s3_files


def upload_file(file_name):
    """Upload a file to S3 if it doesn't already exist or has been updated."""
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    s3_key = S3_PREFIX + file_name

    # Check if the file already exists in S3 and compare ETags (MD5 hashes)
    try:
        s3_head = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        s3_etag = s3_head["ETag"].strip('"')
        local_etag = compute_md5(file_path)
        if s3_etag == local_etag:
            print(f"File is up-to-date in S3: {file_name}")
            return False  # No need to upload
    except ClientError:
        pass  # File does not exist in S3, proceed to upload

    # Upload the file
    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded to S3: {file_name}")
        return True
    except NoCredentialsError:
        print("Credentials not available for S3 upload.")
        return False


def delete_file(file_name):
    """Delete a file from the S3 bucket."""
    s3_key = S3_PREFIX + file_name
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"Deleted from S3: {file_name}")
    except ClientError as e:
        print(f"Error deleting file {file_name} from S3: {e}")


def compute_md5(file_path):
    """Compute the MD5 hash of a file."""
    import hashlib
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main():
    local_files = set(os.listdir(DOWNLOAD_DIR))
    s3_files = get_s3_files()

    for file_name in local_files:
        file_path = os.path.join(DOWNLOAD_DIR, file_name)
        if os.path.isfile(file_path):  # Ensure it's a file, not a directory
            upload_file(file_name)

    for file_name in s3_files - local_files:
        delete_file(file_name)


if __name__ == "__main__":
    main()