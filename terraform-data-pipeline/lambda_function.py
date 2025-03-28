import os
import requests
import boto3
import json
import hashlib
from bs4 import BeautifulSoup
from botocore.exceptions import NoCredentialsError, ClientError


BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
BLS_USER_AGENT = "SanjeetShukal/1.0 (contact: sanjeets1900@gmail.com)"
BLS_DOWNLOAD_DIR = "/tmp/bls_data"  # Using /tmp for Lambda
DATAUSA_API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
DATAUSA_DOWNLOAD_DIR = "/tmp/api_data"
S3_BUCKET = "rearc-pipeline-bucket"
BLS_S3_PREFIX = "bls_data/"
DATAUSA_S3_PREFIX = "datausa_api/"


s3_client = boto3.client('s3')


def setup_directories():
    """Create necessary directories in Lambda's /tmp space"""
    os.makedirs(BLS_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(DATAUSA_DOWNLOAD_DIR, exist_ok=True)


def get_bls_file_list():
    headers = {"User-Agent": BLS_USER_AGENT}
    response = requests.get(BLS_BASE_URL, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch BLS file list (Status Code: {response.status_code})")

    soup = BeautifulSoup(response.text, "html.parser")
    files = [a.text for a in soup.find_all("a") if not a.text.endswith("/")]
    return files[1::]


def download_bls_file(url, file_name):
    headers = {"User-Agent": BLS_USER_AGENT}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(os.path.join(BLS_DOWNLOAD_DIR, file_name), "wb") as f:
            f.write(response.content)
        print(f"Downloaded BLS file: {file_name}")
    else:
        print(f"Failed to download BLS file: {file_name} (Status Code: {response.status_code})")


def get_s3_files(prefix):
    s3_files = set()
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    file_name = os.path.relpath(obj["Key"], prefix)
                    s3_files.add(file_name)
    except ClientError as e:
        print(f"Error fetching S3 files: {e}")
    return s3_files


def compute_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def sync_file_to_s3(file_path, s3_prefix):
    file_name = os.path.basename(file_path)
    s3_key = f"{s3_prefix}{file_name}"

    try:
        s3_head = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        s3_etag = s3_head["ETag"].strip('"')
        local_etag = compute_md5(file_path)
        if s3_etag == local_etag:
            print(f"File is up-to-date in S3: {file_name}")
            return False
    except ClientError:
        pass

    try:
        s3_client.upload_file(file_path, S3_BUCKET, s3_key)
        print(f"Uploaded to S3: {s3_key}")
        return True
    except NoCredentialsError:
        print("Credentials not available for S3 upload.")
        return False
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False


def delete_s3_file(s3_key):
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"Deleted from S3: {s3_key}")
    except ClientError as e:
        print(f"Error deleting file from S3: {e}")


def process_bls_data():
    print("Processing BLS data...")

    bls_files = get_bls_file_list()
    for file_name in bls_files:
        file_url = BLS_BASE_URL + file_name
        download_bls_file(file_url, file_name)

    local_files = set(f for f in os.listdir(BLS_DOWNLOAD_DIR)
                      if os.path.isfile(os.path.join(BLS_DOWNLOAD_DIR, f)))
    s3_files = get_s3_files(BLS_S3_PREFIX)

    for file_name in local_files:
        file_path = os.path.join(BLS_DOWNLOAD_DIR, file_name)
        sync_file_to_s3(file_path, BLS_S3_PREFIX)

    for file_name in s3_files - local_files:
        s3_key = f"{BLS_S3_PREFIX}{file_name}"
        delete_s3_file(s3_key)


def process_datausa_api():
    print("Processing Data USA API...")

    response = requests.get(DATAUSA_API_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch Data USA API (Status Code: {response.status_code})")
    data = response.json()

    local_file = os.path.join(DATAUSA_DOWNLOAD_DIR, "population_data.json")
    with open(local_file, "w") as f:
        json.dump(data, f, indent=4)
    print("Saved Data USA API data to local file")

    sync_file_to_s3(local_file, DATAUSA_S3_PREFIX)


def lambda_handler(event, context):
    try:
        setup_directories()
        process_bls_data()
        process_datausa_api()

        return {
            'statusCode': 200,
            'body': json.dumps('Data pipeline executed successfully')
        }
    except Exception as e:
        print(f"Error in Lambda execution: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


# For local testing
if __name__ == '__main__':
    lambda_handler(None, None)