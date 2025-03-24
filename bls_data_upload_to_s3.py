import os
import requests
import boto3
from botocore.exceptions import NoCredentialsError, ClientError


BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
USER_AGENT = "MyApp/1.0 (contact: your-email@example.com)"
DOWNLOAD_DIR = "bls_data"
S3_BUCKET = "my-bls-data-bucket"
S3_PREFIX = "bls_data/"
s3_client = boto3.client("s3")


os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def get_file_list():
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(BASE_URL, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch file list (Status Code: {response.status_code})")

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")
    files = [a.text for a in soup.find_all("a") if not a.text.endswith("/")]  # Exclude directories
    return files[1::]


def download_file(url, file_name):
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(os.path.join(DOWNLOAD_DIR, file_name), "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {file_name}")
    else:
        print(f"Failed to download: {file_name} (Status Code: {response.status_code})")



def get_s3_files():
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
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    s3_key = S3_PREFIX + file_name

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
        print(f"Uploaded to S3: {file_name}")
        return True
    except NoCredentialsError:
        print("Credentials not available for S3 upload.")
        return False


def delete_file(file_name):
    s3_key = S3_PREFIX + file_name
    try:
        s3_client.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"Deleted from S3: {file_name}")
    except ClientError as e:
        print(f"Error deleting file {file_name} from S3: {e}")


def compute_md5(file_path):
    import hashlib
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def main():
    files = get_file_list()
    for file_name in files:
        file_url = BASE_URL + file_name
        download_file(file_url, file_name)
    local_files = set(os.listdir(DOWNLOAD_DIR))
    s3_files = get_s3_files()

    for file_name in local_files:
        file_path = os.path.join(DOWNLOAD_DIR, file_name)
        if os.path.isfile(file_path):  # Ensure it's a file, not a directory
            upload_file(file_name)

    for file_name in s3_files - local_files:
        delete_file(file_name)


if __name__ == '__main__':
    main()
