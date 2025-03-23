import os

import requests
import boto3
import json
from botocore.exceptions import NoCredentialsError

# Configuration
API_URL = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
S3_BUCKET = "my-bls-data-bucket"
S3_PREFIX = "datausa_api/"
DOWNLOAD_DIR = "api_data"


os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def fetch_data_from_api():
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from API (Status Code: {response.status_code})")


def save_to_local_file(data, file_name):
    with open(file_name, "w") as f:
        json.dump(data, f, indent=4)
    print(f"Data saved to local file: {file_name}")


def upload_to_s3(file_name, bucket, prefix):
    s3_client = boto3.client("s3")

    s3_key = f"{prefix}{os.path.basename(file_name)}"
    try:
        s3_client.upload_file(file_name, bucket, s3_key)
        print(f"File uploaded to S3: s3://{bucket}/{s3_key}")
    except NoCredentialsError:
        print("Credentials not available for S3 upload.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")


def main():
    print("Fetching data from Data USA API...")
    data = fetch_data_from_api()

    local_file = DOWNLOAD_DIR+"/population_data.json"
    save_to_local_file(data, local_file)

    print("Uploading file to S3...")
    upload_to_s3(local_file, S3_BUCKET, S3_PREFIX)


if __name__ == "__main__":
    main()
