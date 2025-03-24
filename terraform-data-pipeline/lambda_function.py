import boto3
import requests
import json
import os

s3 = boto3.client("s3")
bucket_name = os.environ["S3_BUCKET"]

def sync_bls_dataset():
    pass

def fetch_datausa_api():
    pass

def lambda_handler(event, context):
    sync_bls_dataset()
    fetch_datausa_api()
    return {
        "statusCode": 200,
        "body": json.dumps("Data pipeline executed successfully")
    }