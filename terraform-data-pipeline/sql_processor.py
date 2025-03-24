import json
import boto3


def lambda_handler(event, context):
    for record in event["Records"]:
        body = json.loads(record["body"])
        print("Processing SQS message:", body)
    return {
        "statusCode": 200,
        "body": json.dumps("SQS message processed successfully")
    }