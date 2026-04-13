import boto3
import os

# Replace these with your actual keys from the CSV file
AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
BUCKET_NAME = 'ecommerce-pipeline-alireza'
REGION = 'eu-north-1'

s3 = boto3.client(
    's3',
    region_name=REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def upload_file(local_path, s3_key):
    s3.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"Uploaded {local_path} → s3://{BUCKET_NAME}/{s3_key}")

upload_file('data/orders_raw.csv', 'raw/orders_raw.csv')