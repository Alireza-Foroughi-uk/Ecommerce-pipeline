import boto3
import pandas as pd
from io import StringIO

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

def read_from_s3(key):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    content = response['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))

def transform(df):
    print(f"Rows before cleaning: {len(df)}")

    # Drop nulls
    df = df.dropna()

    # Fix date format
    df['order_date'] = pd.to_datetime(df['order_date'])

    # Remove cancelled and refunded orders
    df = df[df['status'].isin(['completed', 'pending'])]

    # Recalculate total_revenue to make sure it's correct
    df['total_revenue'] = df['quantity'] * df['unit_price']
    df['total_revenue'] = df['total_revenue'].round(2)

    # Add month and year columns for easier analysis
    df['order_month'] = df['order_date'].dt.month
    df['order_year'] = df['order_date'].dt.year

    print(f"Rows after cleaning: {len(df)}")
    return df

def upload_to_s3(df, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    print(f"Uploaded cleaned data → s3://{BUCKET_NAME}/{key}")

df_raw = read_from_s3('raw/orders_raw.csv')
df_clean = transform(df_raw)
upload_to_s3(df_clean, 'processed/orders_cleaned.csv')