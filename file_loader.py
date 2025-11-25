""" 
reading the raw data from either:
AWS S3 bucket (if file exists)
Local directory (fallback option)..
"""
import os
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

def load_csv(s3_path: str, local_path: str) -> pd.DataFrame:
    """
    Load CSV from S3 if available; otherwise, fallback to local path.
    """
    # Try S3
    if s3_path.startswith("s3://"):
        try:
            s3 = boto3.client('s3')
            bucket, key = s3_path.replace("s3://", "").split("/", 1)
            s3.head_object(Bucket=bucket, Key=key)
            print(f"⬆ Loading from S3: {s3_path}")
            return pd.read_csv(s3.get_object(Bucket=bucket, Key=key)['Body'])
        except (ClientError, NoCredentialsError):
            print(f"⚠ S3 not available, falling back to local.")

    # Fallback to local
    if os.path.exists(local_path):
        print(f"⬇ Loading from LOCAL: {local_path}")
        return pd.read_csv(local_path)

    # Neither exists
    raise FileNotFoundError(f"❌ File not found in S3 or LOCAL.\nS3: {s3_path}\nLOCAL: {local_path}")

