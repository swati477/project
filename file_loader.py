""" 
reading the raw data from either:
AWS S3 bucket (if file exists)
Local directory (fallback option)..
"""
import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import pandas as pd

def load_csv(s3_path: str, local_path: str) -> pd.DataFrame:
    """
    Load CSV either from S3 or from local fallback path.

    Parameters:
        s3_path (str): s3://bucket/folder/file.csv
        local_path (str): ./data/raw/file.csv

    Returns:
        pd.DataFrame
    """
    # Parse S3 path
    if s3_path.startswith("s3://"):
        try:
            s3 = boto3.client('s3')

            bucket = s3_path.replace("s3://", "").split("/")[0]
            key = "/".join(s3_path.replace("s3://", "").split("/")[1:])

            # Check if file exists in S3
            s3.head_object(Bucket=bucket, Key=key)

            print(f"⬆ Loading from S3: {s3_path}")
            obj = s3.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj['Body'])

        except (ClientError, NoCredentialsError) as e:
            print(f"⚠ S3 file not found or credentials issue: {e}")

    # Fallback to local
    if os.path.exists(local_path):
        print(f"⬇ Loading from LOCAL: {local_path}")
        return pd.read_csv(local_path)

    # If neither exists
    raise FileNotFoundError(f"❌ File not found in S3 or LOCAL.\nS3: {s3_path}\nLOCAL: {local_path}")
