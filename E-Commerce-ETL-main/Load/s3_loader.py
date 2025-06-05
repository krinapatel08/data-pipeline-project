import io
import logging

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)


def upload_df_to_s3(df: pd.DataFrame, bucket: str, key: str) -> bool:
    """
    Upload a DataFrame to an S3 bucket in parquet format with snappy compression.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        bucket (str): S3 bucket name.
        key (str): S3 object key (path + filename).

    Returns:
        bool: True if upload was successful, False otherwise.
    """
    logging.info(f"Starting upload of {key} to bucket {bucket}...")

    parquet_buffer = io.BytesIO()

    # Convert UUID-like columns to string for parquet compatibility
    for col in df.columns:
        if col.endswith("_id") and df[col].dtype == "object":
            df[col] = df[col].astype(str)

    try:
        df.to_parquet(parquet_buffer, engine="pyarrow", index=False, compression="snappy")
        parquet_buffer.seek(0)
    except Exception as e:
        logging.error(f"Failed to convert DataFrame to parquet: {e}")
        return False

    try:
        s3_resource = boto3.resource("s3")
        s3_resource.Bucket(bucket).put_object(
            Body=parquet_buffer.getvalue(),
            Key=key,
            ACL="private"
        )
        logging.info(f"Successfully uploaded {key} to {bucket}")
        return True
    except ClientError as e:
        logging.error(f"Failed to upload {key} to S3 bucket {bucket}: {e}")
        return False
