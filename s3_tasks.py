from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging

def verify_s3_upload(bucket_name, prefix):
    """
    Task 3 Logic: Verify that data was successfully put in S3.
    """
    logging.info(f"Checking bucket: {bucket_name} for prefix: {prefix}")
    
    s3 = S3Hook(aws_conn_id='aws_default')
    
    # Check if any keys exist with the prefix
    keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix)
    
    if keys:
        logging.info(f"Found {len(keys)} files in S3. Validation Successful.")
        return keys
    else:
        raise ValueError(f"No files found in {bucket_name}/{prefix} after Glue Job.")