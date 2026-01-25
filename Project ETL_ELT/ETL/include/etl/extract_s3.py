import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from ..logger import setup_logger
from airflow.exceptions import AirflowException

logging = setup_logger('extract_data')

def get_storage_options (aws_conn_id:str):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    credentials = s3_hook.get_credentials()

    storage_options = {
        'key': credentials.access_key,
        'secret': credentials.secret_key,
    }
    return s3_hook, storage_options


def extract_data_from_s3(bucket:str, folder:str, aws_conn_id:str, file_type:str = "csv"):
    logging.info('Extracting data from s3')
    s3_hook, storage_options = get_storage_options(aws_conn_id)
    keys = s3_hook.list_keys(bucket_name = bucket, prefix = folder)

    if not keys:
        raise AirflowException(f"No keys found in {bucket}/{folder}")


    paths = [f"s3://{bucket}/{key}" for key in keys if key.lower().endswith(f"{file_type}")]
    if not paths:
        raise AirflowException(f"No keys found in {bucket}/{folder}")
    return paths

