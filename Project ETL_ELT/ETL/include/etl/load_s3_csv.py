import pandas as pd
from include.logger import setup_logger
from include.etl.extract_s3 import get_storage_options

logging = setup_logger('loading_data')

def load_df_to_s3(df:pd.DataFrame,s3_path:str, aws_conn_id:str):
    s3_hook, storage_options = get_storage_options(aws_conn_id)

    try:
        df.to_csv(s3_path, index=False, storage_options=storage_options)
    except Exception as e:
        logging.error(f"Error uploading csv to s3 {e}")
        raise

    logging.info(f"Successfully uploaded csv to s3 {s3_path}")
    return s3_path

