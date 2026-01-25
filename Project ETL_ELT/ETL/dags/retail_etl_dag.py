import pandas as pd
import yaml
from typing import List
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException
from pendulum import datetime
from include.etl.extract_s3 import extract_data_from_s3, get_storage_options
from include.etl.load_s3_csv import load_df_to_s3
from include.etl.transform import merge_data, transform_products_data, transform_sales_data

with open("include/config.yaml", "r", encoding="utf-8") as config_file:
    config = yaml.safe_load(config_file)

s3_config = config.get("s3", {})
RAW_PREFIX = s3_config.get("raw_prefix")
PROCESSED_PREFIX = (s3_config.get("processed_prefix"))


def _build_processed_path(filename: str) -> str:
    return f"s3://{config['s3']['bucket']}/{PROCESSED_PREFIX}/{filename}"


def _read_remote_df(path: str, aws_conn_id: str) -> pd.DataFrame:
    _, storage_options = get_storage_options(aws_conn_id)
    if path.lower().endswith(".json"):
        return pd.read_json(path, storage_options=storage_options)
    return pd.read_csv(path, storage_options=storage_options)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail_etl"],
)
def retail_etl_dag():
    aws_conn_id = config["aws_conn_id"]
    bucket = config["s3"]["bucket"]
    raw_prefix = RAW_PREFIX
    if not raw_prefix:
        raise AirflowException(
            "Please set 'raw_prefix' (or legacy 'folder') in include/config.yaml under s3."
        )

    @task_group(group_id="extract_group")
    def extract_group():
        @task(task_id="extract_csv_files")
        def extract_csv_files() -> List[str]:
            return extract_data_from_s3(bucket, raw_prefix, aws_conn_id, "csv")

        @task(task_id="extract_json_files")
        def extract_json_files() -> List[str]:
            return extract_data_from_s3(bucket, raw_prefix, aws_conn_id, "json")

        @task(task_id="get_sales_path")
        def get_sales_path(csv_paths: List[str]) -> str:
            sales_path = next(
                (path for path in csv_paths if path.lower().endswith(".csv") and "sales" in path.lower()),
                None,
            )
            if not sales_path:
                raise AirflowException("Sales file not found in S3")
            return sales_path

        @task(task_id="get_products_path")
        def get_products_path(csv_paths: List[str], json_paths: List[str]) -> str:
            products_path = None
            for collection in (json_paths, csv_paths):
                for path in collection:
                    if "product" in path.lower():
                        products_path = path
                        break
                if products_path:
                    break
            if not products_path:
                raise AirflowException("Products file not found in S3")

            return products_path

        csv_paths = extract_csv_files()
        json_paths = extract_json_files()
        sales_path = get_sales_path(csv_paths)
        products_path = get_products_path(csv_paths, json_paths)
        return {"sales_path": sales_path, "products_path": products_path}

    @task_group(group_id="transform_group")
    def transform_group(sales_path: str, products_path: str):
        @task(task_id="transform_sales")
        def transform_sales(path: str) -> str:
            df = _read_remote_df(path, aws_conn_id)
            cleaned_df = transform_sales_data(df)
            output_path = _build_processed_path("cleaned_sales.csv")
            return load_df_to_s3(cleaned_df, output_path, aws_conn_id)

        @task(task_id="transform_products")
        def transform_products(path: str) -> str:
            df = _read_remote_df(path, aws_conn_id)
            cleaned_df = transform_products_data(df)
            output_path = _build_processed_path("cleaned_products.csv")
            return load_df_to_s3(cleaned_df, output_path, aws_conn_id)

        cleaned_sales = transform_sales(sales_path)
        cleaned_products = transform_products(products_path)
        return {
            "sales_clean": cleaned_sales,
            "products_clean": cleaned_products,
        }

    @task(task_id="merge_data")
    def merge_task(clean_sales_path: str, clean_products_path: str) -> str:
        sales_df = _read_remote_df(clean_sales_path, aws_conn_id)
        products_df = _read_remote_df(clean_products_path, aws_conn_id)
        merged_df = merge_data(sales_df, products_df)
        output_path = _build_processed_path("merged_sales_products.csv")
        return load_df_to_s3(merged_df, output_path, aws_conn_id)

    extracted = extract_group()
    transformed = transform_group(
        extracted["sales_path"], extracted["products_path"]
    )
    merge_task(transformed["sales_clean"], transformed["products_clean"])


retail_etl_dag()
