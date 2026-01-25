import pandas as pd
from include.logger import setup_logger
from include.validations.validate_inputs import (
    validate_input_sales_schema,
    validate_input_products_schema,
)
from include.validations.validate_outputs import (
    validate_output_sales_schema,
    validate_output_products_schema,
)

logger = setup_logger("transform")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    normalized_df.columns = (
        normalized_df.columns.str.strip().str.replace(" ", "_").str.lower()
    )
    return normalized_df


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    sales_df = _normalize_columns(df)
    sales_df["time_stamp"] = pd.to_datetime(
        sales_df["time_stamp"], format="mixed", errors="coerce"
    )
    sales_df = sales_df.dropna(subset=["sales_id", "product_id", "time_stamp"])
    sales_df = sales_df.drop_duplicates(subset=["sales_id"])
    sales_df = validate_input_sales_schema(sales_df)
    logger.info("Sales data cleaned")
    return validate_output_sales_schema(sales_df)


def transform_products_data(df: pd.DataFrame) -> pd.DataFrame:
    products_df = _normalize_columns(df)
    products_df["launch_date"] = pd.to_datetime(
        products_df["launch_date"], format="mixed", errors="coerce"
    )
    products_df = products_df.dropna(subset=["product_id"])
    products_df = products_df.drop_duplicates(subset=["product_id"])
    products_df = products_df.dropna(subset=["launch_date"])
    products_df = validate_input_products_schema(products_df)
    logger.info("Products data cleaned")
    return validate_output_products_schema(products_df)


def merge_data(sales_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Merging data")
    merged_df = sales_df.merge(products_df, how="inner", on="product_id")
    return merged_df
