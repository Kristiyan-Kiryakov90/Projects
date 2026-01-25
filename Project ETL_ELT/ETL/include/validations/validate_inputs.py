import pandas as pd
from airflow.exceptions import AirflowException
from pandera.errors import SchemaErrors
from include.logger import setup_logger
from include.validations.input_schema import sales_input_schema, products_input_schema

logger = setup_logger("input_validation")


def _validate(df: pd.DataFrame, schema, dataset_name: str) -> pd.DataFrame:
    try:
        return schema.validate(df, lazy=True)
    except SchemaErrors as exc:
        failure_cases = exc.failure_cases
        invalid_indexes = (
            failure_cases["index"].dropna().unique()
            if "index" in failure_cases
            else []
        )
        logger.warning(
            f"{dataset_name} validation failed for {len(invalid_indexes)} rows. "
            "Removing invalid rows."
        )
        filtered_df = df.drop(index=invalid_indexes)
        if filtered_df.empty:
            raise AirflowException(f"All rows invalid for {dataset_name}") from exc
        return schema.validate(filtered_df, lazy=True)


def validate_input_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    return _validate(df, sales_input_schema, "sales")


def validate_input_products_schema(df: pd.DataFrame) -> pd.DataFrame:
    return _validate(df, products_input_schema, "products")

