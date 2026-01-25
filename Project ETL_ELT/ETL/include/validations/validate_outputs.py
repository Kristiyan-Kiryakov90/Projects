import pandas as pd
from include.validations.output_schema import sales_output_schema, products_output_schema

def validate_output_sales_schema(df: pd.DataFrame) -> pd.DataFrame:
    return sales_output_schema.validate(df)

def validate_output_products_schema(df) ->pd.DataFrame:
    return products_output_schema.validate(df)