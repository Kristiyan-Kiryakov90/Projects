import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check
from pandera.errors import SchemaError




sales_input_schema = pa.DataFrameSchema(
    {
        "sales_id": Column(int, coerce=True, nullable=False),
        "product_id": Column(int, coerce=True, nullable=False),
        "region": Column(str, coerce=True, nullable=False),
        "qty": Column(int, Check.greater_than(0), coerce=True, nullable=False),
        "price": Column(float, Check.greater_than(0), coerce=True, nullable=False),
        "time_stamp": Column(pa.DateTime, coerce=True, nullable=False),
        "discount": Column(float, coerce=True, nullable=True),
        "order_status": Column(str, coerce=True, nullable=False),
    }
)

products_input_schema = pa.DataFrameSchema(
    {
        "product_id": Column(int, coerce=True, nullable=False),
        "category": Column(str, coerce=True, nullable=False),
        "brand": Column(str, coerce=True, nullable=False),
        "rating": Column(float, coerce=True, nullable=True),
        "in_stock": Column(bool, coerce=True, nullable=False),
        "launch_date": Column(pa.DateTime, coerce=True, nullable=True),
    }
)