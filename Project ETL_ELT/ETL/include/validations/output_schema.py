import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check, DateTime
from pandera.errors import SchemaError




sales_output_schema = pa.DataFrameSchema({
    'sales_id': Column(int),
    'product_id': Column(int),
    'region': Column(str),
    'qty': Column(int),
    'price': Column(float,Check.greater_than(0)),
    'time_stamp': Column(pa.DateTime),
    'discount': Column(float),
    'order_status': Column(str),
})


products_output_schema = pa.DataFrameSchema({
    "product_id": Column(int),
    "category": Column(str),
    "brand": Column(str),
    "rating": Column(float),
    "in_stock": Column(bool),
    "launch_date": Column(pa.DateTime),

})
