"""Shared Arrow / Iceberg schemas for order tables."""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, NestedField, StringType

ORDER_ARROW_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("customer", pa.string(), nullable=False),
        pa.field("total", pa.float64(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
    ]
)

ORDER_ICEBERG_SCHEMA = Schema(
    NestedField(1, "order_id", StringType(), required=True),
    NestedField(2, "customer", StringType(), required=True),
    NestedField(3, "total", DoubleType(), required=True),
    NestedField(4, "status", StringType(), required=True),
)

TABLE_IDENTIFIER = "orders"
