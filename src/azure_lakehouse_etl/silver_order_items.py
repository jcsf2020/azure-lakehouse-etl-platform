"""Silver transformation helpers for order items."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.order_items_raw"
SILVER_TABLE = "silver.order_items"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/order_items"


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("order_item_id", F.col("order_item_id").cast("string"))
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("discount_amount", F.col("discount_amount").cast("double"))
        .withColumn("line_total", F.col("line_total").cast("double"))
    )

    filtered_df = typed_df.filter(
        F.col("order_item_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & F.col("discount_amount").isNotNull()
        & F.col("line_total").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & (F.col("discount_amount") >= 0)
        & (F.col("line_total") >= 0)
    )

    return filtered_df.dropDuplicates(["order_item_id"])
