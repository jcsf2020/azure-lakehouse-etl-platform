"""Silver transformation helpers for orders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.orders_raw"
SILVER_TABLE = "silver.orders"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/orders"


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("order_status", F.col("order_status").cast("string"))
        .withColumn("payment_method", F.col("payment_method").cast("string"))
        .withColumn("currency", F.col("currency").cast("string"))
        .withColumn("order_date", F.col("order_date").cast("date"))
        .withColumn("order_total", F.col("order_total").cast("double"))
        .withColumn("shipping_amount", F.col("shipping_amount").cast("double"))
        .withColumn("discount_amount", F.col("discount_amount").cast("double"))
    )

    filtered_df = typed_df.filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("order_date").isNotNull()
        & F.col("order_status").isNotNull()
        & F.col("payment_method").isNotNull()
        & F.col("currency").isNotNull()
        & F.col("order_total").isNotNull()
        & F.col("shipping_amount").isNotNull()
        & F.col("discount_amount").isNotNull()
        & (F.col("order_total") >= 0)
        & (F.col("shipping_amount") >= 0)
        & (F.col("discount_amount") >= 0)
    )

    return filtered_df.dropDuplicates(["order_id"])
