"""Silver transformation helpers for products."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.products_raw"
SILVER_TABLE = "silver.products"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/products"


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("product_name", F.col("product_name").cast("string"))
        .withColumn("category", F.col("category").cast("string"))
        .withColumn("subcategory", F.col("subcategory").cast("string"))
        .withColumn("brand", F.col("brand").cast("string"))
        .withColumn("list_price", F.col("list_price").cast("double"))
        .withColumn("cost_price", F.col("cost_price").cast("double"))
        .withColumn("currency", F.col("currency").cast("string"))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
        .withColumn("last_updated", F.col("last_updated").cast("timestamp"))
    )

    filtered_df = typed_df.filter(
        F.col("product_id").isNotNull()
        & F.col("product_name").isNotNull()
        & F.col("category").isNotNull()
        & F.col("subcategory").isNotNull()
        & F.col("brand").isNotNull()
        & F.col("list_price").isNotNull()
        & F.col("cost_price").isNotNull()
        & F.col("currency").isNotNull()
        & F.col("is_active").isNotNull()
        & F.col("last_updated").isNotNull()
        & (F.col("list_price") >= 0)
        & (F.col("cost_price") >= 0)
    )

    return filtered_df.dropDuplicates(["product_id"])
