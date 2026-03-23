"""Silver transformation helpers for customers."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.customers_raw"
SILVER_TABLE = "silver.customers"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/customers"


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("first_name", F.col("first_name").cast("string"))
        .withColumn("last_name", F.col("last_name").cast("string"))
        .withColumn("email", F.col("email").cast("string"))
        .withColumn("country", F.col("country").cast("string"))
        .withColumn("city", F.col("city").cast("string"))
        .withColumn("registration_date", F.to_date("registration_date"))
        .withColumn("customer_status", F.col("customer_status").cast("string"))
    )

    filtered_df = typed_df.filter(
        F.col("customer_id").isNotNull()
        & F.col("first_name").isNotNull()
        & F.col("last_name").isNotNull()
        & F.col("email").isNotNull()
        & F.col("country").isNotNull()
        & F.col("city").isNotNull()
        & F.col("registration_date").isNotNull()
        & F.col("customer_status").isNotNull()
    )

    return filtered_df.dropDuplicates(["customer_id"])
