"""Silver transformation for orders.

This module takes the Bronze orders dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.orders_raw"
SILVER_TABLE = "silver.orders"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/orders"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_orders").getOrCreate()


def read_bronze_orders(spark: SparkSession) -> DataFrame:
    """Read the Bronze orders dataset."""
    return spark.table(BRONZE_TABLE)


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


def write_silver_orders(df: DataFrame) -> None:
    """Write the Silver orders dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for orders."""
    spark = get_spark()
    bronze_df = read_bronze_orders(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_orders(silver_df)


if __name__ == "__main__":
    main()
