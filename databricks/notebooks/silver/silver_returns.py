"""Silver transformation for returns.

This module takes the Bronze returns dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.returns_raw"
SILVER_TABLE = "silver.returns"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/returns"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_returns").getOrCreate()


def read_bronze_returns(spark: SparkSession) -> DataFrame:
    """Read the Bronze returns dataset."""
    return spark.table(BRONZE_TABLE)


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("return_id", F.col("return_id").cast("string"))
        .withColumn("return_date", F.to_date("return_date"))
        .withColumn("order_item_id", F.col("order_item_id").cast("string"))
        .withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("return_reason", F.col("return_reason").cast("string"))
        .withColumn("refund_amount", F.col("refund_amount").cast("double"))
    )

    filtered_df = typed_df.filter(
        F.col("return_id").isNotNull()
        & F.col("return_date").isNotNull()
        & F.col("order_item_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("return_reason").isNotNull()
        & F.col("refund_amount").isNotNull()
    )

    non_negative_df = filtered_df.filter(F.col("refund_amount") >= 0)

    return non_negative_df.dropDuplicates(["return_id"])


def write_silver_returns(df: DataFrame) -> None:
    """Write the Silver returns dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for returns."""
    spark = get_spark()
    bronze_df = read_bronze_returns(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_returns(silver_df)


if __name__ == "__main__":
    main()
