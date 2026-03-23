"""Silver transformation for order items.

This module takes the Bronze order_items dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.silver_order_items import (
    BRONZE_TABLE,
    SILVER_PATH,
    SILVER_TABLE,
    apply_schema_and_basic_quality,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_order_items").getOrCreate()


def read_bronze_order_items(spark: SparkSession) -> DataFrame:
    """Read the Bronze order_items dataset."""
    return spark.table(BRONZE_TABLE)


def write_silver_order_items(df: DataFrame) -> None:
    """Write the Silver order_items dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for order items."""
    spark = get_spark()
    bronze_df = read_bronze_order_items(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_order_items(silver_df)


if __name__ == "__main__":
    main()
