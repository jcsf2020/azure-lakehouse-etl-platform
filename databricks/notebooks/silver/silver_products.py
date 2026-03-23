"""Silver transformation for products.

This module takes the Bronze products dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.silver_products import (
    BRONZE_TABLE,
    SILVER_PATH,
    SILVER_TABLE,
    apply_schema_and_basic_quality,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_products").getOrCreate()


def read_bronze_products(spark: SparkSession) -> DataFrame:
    """Read the Bronze products dataset."""
    return spark.table(BRONZE_TABLE)


def write_silver_products(df: DataFrame) -> None:
    """Write the Silver products dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for products."""
    spark = get_spark()
    bronze_df = read_bronze_products(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_products(silver_df)


if __name__ == "__main__":
    main()
