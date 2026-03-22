"""Silver transformation for inventory snapshots.

This module takes the Bronze inventory snapshot dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.silver_inventory_snapshots import (
    BRONZE_TABLE,
    SILVER_PATH,
    SILVER_TABLE,
    apply_schema_and_basic_quality,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_inventory_snapshots").getOrCreate()


def read_bronze_inventory_snapshots(spark: SparkSession) -> DataFrame:
    """Read the Bronze inventory snapshot dataset."""
    return spark.table(BRONZE_TABLE)


def write_silver_inventory_snapshots(df: DataFrame) -> None:
    """Write the Silver inventory snapshot dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for inventory snapshots."""
    spark = get_spark()
    bronze_df = read_bronze_inventory_snapshots(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_inventory_snapshots(silver_df)


if __name__ == "__main__":
    main()
