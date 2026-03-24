"""Gold aggregation for inventory health — Databricks notebook entrypoint.

This module reads from the Silver inventory snapshots dataset and produces the
first Gold-layer aggregate: a daily inventory health summary grouped by
snapshot_date, covering stock on hand and unit flow metrics across all products.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_inventory_health import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_inventory_health,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_inventory_health").getOrCreate()


def read_silver_inventory_snapshots(spark: SparkSession) -> DataFrame:
    """Read the Silver inventory snapshots dataset."""
    return spark.table(SILVER_TABLE)


def write_gold_inventory_health(df: DataFrame) -> None:
    """Write the Gold inventory health dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold aggregation for inventory health."""
    spark = get_spark()
    silver_df = read_silver_inventory_snapshots(spark)
    gold_df = build_inventory_health(silver_df)
    write_gold_inventory_health(gold_df)


if __name__ == "__main__":
    main()
