"""Gold aggregation for inventory health — Databricks notebook entrypoint.

This module reads from the Silver inventory snapshots dataset and produces the
first Gold-layer aggregate: a daily inventory health summary grouped by
snapshot_date, covering stock on hand and unit flow metrics across all products.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_TABLE = "silver.inventory_snapshots"
GOLD_TABLE = "gold.inventory_health"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/inventory_health"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_inventory_health").getOrCreate()


def read_silver_inventory_snapshots(spark: SparkSession) -> DataFrame:
    """Read the Silver inventory snapshots dataset."""
    return spark.table(SILVER_TABLE)


def build_inventory_health(df: DataFrame) -> DataFrame:
    """Aggregate Silver inventory snapshots into a daily inventory health summary.

    Excludes rows with a null snapshot_date before aggregation, then groups by
    snapshot_date to produce stock on hand and unit flow metrics across all products.
    """
    filtered_df = df.filter(F.col("snapshot_date").isNotNull())

    return (
        filtered_df.groupBy("snapshot_date")
        .agg(
            F.countDistinct("product_id").alias("total_products"),
            F.sum("stock_on_hand").alias("total_stock_on_hand"),
            F.sum("units_received").alias("total_units_received"),
            F.sum("units_sold").alias("total_units_sold"),
            F.sum("units_returned").alias("total_units_returned"),
        )
        .select(
            "snapshot_date",
            "total_products",
            "total_stock_on_hand",
            "total_units_received",
            "total_units_sold",
            "total_units_returned",
        )
        .orderBy("snapshot_date")
    )


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
