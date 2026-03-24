"""Gold fact table for fact_inventory_daily — Databricks notebook entrypoint.

Reads from silver.inventory_snapshots and the two applicable Gold dimensions
(dim_date, dim_product), resolves dimension foreign keys via the reusable
module helper (including SCD2 point-in-time join for product), and writes the
final gold.fact_inventory_daily Delta table.

Grain: one row per (snapshot_date, warehouse_id, product_id).
Load strategy: full-refresh.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_dim_date import GOLD_TABLE as DIM_DATE_TABLE
from azure_lakehouse_etl.gold_dim_product import GOLD_TABLE as DIM_PRODUCT_TABLE
from azure_lakehouse_etl.gold_fact_inventory_daily import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_INVENTORY_SNAPSHOTS_TABLE,
    build_fact_inventory_daily,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_fact_inventory_daily").getOrCreate()


def read_silver_inventory_snapshots(spark: SparkSession) -> DataFrame:
    """Read the current Silver inventory_snapshots table."""
    return spark.table(SILVER_INVENTORY_SNAPSHOTS_TABLE)


def read_dim_date(spark: SparkSession) -> DataFrame:
    """Read the Gold dim_date dimension."""
    return spark.table(DIM_DATE_TABLE)


def read_dim_product(spark: SparkSession) -> DataFrame:
    """Read the Gold dim_product dimension."""
    return spark.table(DIM_PRODUCT_TABLE)


def write_gold_fact_inventory_daily(df: DataFrame) -> None:
    """Write the Gold fact_inventory_daily dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold fact_inventory_daily build."""
    spark = get_spark()

    gold_df = build_fact_inventory_daily(
        silver_inventory_snapshots_df=read_silver_inventory_snapshots(spark),
        dim_date_df=read_dim_date(spark),
        dim_product_df=read_dim_product(spark),
    )

    write_gold_fact_inventory_daily(gold_df)


if __name__ == "__main__":
    main()
