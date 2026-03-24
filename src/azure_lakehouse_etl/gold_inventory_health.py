"""Gold aggregation helpers for inventory health."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_TABLE = "silver.inventory_snapshots"
GOLD_TABLE = "gold.inventory_health"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/inventory_health"


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
