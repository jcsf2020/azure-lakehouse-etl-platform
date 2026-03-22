"""Silver transformation helpers for inventory snapshots."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.inventory_snapshots_raw"
SILVER_TABLE = "silver.inventory_snapshots"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/inventory_snapshots"


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("snapshot_date", F.to_date("snapshot_date"))
        .withColumn("warehouse_id", F.col("warehouse_id").cast("string"))
        .withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("stock_on_hand", F.col("stock_on_hand").cast("int"))
        .withColumn("units_received", F.col("units_received").cast("int"))
        .withColumn("units_sold", F.col("units_sold").cast("int"))
        .withColumn("units_returned", F.col("units_returned").cast("int"))
    )

    filtered_df = typed_df.filter(
        F.col("snapshot_date").isNotNull()
        & F.col("warehouse_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("stock_on_hand").isNotNull()
        & F.col("units_received").isNotNull()
        & F.col("units_sold").isNotNull()
        & F.col("units_returned").isNotNull()
    )

    non_negative_df = filtered_df.filter(
        (F.col("stock_on_hand") >= 0)
        & (F.col("units_received") >= 0)
        & (F.col("units_sold") >= 0)
        & (F.col("units_returned") >= 0)
    )

    return non_negative_df.dropDuplicates(
        ["snapshot_date", "warehouse_id", "product_id"]
    )
