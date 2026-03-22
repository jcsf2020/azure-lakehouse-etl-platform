"""Silver transformation for inventory snapshots.

This module takes the Bronze inventory snapshot dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.inventory_snapshots_raw"
SILVER_TABLE = "silver.inventory_snapshots"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/inventory_snapshots"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_inventory_snapshots").getOrCreate()


def read_bronze_inventory_snapshots(spark: SparkSession) -> DataFrame:
    """Read the Bronze inventory snapshot dataset."""
    return spark.table(BRONZE_TABLE)


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
