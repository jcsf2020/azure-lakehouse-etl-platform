"""Gold dimensional table for dim_product (SCD2) — Databricks notebook entrypoint.

Reads from silver.products, applies the SCD2 dimensional shape via the reusable
module helper, and writes the final gold.dim_product Delta table.

Load strategy: full-refresh.  Every run rebuilds the dimension from the complete
Silver products table, treating all rows as current active versions.  The
scd_row_hash column is stored for use by a future incremental merge pipeline.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession

# Explicit effective date — keeps dimensional output deterministic and reproducible.
# Update this constant when a new historical load cutoff is required.
EFFECTIVE_START_DATE: date = date(2025, 1, 1)

from azure_lakehouse_etl.gold_dim_product import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_gold_dim_product,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_dim_product").getOrCreate()


def read_silver_products(spark: SparkSession) -> DataFrame:
    """Read the current Silver products table."""
    return spark.table(SILVER_TABLE)


def write_gold_dim_product(df: DataFrame) -> None:
    """Write the Gold dim_product dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold dim_product build."""
    spark = get_spark()
    silver_df = read_silver_products(spark)
    gold_df = build_gold_dim_product(silver_df, effective_start_date=EFFECTIVE_START_DATE)
    write_gold_dim_product(gold_df)


if __name__ == "__main__":
    main()
