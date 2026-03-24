"""Gold dimensional table for dim_customer (SCD2) — Databricks notebook entrypoint.

Reads from silver.customers, applies the SCD2 dimensional shape via the reusable
module helper, and writes the final gold.dim_customer Delta table.

Load strategy: full-refresh.  Every run rebuilds the dimension from the complete
Silver customers table, treating all rows as current active versions.  The
scd_row_hash column is stored for use by a future incremental merge pipeline.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession

# Explicit effective date — keeps dimensional output deterministic and reproducible.
# Update this constant when a new historical load cutoff is required.
EFFECTIVE_START_DATE: date = date(2025, 1, 1)

from azure_lakehouse_etl.gold_dim_customer import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_gold_dim_customer,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_dim_customer").getOrCreate()


def read_silver_customers(spark: SparkSession) -> DataFrame:
    """Read the current Silver customers table."""
    return spark.table(SILVER_TABLE)


def write_gold_dim_customer(df: DataFrame) -> None:
    """Write the Gold dim_customer dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold dim_customer build."""
    spark = get_spark()
    silver_df = read_silver_customers(spark)
    gold_df = build_gold_dim_customer(silver_df, effective_start_date=EFFECTIVE_START_DATE)
    write_gold_dim_customer(gold_df)


if __name__ == "__main__":
    main()
