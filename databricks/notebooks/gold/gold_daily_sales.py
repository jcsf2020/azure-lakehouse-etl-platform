"""Gold aggregation for daily sales — Databricks notebook entrypoint.

This module reads from the Silver orders dataset and produces the first
Gold-layer aggregate: a daily sales summary grouped by order_date, covering
order volume, gross revenue, shipping, and discounts.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_daily_sales import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_daily_sales,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_daily_sales").getOrCreate()


def read_silver_orders(spark: SparkSession) -> DataFrame:
    """Read the Silver orders dataset."""
    return spark.table(SILVER_TABLE)


def write_gold_daily_sales(df: DataFrame) -> None:
    """Write the Gold daily sales dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold aggregation for daily sales."""
    spark = get_spark()
    silver_df = read_silver_orders(spark)
    gold_df = build_daily_sales(silver_df)
    write_gold_daily_sales(gold_df)


if __name__ == "__main__":
    main()
