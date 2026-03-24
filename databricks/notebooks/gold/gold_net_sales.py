"""Gold aggregation for net sales — Databricks notebook entrypoint.

This module reads from the Gold daily_sales and daily_returns datasets and
produces a net sales summary: gross revenue reduced by any refund amounts
matched on order_date, with net_revenue computed as the difference.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_net_sales import (
    DAILY_RETURNS_TABLE,
    DAILY_SALES_TABLE,
    GOLD_PATH,
    GOLD_TABLE,
    build_net_sales,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_net_sales").getOrCreate()


def read_gold_daily_sales(spark: SparkSession) -> DataFrame:
    """Read the Gold daily sales dataset."""
    return spark.table(DAILY_SALES_TABLE)


def read_gold_daily_returns(spark: SparkSession) -> DataFrame:
    """Read the Gold daily returns dataset."""
    return spark.table(DAILY_RETURNS_TABLE)


def write_gold_net_sales(df: DataFrame) -> None:
    """Write the Gold net sales dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold aggregation for net sales."""
    spark = get_spark()
    daily_sales_df = read_gold_daily_sales(spark)
    daily_returns_df = read_gold_daily_returns(spark)
    net_sales_df = build_net_sales(daily_sales_df, daily_returns_df)
    write_gold_net_sales(net_sales_df)


if __name__ == "__main__":
    main()
