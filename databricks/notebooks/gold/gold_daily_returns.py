"""Gold aggregation for daily returns — Databricks notebook entrypoint.

This module reads from the Silver returns dataset and produces the first
Gold-layer aggregate: a daily returns summary grouped by return_date, covering
return volume and total refund amount.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_daily_returns import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_daily_returns,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_daily_returns").getOrCreate()


def read_silver_returns(spark: SparkSession) -> DataFrame:
    """Read the Silver returns dataset."""
    return spark.table(SILVER_TABLE)


def write_gold_daily_returns(df: DataFrame) -> None:
    """Write the Gold daily returns dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold aggregation for daily returns."""
    spark = get_spark()
    silver_df = read_silver_returns(spark)
    gold_df = build_daily_returns(silver_df)
    write_gold_daily_returns(gold_df)


if __name__ == "__main__":
    main()
