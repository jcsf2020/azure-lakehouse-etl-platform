"""Gold aggregation for daily returns — Databricks notebook entrypoint.

This module reads from the Silver returns dataset and produces the first
Gold-layer aggregate: a daily returns summary grouped by return_date, covering
return volume and total refund amount.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_TABLE = "silver.returns"
GOLD_TABLE = "gold.daily_returns"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/daily_returns"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_daily_returns").getOrCreate()


def read_silver_returns(spark: SparkSession) -> DataFrame:
    """Read the Silver returns dataset."""
    return spark.table(SILVER_TABLE)


def build_daily_returns(df: DataFrame) -> DataFrame:
    """Aggregate Silver returns into a daily returns summary.

    Excludes rows with a null return_date before aggregation, then groups by
    return_date to produce return volume and refund amount metrics.
    """
    filtered_df = df.filter(F.col("return_date").isNotNull())

    return (
        filtered_df.groupBy("return_date")
        .agg(
            F.countDistinct("return_id").alias("total_returns"),
            F.sum("refund_amount").alias("total_refund_amount"),
        )
        .select(
            "return_date",
            "total_returns",
            "total_refund_amount",
        )
        .orderBy("return_date")
    )


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
