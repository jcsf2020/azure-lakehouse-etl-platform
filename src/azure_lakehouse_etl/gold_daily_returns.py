"""Gold aggregation helpers for daily returns."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_TABLE = "silver.returns"
GOLD_TABLE = "gold.daily_returns"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/daily_returns"


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
