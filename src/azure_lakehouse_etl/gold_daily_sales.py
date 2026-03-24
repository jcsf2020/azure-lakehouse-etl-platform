"""Gold aggregation helpers for daily sales."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_TABLE = "silver.orders"
GOLD_TABLE = "gold.daily_sales"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/daily_sales"


def build_daily_sales(df: DataFrame) -> DataFrame:
    """Aggregate Silver orders into a daily sales summary.

    Excludes rows with a null order_date before aggregation, then groups by
    order_date to produce order volume and revenue metrics.
    """
    filtered_df = df.filter(F.col("order_date").isNotNull())

    return (
        filtered_df.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_total").alias("gross_revenue"),
            F.sum("shipping_amount").alias("total_shipping_amount"),
            F.sum("discount_amount").alias("total_discount_amount"),
        )
        .select(
            "order_date",
            "total_orders",
            "gross_revenue",
            "total_shipping_amount",
            "total_discount_amount",
        )
        .orderBy("order_date")
    )
