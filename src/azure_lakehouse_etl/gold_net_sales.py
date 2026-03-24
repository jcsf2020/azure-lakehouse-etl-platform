"""Gold aggregation helpers for net sales."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DAILY_SALES_TABLE = "gold.daily_sales"
DAILY_RETURNS_TABLE = "gold.daily_returns"
GOLD_TABLE = "gold.net_sales"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/net_sales"


def build_net_sales(
    daily_sales_df: DataFrame,
    daily_returns_df: DataFrame,
) -> DataFrame:
    """Join daily sales to daily returns and compute net revenue per day.

    Performs a left join from daily_sales onto daily_returns on the date
    dimension (order_date = return_date).  Where no matching returns exist,
    refund_amount defaults to 0.0.  net_revenue is gross_revenue minus
    refund_amount.
    """
    returns_df = daily_returns_df.select(
        F.col("return_date"),
        F.col("total_refund_amount"),
    )

    joined_df = daily_sales_df.join(
        returns_df,
        on=daily_sales_df["order_date"] == returns_df["return_date"],
        how="left",
    )

    return (
        joined_df.select(
            daily_sales_df["order_date"],
            daily_sales_df["gross_revenue"],
            F.coalesce(returns_df["total_refund_amount"], F.lit(0.0)).alias(
                "refund_amount"
            ),
        )
        .withColumn(
            "net_revenue",
            F.col("gross_revenue") - F.col("refund_amount"),
        )
        .select(
            "order_date",
            "gross_revenue",
            "refund_amount",
            "net_revenue",
        )
        .orderBy("order_date")
    )
