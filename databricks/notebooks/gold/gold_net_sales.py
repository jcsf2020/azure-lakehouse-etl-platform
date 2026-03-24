"""Gold aggregation for net sales — Databricks notebook entrypoint.

This module reads from the Gold daily_sales and daily_returns datasets and
produces a net sales summary: gross revenue reduced by any refund amounts
matched on order_date, with net_revenue computed as the difference.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DAILY_SALES_TABLE = "gold.daily_sales"
DAILY_RETURNS_TABLE = "gold.daily_returns"
GOLD_TABLE = "gold.net_sales"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/net_sales"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_net_sales").getOrCreate()


def read_gold_daily_sales(spark: SparkSession) -> DataFrame:
    """Read the Gold daily sales dataset."""
    return spark.table(DAILY_SALES_TABLE)


def read_gold_daily_returns(spark: SparkSession) -> DataFrame:
    """Read the Gold daily returns dataset."""
    return spark.table(DAILY_RETURNS_TABLE)


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
