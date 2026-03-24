"""Gold aggregation for daily sales.

This module reads from the Silver orders dataset and produces the first
Gold-layer aggregate: a daily sales summary grouped by order_date, covering
order volume, gross revenue, shipping, and discounts.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_TABLE = "silver.orders"
GOLD_TABLE = "gold.daily_sales"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/daily_sales"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_daily_sales").getOrCreate()


def read_silver_orders(spark: SparkSession) -> DataFrame:
    """Read the Silver orders dataset."""
    return spark.table(SILVER_TABLE)


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
