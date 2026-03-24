"""Gold fact table for fact_order_items — Databricks notebook entrypoint.

Reads from silver.order_items, silver.orders, and the four conformed Gold
dimensions (dim_date, dim_channel, dim_customer, dim_product), resolves
dimension foreign keys via the reusable module helper (including SCD2
point-in-time joins for customer and product), and writes the final
gold.fact_order_items Delta table.

Grain: one row per order item line.
Load strategy: full-refresh.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_dim_channel import GOLD_TABLE as DIM_CHANNEL_TABLE
from azure_lakehouse_etl.gold_dim_customer import GOLD_TABLE as DIM_CUSTOMER_TABLE
from azure_lakehouse_etl.gold_dim_date import GOLD_TABLE as DIM_DATE_TABLE
from azure_lakehouse_etl.gold_dim_product import GOLD_TABLE as DIM_PRODUCT_TABLE
from azure_lakehouse_etl.gold_fact_order_items import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_ORDER_ITEMS_TABLE,
    SILVER_ORDERS_TABLE,
    build_fact_order_items,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_fact_order_items").getOrCreate()


def read_silver_order_items(spark: SparkSession) -> DataFrame:
    """Read the current Silver order_items table."""
    return spark.table(SILVER_ORDER_ITEMS_TABLE)


def read_silver_orders(spark: SparkSession) -> DataFrame:
    """Read the current Silver orders table."""
    return spark.table(SILVER_ORDERS_TABLE)


def read_dim_date(spark: SparkSession) -> DataFrame:
    """Read the Gold dim_date dimension."""
    return spark.table(DIM_DATE_TABLE)


def read_dim_channel(spark: SparkSession) -> DataFrame:
    """Read the Gold dim_channel dimension."""
    return spark.table(DIM_CHANNEL_TABLE)


def read_dim_customer(spark: SparkSession) -> DataFrame:
    """Read the Gold dim_customer dimension."""
    return spark.table(DIM_CUSTOMER_TABLE)


def read_dim_product(spark: SparkSession) -> DataFrame:
    """Read the Gold dim_product dimension."""
    return spark.table(DIM_PRODUCT_TABLE)


def write_gold_fact_order_items(df: DataFrame) -> None:
    """Write the Gold fact_order_items dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold fact_order_items build."""
    spark = get_spark()

    gold_df = build_fact_order_items(
        silver_order_items_df=read_silver_order_items(spark),
        silver_orders_df=read_silver_orders(spark),
        dim_date_df=read_dim_date(spark),
        dim_channel_df=read_dim_channel(spark),
        dim_customer_df=read_dim_customer(spark),
        dim_product_df=read_dim_product(spark),
    )

    write_gold_fact_order_items(gold_df)


if __name__ == "__main__":
    main()
