"""Demo KPI queries — analytical result sets from local Gold demo outputs.

Reads materialized Gold parquet files from artifacts/demo_gold/ and produces
seven analytical result sets using Spark DataFrame aggregations.  No Databricks
or cloud services required.

Public API
----------
build_local_spark()        — re-exported from gold_demo_runner (macOS/local fix)
load_gold_tables()         — read all required Gold tables from a local directory
kpi_gross_sales_summary()  — total revenue, quantity, and discount from fact_order_items
kpi_returns_summary()      — total refunds and count from fact_returns
kpi_inventory_summary()    — aggregate stock levels from fact_inventory_daily
kpi_sales_by_channel()     — gross sales split by sales channel
kpi_refunds_by_reason()    — refund totals grouped by return_reason
kpi_stock_by_warehouse()   — stock on hand grouped by warehouse_id
kpi_net_sales_by_date()    — gross sales minus refunds per calendar date
build_kpi_results()        — run all seven KPIs, return dict[name, DataFrame]
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Re-export the macOS-compatible Spark builder so callers only need one import.
from azure_lakehouse_etl.gold_demo_runner import (  # noqa: F401
    DEFAULT_OUTPUT_DIR as DEFAULT_GOLD_DIR,
    build_local_spark,
)

# ---------------------------------------------------------------------------
# Required Gold table names
# ---------------------------------------------------------------------------

_REQUIRED_TABLES = (
    "fact_order_items",
    "fact_returns",
    "fact_inventory_daily",
    "dim_date",
    "dim_channel",
)


# ---------------------------------------------------------------------------
# Loader — read materialized Gold parquet files
# ---------------------------------------------------------------------------


def load_gold_tables(
    spark: SparkSession,
    gold_dir: Path = DEFAULT_GOLD_DIR,
) -> dict[str, DataFrame]:
    """Read required Gold tables from local parquet files.

    Parameters
    ----------
    spark    : active SparkSession
    gold_dir : directory containing one sub-folder per Gold table

    Returns
    -------
    dict mapping table name → DataFrame for each entry in _REQUIRED_TABLES.

    Raises
    ------
    FileNotFoundError if any expected table directory is missing.
    """
    gold_dir = Path(gold_dir)
    tables: dict[str, DataFrame] = {}
    for name in _REQUIRED_TABLES:
        table_path = gold_dir / name
        if not table_path.exists():
            raise FileNotFoundError(
                f"Gold table directory not found: {table_path}\n"
                "Run `python scripts/run_gold_demo.py` to materialise the Gold outputs."
            )
        tables[name] = spark.read.parquet(str(table_path))
    return tables


# ---------------------------------------------------------------------------
# KPI functions
# ---------------------------------------------------------------------------


def kpi_gross_sales_summary(fact_order_items: DataFrame) -> DataFrame:
    """Gross sales summary — total revenue, quantity sold, and discount applied.

    Returns one summary row.
    """
    return fact_order_items.agg(
        F.count("order_item_id").alias("total_line_items"),
        F.sum("quantity").alias("total_units_sold"),
        F.round(F.sum("line_total"), 2).alias("gross_revenue"),
        F.round(F.sum("discount_amount"), 2).alias("total_discount"),
    )


def kpi_returns_summary(fact_returns: DataFrame) -> DataFrame:
    """Returns summary — total refunds issued and return event count.

    Returns one summary row.
    """
    return fact_returns.agg(
        F.count("return_id").alias("total_returns"),
        F.round(F.sum("refund_amount"), 2).alias("total_refund_amount"),
    )


def kpi_inventory_summary(fact_inventory_daily: DataFrame) -> DataFrame:
    """Inventory summary — aggregate stock movements across all dates and locations.

    Returns one summary row.
    """
    return fact_inventory_daily.agg(
        F.sum("stock_on_hand").alias("total_stock_on_hand"),
        F.sum("units_received").alias("total_units_received"),
        F.sum("units_sold").alias("total_units_sold"),
        F.sum("units_returned").alias("total_units_returned"),
    )


def kpi_sales_by_channel(
    fact_order_items: DataFrame,
    dim_channel: DataFrame,
) -> DataFrame:
    """Gross sales split by sales channel.

    Joins fact_order_items to dim_channel to resolve the human-readable
    channel_label, then aggregates line_total and quantity per channel.
    Ordered by gross_revenue descending.
    """
    channel_lookup = dim_channel.select("channel_key", "channel_label", "channel_group")
    return (
        fact_order_items
        .join(channel_lookup, on="channel_key", how="left")
        .groupBy("channel_label", "channel_group")
        .agg(
            F.count("order_item_id").alias("line_items"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total"), 2).alias("gross_revenue"),
        )
        .orderBy(F.col("gross_revenue").desc())
    )


def kpi_refunds_by_reason(fact_returns: DataFrame) -> DataFrame:
    """Refund totals grouped by return reason.

    Ordered by total_refund_amount descending.
    """
    return (
        fact_returns
        .groupBy("return_reason")
        .agg(
            F.count("return_id").alias("return_count"),
            F.round(F.sum("refund_amount"), 2).alias("total_refund_amount"),
        )
        .orderBy(F.col("total_refund_amount").desc())
    )


def kpi_stock_by_warehouse(fact_inventory_daily: DataFrame) -> DataFrame:
    """Latest stock on hand grouped by warehouse_id.

    Aggregates the most recent snapshot_date per warehouse.  Where multiple
    products exist per warehouse, stock is summed at the warehouse level.
    Ordered by stock_on_hand descending.
    """
    latest_date = fact_inventory_daily.agg(F.max("snapshot_date")).collect()[0][0]
    return (
        fact_inventory_daily
        .filter(F.col("snapshot_date") == F.lit(latest_date))
        .groupBy("warehouse_id")
        .agg(
            F.sum("stock_on_hand").alias("stock_on_hand"),
            F.sum("units_received").alias("units_received"),
            F.sum("units_sold").alias("units_sold"),
        )
        .orderBy(F.col("stock_on_hand").desc())
    )


def kpi_net_sales_by_date(
    fact_order_items: DataFrame,
    fact_returns: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Net sales (gross revenue minus refunds) per calendar date.

    Aggregates gross sales from fact_order_items and total refunds from
    fact_returns, both keyed by date_key.  Joins to dim_date to resolve
    the human-readable full_date.  Net sales = gross_revenue - total_refunds.
    Ordered by full_date ascending.
    """
    date_lookup = dim_date.select("date_key", "full_date")

    gross = (
        fact_order_items
        .groupBy("date_key")
        .agg(F.round(F.sum("line_total"), 2).alias("gross_revenue"))
    )

    refunds = (
        fact_returns
        .groupBy("date_key")
        .agg(F.round(F.sum("refund_amount"), 2).alias("total_refunds"))
    )

    return (
        gross
        .join(refunds, on="date_key", how="left")
        .join(date_lookup, on="date_key", how="left")
        .withColumn("total_refunds", F.coalesce(F.col("total_refunds"), F.lit(0.0)))
        .withColumn(
            "net_sales",
            F.round(F.col("gross_revenue") - F.col("total_refunds"), 2),
        )
        .select("full_date", "gross_revenue", "total_refunds", "net_sales")
        .orderBy("full_date")
    )


# ---------------------------------------------------------------------------
# Builder — run all KPIs and return a named dict
# ---------------------------------------------------------------------------


def build_kpi_results(
    spark: SparkSession,
    gold_dir: Path = DEFAULT_GOLD_DIR,
) -> dict[str, DataFrame]:
    """Load Gold tables and run all seven KPI queries.

    Parameters
    ----------
    spark    : active SparkSession
    gold_dir : path to materialized Gold parquet outputs

    Returns
    -------
    dict mapping KPI name → result DataFrame, with keys:
      gross_sales_summary, returns_summary, inventory_summary,
      sales_by_channel, refunds_by_reason, stock_by_warehouse,
      net_sales_by_date
    """
    tables = load_gold_tables(spark, gold_dir)

    return {
        "gross_sales_summary": kpi_gross_sales_summary(tables["fact_order_items"]),
        "returns_summary":     kpi_returns_summary(tables["fact_returns"]),
        "inventory_summary":   kpi_inventory_summary(tables["fact_inventory_daily"]),
        "sales_by_channel":    kpi_sales_by_channel(tables["fact_order_items"], tables["dim_channel"]),
        "refunds_by_reason":   kpi_refunds_by_reason(tables["fact_returns"]),
        "stock_by_warehouse":  kpi_stock_by_warehouse(tables["fact_inventory_daily"]),
        "net_sales_by_date":   kpi_net_sales_by_date(
            tables["fact_order_items"],
            tables["fact_returns"],
            tables["dim_date"],
        ),
    }
