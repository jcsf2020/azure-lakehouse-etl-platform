"""Gold fact table helpers for fact_order_items.

Grain: one row per order item line (order_item_id).

Natural / degenerate keys carried forward from the transactional source:
  order_item_id  — grain identifier
  order_id       — degenerate dimension from the originating order

Dimension foreign keys resolved at build time:
  date_key       — FK to gold.dim_date     (equi-join on order_date = full_date)
  channel_key    — FK to gold.dim_channel  (equi-join on conformed channel name)
  customer_key   — FK to gold.dim_customer (SCD2 point-in-time join on order_date)
  product_key    — FK to gold.dim_product  (SCD2 point-in-time join on order_date)

Measures:
  quantity, unit_price, discount_amount, line_total

SCD2 point-in-time join pattern:
  The join does NOT filter on scd_is_current.  The date window
  [scd_effective_start, scd_effective_end] is the authoritative mechanism for
  version selection.  This correctly resolves historical orders against the
  dimension version that was active on the order date, and is compatible with
  future incremental SCD2 pipelines that will introduce expired versions.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from azure_lakehouse_etl.gold_dimensional_foundations import (
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
    normalize_channel_col,
)


SILVER_ORDER_ITEMS_TABLE = "silver.order_items"
SILVER_ORDERS_TABLE = "silver.orders"
GOLD_TABLE = "gold.fact_order_items"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/fact_order_items"

FACT_ORDER_ITEMS_COLUMNS: tuple[str, ...] = (
    "order_item_id",    # grain key / degenerate identifier
    "order_id",         # degenerate dimension — visible from originating order
    "date_key",         # FK → gold.dim_date
    "channel_key",      # FK → gold.dim_channel
    "customer_key",     # FK → gold.dim_customer (SCD2 point-in-time)
    "product_key",      # FK → gold.dim_product  (SCD2 point-in-time)
    "quantity",         # measure
    "unit_price",       # measure
    "discount_amount",  # measure
    "line_total",       # measure
)


def build_fact_order_items(
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_date_df: DataFrame,
    dim_channel_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    """Build gold.fact_order_items from Silver sources and Gold dimension tables.

    Grain: one row per order item line (order_item_id).

    Join strategy — all dimension joins are LEFT so that unresolvable lookups
    produce a NULL foreign key rather than silently dropping fact rows:

    1. silver.order_items → silver.orders  : equi-join on order_id
                                              resolves order_date, customer_id, channel
    2.                    → gold.dim_date  : equi-join on order_date = full_date
    3.                    → gold.dim_channel : equi-join on conformed channel name
    4.                    → gold.dim_customer : SCD2 point-in-time join —
                                              order_date within [scd_effective_start, scd_effective_end]
    5.                    → gold.dim_product  : same SCD2 point-in-time pattern

    Parameters
    ----------
    silver_order_items_df : Silver order_items DataFrame
    silver_orders_df      : Silver orders DataFrame (must include channel column)
    dim_date_df           : gold.dim_date DataFrame
    dim_channel_df        : gold.dim_channel DataFrame
    dim_customer_df       : gold.dim_customer DataFrame (may contain multiple SCD2 versions)
    dim_product_df        : gold.dim_product DataFrame  (may contain multiple SCD2 versions)

    Returns
    -------
    DataFrame with columns matching FACT_ORDER_ITEMS_COLUMNS, ordered by order_item_id.
    """
    # --- Step 1: Enrich order_items with order context ---
    # Only bring in the columns needed from Silver orders.
    order_context = silver_orders_df.select(
        "order_id",
        "order_date",
        "customer_id",
        F.col("channel").alias("_raw_channel"),
    )

    base = (
        silver_order_items_df
        .select(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "line_total",
        )
        .join(order_context, on="order_id", how="left")
        .withColumn("_conformed_channel", normalize_channel_col("_raw_channel"))
    )

    # --- Step 2: Resolve date_key via equi-join on order_date = full_date ---
    date_lookup = dim_date_df.select("full_date", "date_key")
    pre_date = base
    base = (
        pre_date
        .join(date_lookup, on=pre_date["order_date"] == date_lookup["full_date"], how="left")
        .drop("full_date")
    )

    # --- Step 3: Resolve channel_key via equi-join on conformed channel name ---
    channel_lookup = dim_channel_df.select("channel_name", "channel_key")
    pre_channel = base
    base = (
        pre_channel
        .join(
            channel_lookup,
            on=pre_channel["_conformed_channel"] == channel_lookup["channel_name"],
            how="left",
        )
        .drop("channel_name")
    )

    # --- Step 4: Resolve customer_key via SCD2 point-in-time join ---
    # Finds the dimension version whose effective window contains the order_date.
    # Does NOT filter on scd_is_current — see module docstring.
    cust_lookup = dim_customer_df.select(
        F.col("customer_id").alias("_dim_cust_id"),
        F.col("customer_key"),
        F.col(SCD2_EFFECTIVE_START).alias("_cust_eff_start"),
        F.col(SCD2_EFFECTIVE_END).alias("_cust_eff_end"),
    )
    pre_cust = base
    base = (
        pre_cust
        .join(
            cust_lookup,
            on=(
                (pre_cust["customer_id"] == cust_lookup["_dim_cust_id"])
                & (pre_cust["order_date"] >= cust_lookup["_cust_eff_start"])
                & (pre_cust["order_date"] <= cust_lookup["_cust_eff_end"])
            ),
            how="left",
        )
        .drop("_dim_cust_id", "_cust_eff_start", "_cust_eff_end")
    )

    # --- Step 5: Resolve product_key via SCD2 point-in-time join ---
    prod_lookup = dim_product_df.select(
        F.col("product_id").alias("_dim_prod_id"),
        F.col("product_key"),
        F.col(SCD2_EFFECTIVE_START).alias("_prod_eff_start"),
        F.col(SCD2_EFFECTIVE_END).alias("_prod_eff_end"),
    )
    pre_prod = base
    base = (
        pre_prod
        .join(
            prod_lookup,
            on=(
                (pre_prod["product_id"] == prod_lookup["_dim_prod_id"])
                & (pre_prod["order_date"] >= prod_lookup["_prod_eff_start"])
                & (pre_prod["order_date"] <= prod_lookup["_prod_eff_end"])
            ),
            how="left",
        )
        .drop("_dim_prod_id", "_prod_eff_start", "_prod_eff_end")
    )

    return base.select(*FACT_ORDER_ITEMS_COLUMNS).orderBy("order_item_id")
