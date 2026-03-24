"""Gold fact table helpers for fact_returns.

Grain: one row per return event (return_id).

Natural / degenerate keys carried forward from the transactional source:
  return_id      — grain identifier
  order_item_id  — degenerate identifier from the originating line item
  order_id       — degenerate dimension resolved via silver.order_items

Dimension foreign keys resolved at build time:
  date_key       — FK to gold.dim_date     (equi-join on return_date = full_date)
  channel_key    — FK to gold.dim_channel  (inherited from the originating order;
                                            equi-join on conformed channel name)
  customer_key   — FK to gold.dim_customer (SCD2 point-in-time join on return_date)
  product_key    — FK to gold.dim_product  (SCD2 point-in-time join on return_date)

Descriptor carried through:
  return_reason  — degenerate low-cardinality descriptor; useful for return-reason analysis

Measure:
  refund_amount

SCD2 point-in-time join pattern:
  The join does NOT filter on scd_is_current.  The date window
  [scd_effective_start, scd_effective_end] is the authoritative mechanism for
  version selection.  This correctly resolves historical returns against the
  dimension version that was active on the return date, and is compatible with
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


SILVER_RETURNS_TABLE = "silver.returns"
SILVER_ORDER_ITEMS_TABLE = "silver.order_items"
SILVER_ORDERS_TABLE = "silver.orders"
GOLD_TABLE = "gold.fact_returns"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/fact_returns"

FACT_RETURNS_COLUMNS: tuple[str, ...] = (
    "return_id",        # grain key
    "order_item_id",    # degenerate identifier — visible from originating line item
    "order_id",         # degenerate dimension  — resolved via silver.order_items
    "date_key",         # FK → gold.dim_date
    "channel_key",      # FK → gold.dim_channel (inherited from originating order)
    "customer_key",     # FK → gold.dim_customer (SCD2 point-in-time)
    "product_key",      # FK → gold.dim_product  (SCD2 point-in-time)
    "return_reason",    # degenerate descriptor
    "refund_amount",    # measure
)


def build_fact_returns(
    silver_returns_df: DataFrame,
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_date_df: DataFrame,
    dim_channel_df: DataFrame,
    dim_customer_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    """Build gold.fact_returns from Silver sources and Gold dimension tables.

    Grain: one row per return event (return_id).

    Join strategy — all dimension joins are LEFT so that unresolvable lookups
    produce a NULL foreign key rather than silently dropping fact rows:

    1. silver.returns → silver.order_items : LEFT join on order_item_id
                                             resolves order_id for each return line
    2.               → silver.orders       : LEFT join on order_id
                                             resolves channel for the originating order
    3.               → gold.dim_date       : equi-join on return_date = full_date
    4.               → gold.dim_channel    : equi-join on conformed channel name
    5.               → gold.dim_customer   : SCD2 point-in-time join —
                                             return_date within [scd_effective_start, scd_effective_end]
    6.               → gold.dim_product    : same SCD2 point-in-time pattern

    Parameters
    ----------
    silver_returns_df       : Silver returns DataFrame
    silver_order_items_df   : Silver order_items DataFrame (needed to resolve order_id)
    silver_orders_df        : Silver orders DataFrame (must include channel column)
    dim_date_df             : gold.dim_date DataFrame
    dim_channel_df          : gold.dim_channel DataFrame
    dim_customer_df         : gold.dim_customer DataFrame (may contain multiple SCD2 versions)
    dim_product_df          : gold.dim_product DataFrame  (may contain multiple SCD2 versions)

    Returns
    -------
    DataFrame with columns matching FACT_RETURNS_COLUMNS, ordered by return_id.
    """
    # --- Step 1: Resolve order_id from silver.order_items ---
    order_items_context = silver_order_items_df.select(
        F.col("order_item_id").alias("_oi_order_item_id"),
        F.col("order_id").alias("_oi_order_id"),
    )

    base = (
        silver_returns_df
        .select(
            "return_id",
            "return_date",
            "order_item_id",
            "product_id",
            "customer_id",
            "return_reason",
            "refund_amount",
        )
        .join(order_items_context, on=silver_returns_df["order_item_id"] == order_items_context["_oi_order_item_id"], how="left")
        .withColumn("order_id", F.col("_oi_order_id"))
        .drop("_oi_order_item_id", "_oi_order_id")
    )

    # --- Step 2: Resolve channel from silver.orders ---
    order_context = silver_orders_df.select(
        F.col("order_id").alias("_ord_order_id"),
        F.col("channel").alias("_raw_channel"),
    )

    pre_channel_norm = base
    base = (
        pre_channel_norm
        .join(order_context, on=pre_channel_norm["order_id"] == order_context["_ord_order_id"], how="left")
        .drop("_ord_order_id")
        .withColumn("_conformed_channel", normalize_channel_col("_raw_channel"))
        .drop("_raw_channel")
    )

    # --- Step 3: Resolve date_key via equi-join on return_date = full_date ---
    date_lookup = dim_date_df.select("full_date", "date_key")
    pre_date = base
    base = (
        pre_date
        .join(date_lookup, on=pre_date["return_date"] == date_lookup["full_date"], how="left")
        .drop("full_date")
    )

    # --- Step 4: Resolve channel_key via equi-join on conformed channel name ---
    channel_lookup = dim_channel_df.select("channel_name", "channel_key")
    pre_ch = base
    base = (
        pre_ch
        .join(
            channel_lookup,
            on=pre_ch["_conformed_channel"] == channel_lookup["channel_name"],
            how="left",
        )
        .drop("channel_name", "_conformed_channel")
    )

    # --- Step 5: Resolve customer_key via SCD2 point-in-time join ---
    # Finds the dimension version whose effective window contains the return_date.
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
                & (pre_cust["return_date"] >= cust_lookup["_cust_eff_start"])
                & (pre_cust["return_date"] <= cust_lookup["_cust_eff_end"])
            ),
            how="left",
        )
        .drop("_dim_cust_id", "_cust_eff_start", "_cust_eff_end")
    )

    # --- Step 6: Resolve product_key via SCD2 point-in-time join ---
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
                & (pre_prod["return_date"] >= prod_lookup["_prod_eff_start"])
                & (pre_prod["return_date"] <= prod_lookup["_prod_eff_end"])
            ),
            how="left",
        )
        .drop("_dim_prod_id", "_prod_eff_start", "_prod_eff_end")
    )

    return base.select(*FACT_RETURNS_COLUMNS).orderBy("return_id")
