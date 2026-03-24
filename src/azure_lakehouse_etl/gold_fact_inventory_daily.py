"""Gold fact table helpers for fact_inventory_daily.

Grain: one row per (snapshot_date, warehouse_id, product_id).

The grain reflects exactly the deduplication key enforced by the Silver
inventory_snapshots layer.  No warehouse dimension exists in this project;
warehouse_id is carried as a degenerate identifier.

Dimension foreign keys resolved at build time:
  date_key     — FK to gold.dim_date     (equi-join on snapshot_date = full_date)
  product_key  — FK to gold.dim_product  (SCD2 point-in-time join on snapshot_date)

Degenerate identifiers carried forward from the Silver source:
  snapshot_date — the business date of the inventory reading (also used for joins)
  warehouse_id  — storage location; no warehouse dimension defined in this project
  product_id    — product business key; carried alongside product_key for traceability

Measures:
  stock_on_hand   — inventory level at close of snapshot date
  units_received  — inbound units on this date
  units_sold      — outbound units on this date
  units_returned  — returned units on this date

SCD2 point-in-time join pattern:
  The join does NOT filter on scd_is_current.  The date window
  [scd_effective_start, scd_effective_end] is the authoritative mechanism for
  version selection.  This correctly resolves inventory snapshots against the
  dimension version that was active on the snapshot date, and is compatible with
  future incremental SCD2 pipelines that will introduce expired versions.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from azure_lakehouse_etl.gold_dimensional_foundations import (
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
)


SILVER_INVENTORY_SNAPSHOTS_TABLE = "silver.inventory_snapshots"
GOLD_TABLE = "gold.fact_inventory_daily"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/fact_inventory_daily"

FACT_INVENTORY_DAILY_COLUMNS: tuple[str, ...] = (
    "snapshot_date",    # grain — business date of the snapshot
    "warehouse_id",     # grain — storage location (degenerate; no warehouse dimension)
    "product_id",       # grain — product business key (degenerate identifier)
    "date_key",         # FK → gold.dim_date
    "product_key",      # FK → gold.dim_product (SCD2 point-in-time)
    "stock_on_hand",    # measure — inventory level at end of snapshot date
    "units_received",   # measure — inbound units
    "units_sold",       # measure — outbound units
    "units_returned",   # measure — returned units
)


def build_fact_inventory_daily(
    silver_inventory_snapshots_df: DataFrame,
    dim_date_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:
    """Build gold.fact_inventory_daily from Silver inventory_snapshots and Gold dimensions.

    Grain: one row per (snapshot_date, warehouse_id, product_id).

    Join strategy — all dimension joins are LEFT so that unresolvable lookups
    produce a NULL foreign key rather than silently dropping fact rows:

    1. silver.inventory_snapshots → gold.dim_date    : equi-join on snapshot_date = full_date
    2.                            → gold.dim_product : SCD2 point-in-time join —
                                                       snapshot_date within
                                                       [scd_effective_start, scd_effective_end]

    Parameters
    ----------
    silver_inventory_snapshots_df : Silver inventory_snapshots DataFrame
    dim_date_df                   : gold.dim_date DataFrame
    dim_product_df                : gold.dim_product DataFrame (may contain multiple SCD2 versions)

    Returns
    -------
    DataFrame with columns matching FACT_INVENTORY_DAILY_COLUMNS, ordered by
    snapshot_date, warehouse_id, product_id.
    """
    base = silver_inventory_snapshots_df.select(
        "snapshot_date",
        "warehouse_id",
        "product_id",
        "stock_on_hand",
        "units_received",
        "units_sold",
        "units_returned",
    )

    # --- Step 1: Resolve date_key via equi-join on snapshot_date = full_date ---
    date_lookup = dim_date_df.select("full_date", "date_key")
    pre_date = base
    base = (
        pre_date
        .join(date_lookup, on=pre_date["snapshot_date"] == date_lookup["full_date"], how="left")
        .drop("full_date")
    )

    # --- Step 2: Resolve product_key via SCD2 point-in-time join ---
    # Finds the dimension version whose effective window contains the snapshot_date.
    # Does NOT filter on scd_is_current — see module docstring.
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
                & (pre_prod["snapshot_date"] >= prod_lookup["_prod_eff_start"])
                & (pre_prod["snapshot_date"] <= prod_lookup["_prod_eff_end"])
            ),
            how="left",
        )
        .drop("_dim_prod_id", "_prod_eff_start", "_prod_eff_end")
    )

    return (
        base
        .select(*FACT_INVENTORY_DAILY_COLUMNS)
        .orderBy("snapshot_date", "warehouse_id", "product_id")
    )
