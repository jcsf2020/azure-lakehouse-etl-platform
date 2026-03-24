"""Gold dimensional table helpers for dim_product (SCD2).

Grain: one row per product version.

Business key  : product_id    — visible in every row; must never change
Surrogate key : product_key   — SHA-256(product_id | scd_effective_start)
                                  unique across all versions of the same product

Tracked attributes (a change in any of these opens a new SCD2 version):
  product_name, category, subcategory, brand, list_price, cost_price, is_active

Non-tracked attributes (stable — never trigger a new version):
  currency, last_updated

SCD2 technical columns (from gold_dimensional_foundations):
  scd_effective_start, scd_effective_end, scd_is_current, scd_row_hash
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame

from azure_lakehouse_etl.gold_dimensional_foundations import (
    SCD2_EFFECTIVE_START,
    SCD2_TECHNICAL_COLUMNS,
    add_scd2_columns,
    surrogate_key,
)


SILVER_TABLE = "silver.products"
GOLD_TABLE = "gold.dim_product"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/dim_product"

# Columns included in the SCD2 row hash for change detection.
# A change in any of these triggers a new version row.
TRACKED_ATTRIBUTES: tuple[str, ...] = (
    "product_name",
    "category",
    "subcategory",
    "brand",
    "list_price",
    "cost_price",
    "is_active",
)

# Final column order for the Gold dimension.
DIM_PRODUCT_COLUMNS: tuple[str, ...] = (
    "product_key",    # surrogate key — identifies the versioned row
    "product_id",     # business key  — remains visible in every version
    "product_name",
    "category",
    "subcategory",
    "brand",
    "list_price",
    "cost_price",
    "currency",
    "is_active",
    "last_updated",
    *SCD2_TECHNICAL_COLUMNS,
)


def build_gold_dim_product(
    silver_df: DataFrame,
    effective_start_date: date,
) -> DataFrame:
    """Build the Gold dim_product SCD2 dimension from a Silver products DataFrame.

    Produces the dimensional shape for an initial or full-refresh load.  Every
    input row is treated as a current active version: scd_is_current is True and
    scd_effective_end is set to the open-ended sentinel (9999-12-31).

    In a future incremental merge pipeline the caller would:
      1. Compare incoming scd_row_hash against the persisted active record's hash.
      2. Close expired versions (set scd_is_current=False, backfill scd_effective_end).
      3. UNION the new versions produced by this function.

    Parameters
    ----------
    silver_df            : cleaned Silver products DataFrame
                           (output of silver_products.apply_schema_and_basic_quality)
    effective_start_date : the business date on which these versions go live

    Returns
    -------
    DataFrame with columns matching DIM_PRODUCT_COLUMNS, ordered by
    product_id, scd_effective_start ascending.
    """
    scd_df = add_scd2_columns(
        silver_df,
        effective_start_date=effective_start_date,
        tracked_col_names=list(TRACKED_ATTRIBUTES),
    )

    return (
        scd_df
        .withColumn(
            "product_key",
            surrogate_key("product_id", SCD2_EFFECTIVE_START),
        )
        .select(*DIM_PRODUCT_COLUMNS)
        .orderBy("product_id", SCD2_EFFECTIVE_START)
    )
