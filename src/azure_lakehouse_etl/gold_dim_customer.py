"""Gold dimensional table helpers for dim_customer (SCD2).

Grain: one row per customer version.

Business key  : customer_id    — visible in every row; must never change
Surrogate key : customer_key   — SHA-256(customer_id | scd_effective_start)
                                  unique across all versions of the same customer

Tracked attributes (a change in any of these opens a new SCD2 version):
  email, city, country, customer_status

Non-tracked attributes (stable — never trigger a new version):
  first_name, last_name, registration_date

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


SILVER_TABLE = "silver.customers"
GOLD_TABLE = "gold.dim_customer"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/dim_customer"

# Columns included in the SCD2 row hash for change detection.
# A change in any of these triggers a new version row.
TRACKED_ATTRIBUTES: tuple[str, ...] = (
    "email",
    "city",
    "country",
    "customer_status",
)

# Final column order for the Gold dimension.
DIM_CUSTOMER_COLUMNS: tuple[str, ...] = (
    "customer_key",       # surrogate key — identifies the versioned row
    "customer_id",        # business key  — remains visible in every version
    "first_name",
    "last_name",
    "email",
    "city",
    "country",
    "registration_date",
    "customer_status",
    *SCD2_TECHNICAL_COLUMNS,
)


def build_gold_dim_customer(
    silver_df: DataFrame,
    effective_start_date: date,
) -> DataFrame:
    """Build the Gold dim_customer SCD2 dimension from a Silver customers DataFrame.

    Produces the dimensional shape for an initial or full-refresh load.  Every
    input row is treated as a current active version: scd_is_current is True and
    scd_effective_end is set to the open-ended sentinel (9999-12-31).

    In a future incremental merge pipeline the caller would:
      1. Compare incoming scd_row_hash against the persisted active record's hash.
      2. Close expired versions (set scd_is_current=False, backfill scd_effective_end).
      3. UNION the new versions produced by this function.

    Parameters
    ----------
    silver_df            : cleaned Silver customers DataFrame
                           (output of silver_customers.apply_schema_and_basic_quality)
    effective_start_date : the business date on which these versions go live

    Returns
    -------
    DataFrame with columns matching DIM_CUSTOMER_COLUMNS, ordered by
    customer_id, scd_effective_start ascending.
    """
    scd_df = add_scd2_columns(
        silver_df,
        effective_start_date=effective_start_date,
        tracked_col_names=list(TRACKED_ATTRIBUTES),
    )

    return (
        scd_df
        .withColumn(
            "customer_key",
            surrogate_key("customer_id", SCD2_EFFECTIVE_START),
        )
        .select(*DIM_CUSTOMER_COLUMNS)
        .orderBy("customer_id", SCD2_EFFECTIVE_START)
    )
