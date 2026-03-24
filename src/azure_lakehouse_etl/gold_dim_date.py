"""Gold dimensional table helpers for dim_date."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_dimensional_foundations import (
    DATE_DIM_COLUMNS,
    build_date_dimension,
)


GOLD_TABLE = "gold.dim_date"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/dim_date"

DEFAULT_START_DATE = "2020-01-01"
DEFAULT_END_DATE = "2030-12-31"


def build_dim_date(
    spark: SparkSession,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
) -> DataFrame:
    """Build the Gold dim_date dimensional table.

    Wraps the date dimension foundation helper to produce the final
    gold.dim_date table with one row per calendar date.  Output is
    ordered by full_date ascending and exposes all DATE_DIM_COLUMNS.

    Parameters
    ----------
    spark      : active SparkSession
    start_date : ISO string 'YYYY-MM-DD' (inclusive, defaults to 2020-01-01)
    end_date   : ISO string 'YYYY-MM-DD' (inclusive, defaults to 2030-12-31)

    Returns
    -------
    DataFrame with columns matching DATE_DIM_COLUMNS, ordered by full_date.

    Raises
    ------
    ValueError if end_date is before start_date.
    """
    return build_date_dimension(spark, start_date, end_date)
