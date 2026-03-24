"""Local smoke checks for the Gold dim_date dimensional table."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.gold_dim_date import (
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    GOLD_PATH,
    GOLD_TABLE,
    build_dim_date,
)
from azure_lakehouse_etl.gold_dimensional_foundations import DATE_DIM_COLUMNS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _java_available() -> bool:
    """Return True only when a working JRE is reachable."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_java = pytest.mark.skipif(
    not _java_available(),
    reason="Java is not installed — Spark-dependent tests skipped",
)


def _local_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("test_gold_dim_date_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_dim_date_module_exports_expected_constants() -> None:
    """dim_date module must expose the two table/path constants and date range defaults."""
    assert GOLD_TABLE == "gold.dim_date"
    assert "gold/dim_date" in GOLD_PATH
    assert DEFAULT_START_DATE == "2020-01-01"
    assert DEFAULT_END_DATE == "2030-12-31"


def test_build_dim_date_signature() -> None:
    """build_dim_date must accept spark, start_date, and end_date parameters."""
    sig = inspect.signature(build_dim_date)
    assert list(sig.parameters) == ["spark", "start_date", "end_date"]


def test_build_dim_date_has_defaults_for_date_range() -> None:
    """start_date and end_date parameters must have default values."""
    sig = inspect.signature(build_dim_date)
    assert sig.parameters["start_date"].default == DEFAULT_START_DATE
    assert sig.parameters["end_date"].default == DEFAULT_END_DATE


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_dim_date_returns_expected_columns() -> None:
    """build_dim_date must return a DataFrame with exactly DATE_DIM_COLUMNS."""
    spark = _local_spark()
    df = build_dim_date(spark, "2024-01-01", "2024-01-07")

    assert set(df.columns) == set(DATE_DIM_COLUMNS)


@requires_java
def test_build_dim_date_deterministic_row_count() -> None:
    """build_dim_date must return exactly one row per calendar day in the range."""
    spark = _local_spark()

    # 7 days: 2024-01-01 to 2024-01-07
    df = build_dim_date(spark, "2024-01-01", "2024-01-07")
    assert df.count() == 7

    # 31 days: January 2024
    df_jan = build_dim_date(spark, "2024-01-01", "2024-01-31")
    assert df_jan.count() == 31

    # 29 days: February 2024 (leap year)
    df_feb = build_dim_date(spark, "2024-02-01", "2024-02-29")
    assert df_feb.count() == 29


@requires_java
def test_build_dim_date_date_key_is_unique() -> None:
    """date_key must be unique across the entire output."""
    spark = _local_spark()
    df = build_dim_date(spark, "2024-01-01", "2024-03-31")

    total = df.count()
    distinct = df.select("date_key").distinct().count()
    assert distinct == total


@requires_java
def test_build_dim_date_no_null_date_key() -> None:
    """date_key must never be null."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_dim_date(spark, "2024-01-01", "2024-01-31")

    null_count = df.filter(F.col("date_key").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_dim_date_date_key_format() -> None:
    """date_key must be an integer in YYYYMMDD form."""
    spark = _local_spark()
    df = build_dim_date(spark, "2024-06-15", "2024-06-15")

    row = df.collect()[0]
    assert row["date_key"] == 20240615


@requires_java
def test_build_dim_date_is_deterministic() -> None:
    """Two calls with identical parameters must produce identical date_key sets."""
    spark = _local_spark()
    run_a = {r["date_key"] for r in build_dim_date(spark, "2024-01-01", "2024-01-10").collect()}
    run_b = {r["date_key"] for r in build_dim_date(spark, "2024-01-01", "2024-01-10").collect()}
    assert run_a == run_b


@requires_java
def test_build_dim_date_ordered_by_full_date() -> None:
    """Output must be ordered ascending by full_date."""
    spark = _local_spark()
    df = build_dim_date(spark, "2024-01-01", "2024-01-05")
    keys = [r["date_key"] for r in df.collect()]
    assert keys == sorted(keys)
