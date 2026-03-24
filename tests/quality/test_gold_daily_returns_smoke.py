"""Local smoke checks for the Gold daily_returns aggregation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.gold_daily_returns import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_daily_returns,
)


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
        .appName("test_gold_daily_returns_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_module_exports_expected_constants() -> None:
    """Gold module must expose the three table/path constants."""
    assert SILVER_TABLE == "silver.returns"
    assert GOLD_TABLE == "gold.daily_returns"
    assert "gold/daily_returns" in GOLD_PATH


def test_build_daily_returns_is_callable() -> None:
    """build_daily_returns must accept a single DataFrame argument."""
    sig = inspect.signature(build_daily_returns)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_daily_returns_aggregates_correctly() -> None:
    """Gold logic should exclude nulls, aggregate by day, and count correctly."""
    spark = _local_spark()

    input_rows = [
        # Day 1 — two distinct returns
        {
            "return_id": "RET-001",
            "return_date": "2024-01-01",
            "refund_amount": 50.00,
        },
        {
            "return_id": "RET-002",
            "return_date": "2024-01-01",
            "refund_amount": 30.00,
        },
        # Day 1 — duplicate return_id, should count only once toward total_returns
        {
            "return_id": "RET-001",
            "return_date": "2024-01-01",
            "refund_amount": 50.00,
        },
        # Day 2 — one return
        {
            "return_id": "RET-003",
            "return_date": "2024-01-02",
            "refund_amount": 20.00,
        },
        # Null return_date — must be excluded
        {
            "return_id": "RET-004",
            "return_date": None,
            "refund_amount": 999.00,
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = build_daily_returns(input_df)
    rows = {r["return_date"]: r for r in result_df.collect()}

    # Only two dates should appear (null row excluded)
    assert set(rows.keys()) == {"2024-01-01", "2024-01-02"}

    day1 = rows["2024-01-01"]
    assert day1["total_returns"] == 2  # RET-001 and RET-002 (duplicate not counted)
    assert day1["total_refund_amount"] == pytest.approx(130.00)

    day2 = rows["2024-01-02"]
    assert day2["total_returns"] == 1
    assert day2["total_refund_amount"] == pytest.approx(20.00)
