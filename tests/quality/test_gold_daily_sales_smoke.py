"""Local smoke checks for the Gold daily_sales aggregation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.gold_daily_sales import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_daily_sales,
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
        .appName("test_gold_daily_sales_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_module_exports_expected_constants() -> None:
    """Gold module must expose the three table/path constants."""
    assert SILVER_TABLE == "silver.orders"
    assert GOLD_TABLE == "gold.daily_sales"
    assert "gold/daily_sales" in GOLD_PATH


def test_build_daily_sales_is_callable() -> None:
    """build_daily_sales must accept a single DataFrame argument."""
    sig = inspect.signature(build_daily_sales)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_daily_sales_aggregates_correctly() -> None:
    """Gold logic should exclude nulls, aggregate by day, and sum correctly."""
    spark = _local_spark()

    input_rows = [
        # Day 1 — two distinct orders
        {
            "order_id": "ORD-001",
            "order_date": "2024-01-01",
            "order_total": 100.00,
            "shipping_amount": 5.00,
            "discount_amount": 10.00,
        },
        {
            "order_id": "ORD-002",
            "order_date": "2024-01-01",
            "order_total": 200.00,
            "shipping_amount": 8.00,
            "discount_amount": 0.00,
        },
        # Day 1 — duplicate order_id, should count only once toward total_orders
        {
            "order_id": "ORD-001",
            "order_date": "2024-01-01",
            "order_total": 100.00,
            "shipping_amount": 5.00,
            "discount_amount": 10.00,
        },
        # Day 2 — one order
        {
            "order_id": "ORD-003",
            "order_date": "2024-01-02",
            "order_total": 50.00,
            "shipping_amount": 3.00,
            "discount_amount": 5.00,
        },
        # Null order_date — must be excluded
        {
            "order_id": "ORD-004",
            "order_date": None,
            "order_total": 999.00,
            "shipping_amount": 99.00,
            "discount_amount": 99.00,
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = build_daily_sales(input_df)
    rows = {r["order_date"]: r for r in result_df.collect()}

    # Only two dates should appear (null row excluded)
    assert set(rows.keys()) == {"2024-01-01", "2024-01-02"}

    day1 = rows["2024-01-01"]
    assert day1["total_orders"] == 2  # ORD-001 and ORD-002 (duplicate not counted)
    assert day1["gross_revenue"] == pytest.approx(400.00)
    assert day1["total_shipping_amount"] == pytest.approx(18.00)
    assert day1["total_discount_amount"] == pytest.approx(20.00)

    day2 = rows["2024-01-02"]
    assert day2["total_orders"] == 1
    assert day2["gross_revenue"] == pytest.approx(50.00)
    assert day2["total_shipping_amount"] == pytest.approx(3.00)
    assert day2["total_discount_amount"] == pytest.approx(5.00)
