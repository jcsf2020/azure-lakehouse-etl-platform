"""Local smoke checks for the Gold inventory_health aggregation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.gold_inventory_health import (
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    build_inventory_health,
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
        .appName("test_gold_inventory_health_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_module_exports_expected_constants() -> None:
    """Gold module must expose the three table/path constants."""
    assert SILVER_TABLE == "silver.inventory_snapshots"
    assert GOLD_TABLE == "gold.inventory_health"
    assert "gold/inventory_health" in GOLD_PATH


def test_build_inventory_health_is_callable() -> None:
    """build_inventory_health must accept a single DataFrame argument."""
    sig = inspect.signature(build_inventory_health)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_inventory_health_aggregates_correctly() -> None:
    """Gold logic should exclude nulls, aggregate by day, and count correctly."""
    spark = _local_spark()

    input_rows = [
        # Day 1 — two distinct products
        {
            "product_id": "P-001",
            "snapshot_date": "2024-01-01",
            "stock_on_hand": 100,
            "units_received": 10,
            "units_sold": 5,
            "units_returned": 1,
        },
        {
            "product_id": "P-002",
            "snapshot_date": "2024-01-01",
            "stock_on_hand": 200,
            "units_received": 20,
            "units_sold": 8,
            "units_returned": 2,
        },
        # Day 1 — duplicate product_id, should count only once toward total_products
        {
            "product_id": "P-001",
            "snapshot_date": "2024-01-01",
            "stock_on_hand": 50,
            "units_received": 5,
            "units_sold": 2,
            "units_returned": 0,
        },
        # Day 2 — one product
        {
            "product_id": "P-003",
            "snapshot_date": "2024-01-02",
            "stock_on_hand": 75,
            "units_received": 15,
            "units_sold": 3,
            "units_returned": 1,
        },
        # Null snapshot_date — must be excluded
        {
            "product_id": "P-999",
            "snapshot_date": None,
            "stock_on_hand": 999,
            "units_received": 999,
            "units_sold": 999,
            "units_returned": 999,
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = build_inventory_health(input_df)
    rows = {r["snapshot_date"]: r for r in result_df.collect()}

    # Only two dates should appear (null row excluded)
    assert set(rows.keys()) == {"2024-01-01", "2024-01-02"}

    day1 = rows["2024-01-01"]
    assert day1["total_products"] == 2  # P-001 and P-002 (duplicate not counted)
    assert day1["total_stock_on_hand"] == 350  # 100 + 200 + 50
    assert day1["total_units_received"] == 35   # 10 + 20 + 5
    assert day1["total_units_sold"] == 15       # 5 + 8 + 2
    assert day1["total_units_returned"] == 3    # 1 + 2 + 0

    day2 = rows["2024-01-02"]
    assert day2["total_products"] == 1
    assert day2["total_stock_on_hand"] == 75
    assert day2["total_units_received"] == 15
    assert day2["total_units_sold"] == 3
    assert day2["total_units_returned"] == 1
