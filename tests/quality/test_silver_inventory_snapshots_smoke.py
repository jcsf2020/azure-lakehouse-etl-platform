"""Local smoke checks for the Silver inventory snapshots transformation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.silver_inventory_snapshots import (
    BRONZE_TABLE,
    SILVER_PATH,
    SILVER_TABLE,
    apply_schema_and_basic_quality,
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
        .appName("test_silver_inventory_snapshots_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_silver_module_exports_expected_constants() -> None:
    """Silver module must expose the three table/path constants."""
    assert BRONZE_TABLE == "bronze.inventory_snapshots_raw"
    assert SILVER_TABLE == "silver.inventory_snapshots"
    assert "silver/inventory_snapshots" in SILVER_PATH


def test_apply_schema_and_basic_quality_is_callable() -> None:
    """apply_schema_and_basic_quality must accept a single DataFrame argument."""
    sig = inspect.signature(apply_schema_and_basic_quality)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_silver_inventory_snapshots_applies_expected_filters_and_deduplication() -> None:
    """Silver logic should type, filter, and deduplicate inventory snapshots."""
    spark = _local_spark()

    input_rows = [
        {
            "snapshot_date": "2026-03-15",
            "warehouse_id": "WH_LIS_01",
            "product_id": "SKU-1001",
            "stock_on_hand": "120",
            "units_received": "25",
            "units_sold": "18",
            "units_returned": "1",
        },
        # duplicate — should be dropped
        {
            "snapshot_date": "2026-03-15",
            "warehouse_id": "WH_LIS_01",
            "product_id": "SKU-1001",
            "stock_on_hand": "120",
            "units_received": "25",
            "units_sold": "18",
            "units_returned": "1",
        },
        # negative stock_on_hand — should be filtered out
        {
            "snapshot_date": "2026-03-16",
            "warehouse_id": "WH_PRT_01",
            "product_id": "SKU-1003",
            "stock_on_hand": "-1",
            "units_received": "10",
            "units_sold": "5",
            "units_returned": "0",
        },
        # null snapshot_date — should be filtered out
        {
            "snapshot_date": None,
            "warehouse_id": "WH_PRT_01",
            "product_id": "SKU-1002",
            "stock_on_hand": "70",
            "units_received": "8",
            "units_sold": "4",
            "units_returned": "0",
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = apply_schema_and_basic_quality(input_df)

    assert result_df.count() == 1

    row = result_df.select(
        "snapshot_date",
        "warehouse_id",
        "product_id",
        "stock_on_hand",
        "units_received",
        "units_sold",
        "units_returned",
    ).collect()[0]

    assert str(row["snapshot_date"]) == "2026-03-15"
    assert row["warehouse_id"] == "WH_LIS_01"
    assert row["product_id"] == "SKU-1001"
    assert row["stock_on_hand"] == 120
    assert row["units_received"] == 25
    assert row["units_sold"] == 18
    assert row["units_returned"] == 1
