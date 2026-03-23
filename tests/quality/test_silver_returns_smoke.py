"""Local smoke checks for the Silver returns transformation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.silver_returns import (
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
        .appName("test_silver_returns_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_silver_module_exports_expected_constants() -> None:
    """Silver module must expose the three table/path constants."""
    assert BRONZE_TABLE == "bronze.returns_raw"
    assert SILVER_TABLE == "silver.returns"
    assert "silver/returns" in SILVER_PATH


def test_apply_schema_and_basic_quality_is_callable() -> None:
    """apply_schema_and_basic_quality must accept a single DataFrame argument."""
    sig = inspect.signature(apply_schema_and_basic_quality)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_silver_returns_applies_expected_filters_and_deduplication() -> None:
    """Silver logic should type, filter, and deduplicate returns."""
    spark = _local_spark()

    input_rows = [
        {
            "return_id": "RET-001",
            "return_date": "2026-03-10",
            "order_item_id": "OI-501",
            "product_id": "SKU-1001",
            "customer_id": "CUST-42",
            "return_reason": "damaged",
            "refund_amount": "49.99",
        },
        # duplicate return_id — should be dropped
        {
            "return_id": "RET-001",
            "return_date": "2026-03-10",
            "order_item_id": "OI-501",
            "product_id": "SKU-1001",
            "customer_id": "CUST-42",
            "return_reason": "damaged",
            "refund_amount": "49.99",
        },
        # negative refund_amount — should be filtered out
        {
            "return_id": "RET-002",
            "return_date": "2026-03-11",
            "order_item_id": "OI-502",
            "product_id": "SKU-1002",
            "customer_id": "CUST-43",
            "return_reason": "wrong item",
            "refund_amount": "-5.00",
        },
        # null return_date — should be filtered out
        {
            "return_id": "RET-003",
            "return_date": None,
            "order_item_id": "OI-503",
            "product_id": "SKU-1003",
            "customer_id": "CUST-44",
            "return_reason": "not needed",
            "refund_amount": "20.00",
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = apply_schema_and_basic_quality(input_df)

    assert result_df.count() == 1

    row = result_df.select(
        "return_id",
        "return_date",
        "order_item_id",
        "product_id",
        "customer_id",
        "return_reason",
        "refund_amount",
    ).collect()[0]

    assert row["return_id"] == "RET-001"
    assert str(row["return_date"]) == "2026-03-10"
    assert row["order_item_id"] == "OI-501"
    assert row["product_id"] == "SKU-1001"
    assert row["customer_id"] == "CUST-42"
    assert row["return_reason"] == "damaged"
    assert row["refund_amount"] == 49.99
