"""Local smoke checks for the Silver order_items transformation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.silver_order_items import (
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
        .appName("test_silver_order_items_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_silver_module_exports_expected_constants() -> None:
    """Silver module must expose the three table/path constants."""
    assert BRONZE_TABLE == "bronze.order_items_raw"
    assert SILVER_TABLE == "silver.order_items"
    assert "silver/order_items" in SILVER_PATH


def test_apply_schema_and_basic_quality_is_callable() -> None:
    """apply_schema_and_basic_quality must accept a single DataFrame argument."""
    sig = inspect.signature(apply_schema_and_basic_quality)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_silver_order_items_applies_expected_filters_and_deduplication() -> None:
    """Silver logic should type, filter, and deduplicate order items."""
    spark = _local_spark()

    input_rows = [
        {
            "order_item_id": "ITEM-001",
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "quantity": 2,
            "unit_price": 49.99,
            "discount_amount": 5.00,
            "line_total": 94.98,
        },
        # duplicate order_item_id — should be dropped
        {
            "order_item_id": "ITEM-001",
            "order_id": "ORD-001",
            "product_id": "PROD-001",
            "quantity": 2,
            "unit_price": 49.99,
            "discount_amount": 5.00,
            "line_total": 94.98,
        },
        # null product_id — should be filtered out
        {
            "order_item_id": "ITEM-002",
            "order_id": "ORD-002",
            "product_id": None,
            "quantity": 1,
            "unit_price": 19.99,
            "discount_amount": 0.00,
            "line_total": 19.99,
        },
        # quantity <= 0 — should be filtered out
        {
            "order_item_id": "ITEM-003",
            "order_id": "ORD-003",
            "product_id": "PROD-003",
            "quantity": 0,
            "unit_price": 9.99,
            "discount_amount": 0.00,
            "line_total": 0.00,
        },
        # negative line_total — should be filtered out
        {
            "order_item_id": "ITEM-004",
            "order_id": "ORD-004",
            "product_id": "PROD-004",
            "quantity": 1,
            "unit_price": 9.99,
            "discount_amount": 0.00,
            "line_total": -9.99,
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = apply_schema_and_basic_quality(input_df)

    assert result_df.count() == 1

    row = result_df.select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total",
    ).collect()[0]

    assert row["order_item_id"] == "ITEM-001"
    assert row["order_id"] == "ORD-001"
    assert row["product_id"] == "PROD-001"
    assert row["quantity"] == 2
    assert row["unit_price"] == 49.99
    assert row["discount_amount"] == 5.00
    assert row["line_total"] == 94.98
