"""Local smoke checks for the Silver products transformation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.silver_products import (
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
        .appName("test_silver_products_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_silver_module_exports_expected_constants() -> None:
    """Silver module must expose the three table/path constants."""
    assert BRONZE_TABLE == "bronze.products_raw"
    assert SILVER_TABLE == "silver.products"
    assert "silver/products" in SILVER_PATH


def test_apply_schema_and_basic_quality_is_callable() -> None:
    """apply_schema_and_basic_quality must accept a single DataFrame argument."""
    sig = inspect.signature(apply_schema_and_basic_quality)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_silver_products_applies_expected_filters_and_deduplication() -> None:
    """Silver logic should type, filter, and deduplicate products."""
    spark = _local_spark()

    input_rows = [
        {
            "product_id": "PROD-001",
            "product_name": "Widget A",
            "category": "Electronics",
            "subcategory": "Gadgets",
            "brand": "Acme",
            "list_price": 29.99,
            "cost_price": 12.50,
            "currency": "USD",
            "is_active": True,
            "last_updated": "2024-01-15T10:00:00",
        },
        # duplicate product_id — should be dropped
        {
            "product_id": "PROD-001",
            "product_name": "Widget A",
            "category": "Electronics",
            "subcategory": "Gadgets",
            "brand": "Acme",
            "list_price": 29.99,
            "cost_price": 12.50,
            "currency": "USD",
            "is_active": True,
            "last_updated": "2024-01-15T10:00:00",
        },
        # null product_name — should be filtered out
        {
            "product_id": "PROD-002",
            "product_name": None,
            "category": "Electronics",
            "subcategory": "Gadgets",
            "brand": "Acme",
            "list_price": 19.99,
            "cost_price": 8.00,
            "currency": "USD",
            "is_active": True,
            "last_updated": "2024-02-01T09:00:00",
        },
        # negative list_price — should be filtered out
        {
            "product_id": "PROD-003",
            "product_name": "Widget C",
            "category": "Electronics",
            "subcategory": "Gadgets",
            "brand": "Acme",
            "list_price": -5.00,
            "cost_price": 8.00,
            "currency": "USD",
            "is_active": False,
            "last_updated": "2024-03-10T08:00:00",
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = apply_schema_and_basic_quality(input_df)

    assert result_df.count() == 1

    row = result_df.select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "list_price",
        "cost_price",
        "currency",
        "is_active",
        "last_updated",
    ).collect()[0]

    assert row["product_id"] == "PROD-001"
    assert row["product_name"] == "Widget A"
    assert row["category"] == "Electronics"
    assert row["subcategory"] == "Gadgets"
    assert row["brand"] == "Acme"
    assert row["list_price"] == 29.99
    assert row["cost_price"] == 12.50
    assert row["currency"] == "USD"
    assert row["is_active"] is True
    assert row["last_updated"] is not None
