"""Local smoke checks for the Silver orders transformation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.silver_orders import (
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
        .appName("test_silver_orders_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_silver_module_exports_expected_constants() -> None:
    """Silver module must expose the three table/path constants."""
    assert BRONZE_TABLE == "bronze.orders_raw"
    assert SILVER_TABLE == "silver.orders"
    assert "silver/orders" in SILVER_PATH


def test_apply_schema_and_basic_quality_is_callable() -> None:
    """apply_schema_and_basic_quality must accept a single DataFrame argument."""
    sig = inspect.signature(apply_schema_and_basic_quality)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_silver_orders_applies_expected_filters_and_deduplication() -> None:
    """Silver logic should type, filter, and deduplicate orders."""
    spark = _local_spark()

    input_rows = [
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "order_status": "completed",
            "payment_method": "credit_card",
            "currency": "USD",
            "order_date": "2024-01-15",
            "order_total": 99.99,
            "shipping_amount": 5.00,
            "discount_amount": 10.00,
        },
        # duplicate order_id — should be dropped
        {
            "order_id": "ORD-001",
            "customer_id": "CUST-001",
            "order_status": "completed",
            "payment_method": "credit_card",
            "currency": "USD",
            "order_date": "2024-01-15",
            "order_total": 99.99,
            "shipping_amount": 5.00,
            "discount_amount": 10.00,
        },
        # null customer_id — should be filtered out
        {
            "order_id": "ORD-002",
            "customer_id": None,
            "order_status": "pending",
            "payment_method": "paypal",
            "currency": "USD",
            "order_date": "2024-02-01",
            "order_total": 49.99,
            "shipping_amount": 3.00,
            "discount_amount": 0.00,
        },
        # negative order_total — should be filtered out
        {
            "order_id": "ORD-003",
            "customer_id": "CUST-003",
            "order_status": "refunded",
            "payment_method": "credit_card",
            "currency": "USD",
            "order_date": "2024-03-10",
            "order_total": -20.00,
            "shipping_amount": 0.00,
            "discount_amount": 0.00,
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = apply_schema_and_basic_quality(input_df)

    assert result_df.count() == 1

    row = result_df.select(
        "order_id",
        "customer_id",
        "order_status",
        "payment_method",
        "currency",
        "order_date",
        "order_total",
        "shipping_amount",
        "discount_amount",
    ).collect()[0]

    assert row["order_id"] == "ORD-001"
    assert row["customer_id"] == "CUST-001"
    assert row["order_status"] == "completed"
    assert row["payment_method"] == "credit_card"
    assert row["currency"] == "USD"
    assert row["order_date"] is not None
    assert row["order_total"] == 99.99
    assert row["shipping_amount"] == 5.00
    assert row["discount_amount"] == 10.00
