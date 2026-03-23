"""Local smoke checks for the Silver customers transformation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.silver_customers import (
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
        .appName("test_silver_customers_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_silver_module_exports_expected_constants() -> None:
    """Silver module must expose the three table/path constants."""
    assert BRONZE_TABLE == "bronze.customers_raw"
    assert SILVER_TABLE == "silver.customers"
    assert "silver/customers" in SILVER_PATH


def test_apply_schema_and_basic_quality_is_callable() -> None:
    """apply_schema_and_basic_quality must accept a single DataFrame argument."""
    sig = inspect.signature(apply_schema_and_basic_quality)
    params = list(sig.parameters)
    assert params == ["df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_silver_customers_applies_expected_filters_and_deduplication() -> None:
    """Silver logic should type, filter, and deduplicate customers."""
    spark = _local_spark()

    input_rows = [
        {
            "customer_id": "CUST-001",
            "first_name": "Alice",
            "last_name": "Smith",
            "email": "alice@example.com",
            "country": "US",
            "city": "New York",
            "registration_date": "2024-01-15",
            "customer_status": "active",
        },
        # duplicate customer_id — should be dropped
        {
            "customer_id": "CUST-001",
            "first_name": "Alice",
            "last_name": "Smith",
            "email": "alice@example.com",
            "country": "US",
            "city": "New York",
            "registration_date": "2024-01-15",
            "customer_status": "active",
        },
        # null email — should be filtered out
        {
            "customer_id": "CUST-002",
            "first_name": "Bob",
            "last_name": "Jones",
            "email": None,
            "country": "UK",
            "city": "London",
            "registration_date": "2024-02-20",
            "customer_status": "active",
        },
        # null registration_date — should be filtered out
        {
            "customer_id": "CUST-003",
            "first_name": "Carol",
            "last_name": "White",
            "email": "carol@example.com",
            "country": "CA",
            "city": "Toronto",
            "registration_date": None,
            "customer_status": "inactive",
        },
    ]

    input_df = spark.createDataFrame(input_rows)
    result_df = apply_schema_and_basic_quality(input_df)

    assert result_df.count() == 1

    row = result_df.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "registration_date",
        "customer_status",
    ).collect()[0]

    assert row["customer_id"] == "CUST-001"
    assert row["first_name"] == "Alice"
    assert row["last_name"] == "Smith"
    assert row["email"] == "alice@example.com"
    assert row["country"] == "US"
    assert row["city"] == "New York"
    assert str(row["registration_date"]) == "2024-01-15"
    assert row["customer_status"] == "active"
