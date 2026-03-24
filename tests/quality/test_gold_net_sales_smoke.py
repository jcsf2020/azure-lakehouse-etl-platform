"""Local smoke checks for the Gold net_sales aggregation."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.gold_net_sales import (
    DAILY_RETURNS_TABLE,
    DAILY_SALES_TABLE,
    GOLD_PATH,
    GOLD_TABLE,
    build_net_sales,
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
        .appName("test_gold_net_sales_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_module_exports_expected_constants() -> None:
    """Gold module must expose the four table/path constants."""
    assert DAILY_SALES_TABLE == "gold.daily_sales"
    assert DAILY_RETURNS_TABLE == "gold.daily_returns"
    assert GOLD_TABLE == "gold.net_sales"
    assert "gold/net_sales" in GOLD_PATH


def test_build_net_sales_is_callable() -> None:
    """build_net_sales must accept daily_sales_df and daily_returns_df arguments."""
    sig = inspect.signature(build_net_sales)
    params = list(sig.parameters)
    assert params == ["daily_sales_df", "daily_returns_df"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke test (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_net_sales_join_and_net_revenue() -> None:
    """Net sales logic should left-join by date and compute net_revenue correctly."""
    spark = _local_spark()

    sales_rows = [
        {"order_date": "2024-01-01", "gross_revenue": 300.0},
        {"order_date": "2024-01-02", "gross_revenue": 150.0},
        # Day with no matching returns
        {"order_date": "2024-01-03", "gross_revenue": 80.0},
    ]

    returns_rows = [
        {"return_date": "2024-01-01", "total_refund_amount": 50.0},
        {"return_date": "2024-01-02", "total_refund_amount": 30.0},
        # No return for 2024-01-03
    ]

    sales_df = spark.createDataFrame(sales_rows)
    returns_df = spark.createDataFrame(returns_rows)

    result_df = build_net_sales(sales_df, returns_df)
    rows = {r["order_date"]: r for r in result_df.collect()}

    # All three sales dates must be present (left join keeps unmatched sales)
    assert set(rows.keys()) == {"2024-01-01", "2024-01-02", "2024-01-03"}

    # Day with matching return
    day1 = rows["2024-01-01"]
    assert day1["gross_revenue"] == pytest.approx(300.0)
    assert day1["refund_amount"] == pytest.approx(50.0)
    assert day1["net_revenue"] == pytest.approx(250.0)

    day2 = rows["2024-01-02"]
    assert day2["gross_revenue"] == pytest.approx(150.0)
    assert day2["refund_amount"] == pytest.approx(30.0)
    assert day2["net_revenue"] == pytest.approx(120.0)

    # Day with no matching return — refund_amount must default to 0.0
    day3 = rows["2024-01-03"]
    assert day3["gross_revenue"] == pytest.approx(80.0)
    assert day3["refund_amount"] == pytest.approx(0.0)
    assert day3["net_revenue"] == pytest.approx(80.0)

    # Output must be ordered by order_date
    dates = [r["order_date"] for r in result_df.collect()]
    assert dates == sorted(dates)
