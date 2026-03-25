"""Smoke tests for the demo KPI queries layer."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Java / Spark availability guard
# ---------------------------------------------------------------------------


def _java_available() -> bool:
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


# ---------------------------------------------------------------------------
# Session-scoped fixture: materialise Gold outputs into a temp directory
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def demo_gold_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Materialise Gold demo outputs into a temporary directory.

    Uses the real build_gold_datasets + write_demo_outputs pipeline so Spark
    tests run against genuine data without depending on pre-existing local
    artifacts.  Skipped automatically when Java / Spark is unavailable.
    """
    if not _java_available():
        pytest.skip("Java is not installed — Spark-dependent fixture skipped")

    from azure_lakehouse_etl.gold_demo_runner import (
        build_gold_datasets,
        build_local_spark,
        write_demo_outputs,
    )

    tmp_dir = tmp_path_factory.mktemp("demo_gold")
    spark = build_local_spark(app_name="test_kpi_fixture")
    try:
        datasets = build_gold_datasets(spark)
        write_demo_outputs(datasets, tmp_dir)
        yield tmp_dir
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_demo_kpi_queries_module_imports_cleanly() -> None:
    """demo_kpi_queries must be importable without starting Spark."""
    import azure_lakehouse_etl.demo_kpi_queries  # noqa: F401


def test_demo_kpi_queries_exports_public_api() -> None:
    """Module must expose the expected public functions and constants."""
    from azure_lakehouse_etl.demo_kpi_queries import (
        DEFAULT_GOLD_DIR,
        build_kpi_results,
        build_local_spark,
        kpi_gross_sales_summary,
        kpi_inventory_summary,
        kpi_net_sales_by_date,
        kpi_refunds_by_reason,
        kpi_returns_summary,
        kpi_sales_by_channel,
        kpi_stock_by_warehouse,
        load_gold_tables,
    )

    assert callable(build_local_spark)
    assert callable(load_gold_tables)
    assert callable(build_kpi_results)
    assert callable(kpi_gross_sales_summary)
    assert callable(kpi_returns_summary)
    assert callable(kpi_inventory_summary)
    assert callable(kpi_sales_by_channel)
    assert callable(kpi_refunds_by_reason)
    assert callable(kpi_stock_by_warehouse)
    assert callable(kpi_net_sales_by_date)
    assert "artifacts" in str(DEFAULT_GOLD_DIR)


def test_default_gold_dir_exists() -> None:
    """DEFAULT_GOLD_DIR points to a valid local directory when pre-generated.

    Skipped on CI or fresh checkouts where artifacts have not been generated.
    Run `python scripts/run_gold_demo.py` to materialise them locally.
    """
    from azure_lakehouse_etl.demo_kpi_queries import DEFAULT_GOLD_DIR

    gold_dir = Path(DEFAULT_GOLD_DIR)
    if not gold_dir.exists():
        pytest.skip(
            f"Gold demo outputs not present at {DEFAULT_GOLD_DIR}. "
            "Run `python scripts/run_gold_demo.py` to materialise them locally."
        )


def test_default_gold_dir_contains_required_tables() -> None:
    """All five tables required by the KPI layer must be present under DEFAULT_GOLD_DIR.

    Skipped on CI or fresh checkouts where artifacts have not been generated.
    """
    from azure_lakehouse_etl.demo_kpi_queries import DEFAULT_GOLD_DIR, _REQUIRED_TABLES

    gold_dir = Path(DEFAULT_GOLD_DIR)
    if not gold_dir.exists():
        pytest.skip(
            f"Gold demo outputs not present at {DEFAULT_GOLD_DIR}. "
            "Run `python scripts/run_gold_demo.py` to materialise them locally."
        )
    for name in _REQUIRED_TABLES:
        assert (gold_dir / name).exists(), f"Missing Gold table directory: {gold_dir / name}"


def test_build_kpi_results_signature() -> None:
    """build_kpi_results must accept spark and optional gold_dir."""
    import inspect
    from azure_lakehouse_etl.demo_kpi_queries import build_kpi_results

    sig = inspect.signature(build_kpi_results)
    assert "spark" in sig.parameters
    assert "gold_dir" in sig.parameters


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_load_gold_tables_returns_required_keys(demo_gold_dir: Path) -> None:
    """load_gold_tables must return a dict with all required table names."""
    from azure_lakehouse_etl.demo_kpi_queries import (
        _REQUIRED_TABLES,
        build_local_spark,
        load_gold_tables,
    )

    spark = build_local_spark(app_name="test_kpi_load_tables")
    tables = load_gold_tables(spark, demo_gold_dir)
    assert set(tables.keys()) == set(_REQUIRED_TABLES)


@requires_java
def test_load_gold_tables_all_non_empty(demo_gold_dir: Path) -> None:
    """Every loaded Gold table must contain at least one row."""
    from azure_lakehouse_etl.demo_kpi_queries import (
        build_local_spark,
        load_gold_tables,
    )

    spark = build_local_spark(app_name="test_kpi_tables_non_empty")
    tables = load_gold_tables(spark, demo_gold_dir)
    for name, df in tables.items():
        assert df.count() > 0, f"Gold table '{name}' loaded 0 rows"


@requires_java
def test_build_kpi_results_returns_all_seven_kpis(demo_gold_dir: Path) -> None:
    """build_kpi_results must return a dict with all seven KPI keys."""
    from azure_lakehouse_etl.demo_kpi_queries import (
        build_kpi_results,
        build_local_spark,
    )

    expected_keys = {
        "gross_sales_summary",
        "returns_summary",
        "inventory_summary",
        "sales_by_channel",
        "refunds_by_reason",
        "stock_by_warehouse",
        "net_sales_by_date",
    }

    spark = build_local_spark(app_name="test_kpi_all_seven")
    results = build_kpi_results(spark, demo_gold_dir)
    assert set(results.keys()) == expected_keys


@requires_java
def test_all_kpi_results_are_non_empty(demo_gold_dir: Path) -> None:
    """Every KPI result set must contain at least one row."""
    from azure_lakehouse_etl.demo_kpi_queries import (
        build_kpi_results,
        build_local_spark,
    )

    spark = build_local_spark(app_name="test_kpi_non_empty")
    results = build_kpi_results(spark, demo_gold_dir)
    for name, df in results.items():
        assert df.count() > 0, f"KPI '{name}' produced 0 rows"


@requires_java
def test_net_sales_by_date_columns_and_values(demo_gold_dir: Path) -> None:
    """net_sales_by_date must have the expected columns and correct net_sales arithmetic."""
    from pyspark.sql import functions as F

    from azure_lakehouse_etl.demo_kpi_queries import (
        build_kpi_results,
        build_local_spark,
    )

    spark = build_local_spark(app_name="test_kpi_net_sales_by_date")
    results = build_kpi_results(spark, demo_gold_dir)
    df = results["net_sales_by_date"]

    assert set(df.columns) == {"full_date", "gross_revenue", "total_refunds", "net_sales"}

    row_count = df.count()
    assert row_count > 0, "net_sales_by_date returned 0 rows"

    # net_sales must equal gross_revenue minus total_refunds on every row.
    bad_rows = (
        df.filter(
            F.round(F.col("net_sales"), 2)
            != F.round(F.col("gross_revenue") - F.col("total_refunds"), 2)
        )
        .count()
    )
    assert bad_rows == 0, f"{bad_rows} row(s) have net_sales != gross_revenue - total_refunds"


@requires_java
def test_gross_sales_summary_has_one_row(demo_gold_dir: Path) -> None:
    """gross_sales_summary is a single aggregated row."""
    from azure_lakehouse_etl.demo_kpi_queries import (
        build_kpi_results,
        build_local_spark,
    )

    spark = build_local_spark(app_name="test_kpi_gross_summary")
    results = build_kpi_results(spark, demo_gold_dir)
    assert results["gross_sales_summary"].count() == 1


@requires_java
def test_sales_by_channel_channel_label_not_null(demo_gold_dir: Path) -> None:
    """sales_by_channel must not have null channel_label — all keys must resolve."""
    from pyspark.sql import functions as F

    from azure_lakehouse_etl.demo_kpi_queries import (
        build_kpi_results,
        build_local_spark,
    )

    spark = build_local_spark(app_name="test_kpi_sales_by_channel")
    results = build_kpi_results(spark, demo_gold_dir)
    null_labels = results["sales_by_channel"].filter(F.col("channel_label").isNull()).count()
    assert null_labels == 0, f"{null_labels} row(s) have null channel_label"


@requires_java
def test_load_gold_tables_raises_for_missing_directory(tmp_path: Path) -> None:
    """load_gold_tables must raise FileNotFoundError when a table directory is absent."""
    from azure_lakehouse_etl.demo_kpi_queries import build_local_spark, load_gold_tables

    spark = build_local_spark(app_name="test_kpi_missing_dir")
    with pytest.raises(FileNotFoundError, match="Gold table directory not found"):
        load_gold_tables(spark, tmp_path)
