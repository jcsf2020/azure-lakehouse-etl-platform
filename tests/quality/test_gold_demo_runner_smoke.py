"""Local smoke checks for the Gold demo runner."""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
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


import pytest

requires_java = pytest.mark.skipif(
    not _java_available(),
    reason="Java is not installed — Spark-dependent tests skipped",
)


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_demo_runner_module_imports_cleanly() -> None:
    """gold_demo_runner must be importable without starting Spark."""
    import azure_lakehouse_etl.gold_demo_runner  # noqa: F401


def test_demo_runner_exports_constants() -> None:
    """Module must expose the key public constants."""
    from azure_lakehouse_etl.gold_demo_runner import (
        DEFAULT_OUTPUT_DIR,
        DEMO_DATE_END,
        DEMO_DATE_START,
        DEMO_DATASETS,
        DEMO_EFFECTIVE_DATE,
        SEED_DIR,
    )

    assert DEMO_DATE_START == "2026-01-01"
    assert DEMO_DATE_END == "2026-12-31"
    assert "artifacts" in str(DEFAULT_OUTPUT_DIR)
    assert SEED_DIR.exists(), f"SEED_DIR not found: {SEED_DIR}"
    assert len(DEMO_DATASETS) == 7


def test_demo_datasets_tuple_contains_all_targets() -> None:
    """DEMO_DATASETS must list all seven expected Gold tables."""
    from azure_lakehouse_etl.gold_demo_runner import DEMO_DATASETS

    expected = {
        "dim_date",
        "dim_channel",
        "dim_customer",
        "dim_product",
        "fact_order_items",
        "fact_returns",
        "fact_inventory_daily",
    }
    assert set(DEMO_DATASETS) == expected


def test_build_local_spark_is_callable() -> None:
    """build_local_spark must be importable and callable as a function."""
    import inspect
    from azure_lakehouse_etl.gold_demo_runner import build_local_spark

    assert callable(build_local_spark)
    sig = inspect.signature(build_local_spark)
    assert "app_name" in sig.parameters


def test_build_gold_datasets_is_callable() -> None:
    """build_gold_datasets must accept spark plus optional keyword arguments."""
    import inspect
    from azure_lakehouse_etl.gold_demo_runner import build_gold_datasets

    sig = inspect.signature(build_gold_datasets)
    assert "spark" in sig.parameters
    assert "date_start" in sig.parameters
    assert "date_end" in sig.parameters
    assert "effective_date" in sig.parameters


def test_write_demo_outputs_is_callable() -> None:
    """write_demo_outputs must accept datasets and output_dir."""
    import inspect
    from azure_lakehouse_etl.gold_demo_runner import write_demo_outputs

    sig = inspect.signature(write_demo_outputs)
    assert "datasets" in sig.parameters
    assert "output_dir" in sig.parameters


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_local_spark_returns_spark_session() -> None:
    """build_local_spark must return an active SparkSession."""
    from pyspark.sql import SparkSession
    from azure_lakehouse_etl.gold_demo_runner import build_local_spark

    spark = build_local_spark(app_name="test_demo_runner_session")
    assert isinstance(spark, SparkSession)
    assert spark.sparkContext.master == "local[*]"


@requires_java
def test_build_gold_datasets_returns_all_expected_keys() -> None:
    """build_gold_datasets must return a dict with all seven Gold dataset names."""
    from azure_lakehouse_etl.gold_demo_runner import (
        DEMO_DATASETS,
        build_gold_datasets,
        build_local_spark,
    )

    spark = build_local_spark(app_name="test_demo_runner_datasets")
    datasets = build_gold_datasets(spark)

    assert set(datasets.keys()) == set(DEMO_DATASETS)


@requires_java
def test_build_gold_datasets_all_tables_are_non_empty() -> None:
    """Every Gold dataset must contain at least one row."""
    from azure_lakehouse_etl.gold_demo_runner import build_gold_datasets, build_local_spark

    spark = build_local_spark(app_name="test_demo_runner_non_empty")
    datasets = build_gold_datasets(spark)

    for name, df in datasets.items():
        count = df.count()
        assert count > 0, f"{name!r} has 0 rows"


@requires_java
def test_dim_date_covers_demo_date_range() -> None:
    """dim_date must include rows for the seed data date range (2026-03-15 to 2026-03-17)."""
    from datetime import date
    from pyspark.sql import functions as F
    from azure_lakehouse_etl.gold_demo_runner import build_gold_datasets, build_local_spark

    spark = build_local_spark(app_name="test_demo_runner_dim_date")
    datasets = build_gold_datasets(spark)

    march_rows = (
        datasets["dim_date"]
        .filter(
            (F.col("full_date") >= F.lit(date(2026, 3, 15)))
            & (F.col("full_date") <= F.lit(date(2026, 3, 17)))
        )
        .count()
    )
    assert march_rows == 3


@requires_java
def test_fact_order_items_date_key_resolves() -> None:
    """fact_order_items must have no null date_key for the seed order dates."""
    from pyspark.sql import functions as F
    from azure_lakehouse_etl.gold_demo_runner import build_gold_datasets, build_local_spark

    spark = build_local_spark(app_name="test_demo_runner_foi_date_key")
    datasets = build_gold_datasets(spark)

    null_count = (
        datasets["fact_order_items"].filter(F.col("date_key").isNull()).count()
    )
    assert null_count == 0, f"fact_order_items has {null_count} rows with null date_key"


@requires_java
def test_write_demo_outputs_creates_expected_directories(tmp_path: Path) -> None:
    """write_demo_outputs must create one directory per Gold dataset."""
    from azure_lakehouse_etl.gold_demo_runner import (
        DEMO_DATASETS,
        build_gold_datasets,
        build_local_spark,
        write_demo_outputs,
    )

    spark = build_local_spark(app_name="test_demo_runner_write")
    datasets = build_gold_datasets(spark)
    write_demo_outputs(datasets, output_dir=tmp_path)

    for name in DEMO_DATASETS:
        dest = tmp_path / name
        assert dest.exists(), f"Output directory missing: {dest}"
        parquet_files = list(dest.glob("*.parquet"))
        assert len(parquet_files) >= 1, f"No parquet files found in {dest}"
