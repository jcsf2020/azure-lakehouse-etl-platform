"""Smoke tests for the demo KPI exports layer."""

from __future__ import annotations

import csv
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
# Session-scoped fixture: materialise Gold outputs and run KPI exports
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kpi_export_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Materialise Gold outputs and run KPI exports into temporary directories.

    Builds Gold datasets, writes them to a temp gold dir, then exports KPI
    results to a temp export dir.  Skipped when Java / Spark is unavailable.
    """
    if not _java_available():
        pytest.skip("Java is not installed — Spark-dependent fixture skipped")

    from azure_lakehouse_etl.demo_kpi_exports import write_kpi_exports
    from azure_lakehouse_etl.demo_kpi_queries import build_kpi_results
    from azure_lakehouse_etl.gold_demo_runner import (
        build_gold_datasets,
        build_local_spark,
        write_demo_outputs,
    )

    gold_dir = tmp_path_factory.mktemp("demo_gold")
    export_dir = tmp_path_factory.mktemp("demo_kpi_exports")

    spark = build_local_spark(app_name="test_kpi_exports_fixture")
    try:
        datasets = build_gold_datasets(spark)
        write_demo_outputs(datasets, gold_dir)
        kpi_results = build_kpi_results(spark, gold_dir)
        write_kpi_exports(kpi_results, export_dir)
        yield export_dir
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_demo_kpi_exports_module_imports_cleanly() -> None:
    """demo_kpi_exports must be importable without starting Spark."""
    import azure_lakehouse_etl.demo_kpi_exports  # noqa: F401


def test_demo_kpi_exports_public_api() -> None:
    """Module must expose the expected public symbols."""
    from azure_lakehouse_etl.demo_kpi_exports import (
        DEFAULT_EXPORT_DIR,
        build_kpi_exports,
        build_local_spark,
        write_kpi_exports,
    )

    assert callable(write_kpi_exports)
    assert callable(build_kpi_exports)
    assert callable(build_local_spark)
    assert "artifacts" in str(DEFAULT_EXPORT_DIR)


def test_write_kpi_exports_signature() -> None:
    """write_kpi_exports must accept kpi_results and optional export_dir."""
    import inspect

    from azure_lakehouse_etl.demo_kpi_exports import write_kpi_exports

    sig = inspect.signature(write_kpi_exports)
    assert "kpi_results" in sig.parameters
    assert "export_dir" in sig.parameters


def test_build_kpi_exports_signature() -> None:
    """build_kpi_exports must accept spark, optional gold_dir, and optional export_dir."""
    import inspect

    from azure_lakehouse_etl.demo_kpi_exports import build_kpi_exports

    sig = inspect.signature(build_kpi_exports)
    assert "spark" in sig.parameters
    assert "gold_dir" in sig.parameters
    assert "export_dir" in sig.parameters


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_write_kpi_exports_produces_all_csv_files(kpi_export_dir: Path) -> None:
    """write_kpi_exports must create one CSV file per KPI result set."""
    from azure_lakehouse_etl.demo_kpi_exports import _KPI_NAMES

    for name in _KPI_NAMES:
        assert (kpi_export_dir / f"{name}.csv").exists(), (
            f"Missing export file: {name}.csv"
        )


@requires_java
def test_all_exported_csv_files_are_non_empty(kpi_export_dir: Path) -> None:
    """Every exported CSV must contain a header row plus at least one data row."""
    from azure_lakehouse_etl.demo_kpi_exports import _KPI_NAMES

    for name in _KPI_NAMES:
        path = kpi_export_dir / f"{name}.csv"
        with path.open(encoding="utf-8") as fh:
            rows = list(csv.reader(fh))
        assert len(rows) >= 2, (
            f"{name}.csv has {len(rows)} row(s); expected header + at least one data row"
        )


@requires_java
def test_exported_csv_headers_are_non_empty_strings(kpi_export_dir: Path) -> None:
    """Every exported CSV header row must contain non-empty column names."""
    from azure_lakehouse_etl.demo_kpi_exports import _KPI_NAMES

    for name in _KPI_NAMES:
        path = kpi_export_dir / f"{name}.csv"
        with path.open(encoding="utf-8") as fh:
            header = next(csv.reader(fh))
        assert len(header) > 0, f"{name}.csv has an empty header row"
        for col in header:
            assert col.strip(), f"{name}.csv has a blank column name in header: {header}"


@requires_java
def test_build_kpi_exports_returns_path_for_each_kpi(tmp_path: Path) -> None:
    """build_kpi_exports must return a dict mapping each KPI name to a Path."""
    from azure_lakehouse_etl.demo_kpi_exports import (
        _KPI_NAMES,
        build_kpi_exports,
        build_local_spark,
    )
    from azure_lakehouse_etl.gold_demo_runner import (
        build_gold_datasets,
        write_demo_outputs,
    )

    gold_dir = tmp_path / "gold"
    export_dir = tmp_path / "exports"

    spark = build_local_spark(app_name="test_build_kpi_exports")
    try:
        datasets = build_gold_datasets(spark)
        write_demo_outputs(datasets, gold_dir)
        written = build_kpi_exports(spark, gold_dir, export_dir)
    finally:
        spark.stop()

    assert set(written.keys()) == set(_KPI_NAMES)
    for name, path in written.items():
        assert path.exists(), f"Expected file not found: {path}"
        assert path.suffix == ".csv", f"Expected .csv suffix for {name}, got {path.suffix}"
