"""Demo KPI exports — write KPI result sets to local CSV files.

Reads KPI result DataFrames already produced by the demo_kpi_queries layer
and writes one CSV file per KPI result set under a local export directory.
No Databricks, cloud services, or external databases required.

Public API
----------
DEFAULT_EXPORT_DIR     — default output path: artifacts/demo_kpi_exports/
write_kpi_exports()    — write a dict of KPI DataFrames to CSV files
build_kpi_exports()    — build KPI results then write them (convenience wrapper)
"""

from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.demo_kpi_queries import (  # noqa: F401
    DEFAULT_GOLD_DIR,
    build_kpi_results,
    build_local_spark,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_EXPORT_DIR: Path = Path(__file__).parents[2] / "artifacts" / "demo_kpi_exports"

# Stable ordered list of KPI names — defines export file order.
_KPI_NAMES = (
    "gross_sales_summary",
    "returns_summary",
    "inventory_summary",
    "sales_by_channel",
    "refunds_by_reason",
    "stock_by_warehouse",
    "net_sales_by_date",
)


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


def write_kpi_exports(
    kpi_results: dict[str, DataFrame],
    export_dir: Path = DEFAULT_EXPORT_DIR,
) -> dict[str, Path]:
    """Write each KPI result DataFrame to a CSV file under export_dir.

    Each KPI produces one file: ``<export_dir>/<kpi_name>.csv``.
    Rows are collected from Spark before writing so the output is a single,
    clean file rather than a partitioned Spark directory.

    Parameters
    ----------
    kpi_results : dict mapping KPI name → Spark DataFrame
    export_dir  : destination directory; created if it does not exist

    Returns
    -------
    dict mapping KPI name → Path of the written CSV file
    """
    export_dir = Path(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    written: dict[str, Path] = {}
    for name in _KPI_NAMES:
        df = kpi_results[name]
        out_path = export_dir / f"{name}.csv"
        columns = df.columns
        rows = df.collect()
        with out_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(columns)
            for row in rows:
                writer.writerow(list(row))
        written[name] = out_path

    return written


# ---------------------------------------------------------------------------
# Builder — convenience wrapper
# ---------------------------------------------------------------------------


def build_kpi_exports(
    spark: SparkSession,
    gold_dir: Path = DEFAULT_GOLD_DIR,
    export_dir: Path = DEFAULT_EXPORT_DIR,
) -> dict[str, Path]:
    """Build all KPI results and write them to CSV files.

    Calls ``build_kpi_results()`` from the demo_kpi_queries layer then
    passes the result dict to ``write_kpi_exports()``.

    Parameters
    ----------
    spark      : active SparkSession
    gold_dir   : path to materialized Gold parquet outputs
    export_dir : destination directory for exported CSV files

    Returns
    -------
    dict mapping KPI name → Path of the written CSV file
    """
    kpi_results = build_kpi_results(spark, gold_dir)
    return write_kpi_exports(kpi_results, export_dir)
