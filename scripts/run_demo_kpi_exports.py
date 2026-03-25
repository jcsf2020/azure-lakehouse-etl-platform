"""Entrypoint: export demo KPI results to local CSV files.

Usage
-----
    python scripts/run_demo_kpi_exports.py

Reads materialized Gold parquet files from artifacts/demo_gold/, runs the
seven demo KPI queries, and writes one CSV file per KPI result set to
artifacts/demo_kpi_exports/.  No Databricks or cloud services required.

Prerequisites
-------------
Run `python scripts/run_gold_demo.py` first if artifacts/demo_gold/ does not
exist.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from azure_lakehouse_etl.demo_kpi_exports import (
    DEFAULT_EXPORT_DIR,
    build_kpi_exports,
    build_local_spark,
)
from azure_lakehouse_etl.demo_kpi_queries import DEFAULT_GOLD_DIR


def main() -> None:
    print("Loading Gold demo outputs from:", DEFAULT_GOLD_DIR)
    print("Exporting KPI results to:      ", DEFAULT_EXPORT_DIR)

    spark = build_local_spark(app_name="demo_kpi_exports")
    try:
        written = build_kpi_exports(spark, DEFAULT_GOLD_DIR, DEFAULT_EXPORT_DIR)

        print(f"\nExported {len(written)} KPI result set(s):")
        for name, path in written.items():
            print(f"  {name:<28}  {path}")

        print("\nDone.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
