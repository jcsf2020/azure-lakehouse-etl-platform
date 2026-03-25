"""Entrypoint: run demo KPI queries against local Gold outputs.

Usage
-----
    python scripts/run_demo_kpis.py

Reads materialized Gold parquet files from artifacts/demo_gold/ and prints
seven analytical result sets to the terminal.  No Databricks or cloud
services required.  Run `python scripts/run_gold_demo.py` first if
artifacts/demo_gold/ does not exist.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from azure_lakehouse_etl.demo_kpi_queries import (
    DEFAULT_GOLD_DIR,
    build_kpi_results,
    build_local_spark,
)


_SEPARATOR = "-" * 60


def _print_kpi(title: str, df) -> None:
    print(f"\n{_SEPARATOR}")
    print(f"  {title}")
    print(_SEPARATOR)
    df.show(truncate=False)


def main() -> None:
    print("Loading Gold demo outputs from:", DEFAULT_GOLD_DIR)
    spark = build_local_spark(app_name="demo_kpi_queries")

    kpis = build_kpi_results(spark, DEFAULT_GOLD_DIR)

    _print_kpi("1. Gross Sales Summary", kpis["gross_sales_summary"])
    _print_kpi("2. Returns Summary", kpis["returns_summary"])
    _print_kpi("3. Inventory Summary", kpis["inventory_summary"])
    _print_kpi("4. Gross Sales by Channel", kpis["sales_by_channel"])
    _print_kpi("5. Refunds by Return Reason", kpis["refunds_by_reason"])
    _print_kpi("6. Stock on Hand by Warehouse", kpis["stock_by_warehouse"])
    _print_kpi("7. Net Sales by Date", kpis["net_sales_by_date"])

    print(f"\n{_SEPARATOR}")
    print(f"  Done — {len(kpis)} KPI result sets produced.")
    print(_SEPARATOR)


if __name__ == "__main__":
    main()
