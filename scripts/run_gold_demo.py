"""Entrypoint: build and write Gold demo outputs to artifacts/demo_gold/.

Usage
-----
    python scripts/run_gold_demo.py

Writes one Parquet directory per Gold dataset under artifacts/demo_gold/.
No Databricks or cloud services required.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from repo root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from azure_lakehouse_etl.gold_demo_runner import (
    DEFAULT_OUTPUT_DIR,
    DEMO_DATASETS,
    run_demo,
)


def main() -> None:
    print("Building Gold demo outputs...")
    datasets = run_demo()
    print(f"Wrote {len(datasets)} datasets to: {DEFAULT_OUTPUT_DIR}")
    for name in DEMO_DATASETS:
        dest = DEFAULT_OUTPUT_DIR / name
        print(f"  {name:25s} → {dest}")


if __name__ == "__main__":
    main()
