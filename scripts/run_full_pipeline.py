#!/usr/bin/env python3
"""
run_full_pipeline.py — End-to-end execution harness for the Azure Lakehouse ETL Platform.

Executes all SQL assets in dependency order against a Databricks SQL warehouse,
validates data quality via v_dq_status, and exports artifacts to a timestamped
run directory under artifacts/execution_runs/<run_id>/.

Usage
-----
    python scripts/run_full_pipeline.py               # Full run (all layers)
    python scripts/run_full_pipeline.py --skip-bronze # Skip Bronze re-ingestion
    python scripts/run_full_pipeline.py --dry-run     # Validate assets; no execution

Environment (required for live execution)
-----------------------------------------
    DATABRICKS_HOST        https://adb-xxxx.azuredatabricks.net
    DATABRICKS_TOKEN       Personal access token or service principal secret
    DATABRICKS_HTTP_PATH   /sql/1.0/warehouses/xxxx
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Repository layout
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = REPO_ROOT / "sql"
ARTIFACTS_ROOT = REPO_ROOT / "artifacts" / "execution_runs"

# Layer execution order — dependency-safe.
# Within each layer, files are sorted by filename prefix (01_, 02_, ...).
# Gold 04_fact_returns_enriched reads from gold.fact_sales — prefix ordering
# ensures fact_sales (01) is built before fact_returns_enriched (04).
_SQL_LAYERS = ("bronze", "silver", "gold", "dq", "contracts")

# DQ checks that are expected to return issue_count > 0 by design.
# These are reported but do not mark the run as DQ_WARNING.
_EXPECTED_NONZERO: frozenset[str] = frozenset({"fact_returns_unmatched_sales"})

# SELECT queries used to export artifacts after the Gold build.
_EXPORT_QUERIES: dict[str, str] = {
    "dq_status": (
        "SELECT check_name, issue_count, check_status "
        "FROM lakehouse_prod.gold.v_dq_status "
        "ORDER BY check_name"
    ),
    "fact_sales_sample": (
        "SELECT * FROM lakehouse_prod.gold.fact_sales LIMIT 100"
    ),
    "fact_returns_enriched_sample": (
        "SELECT * FROM lakehouse_prod.gold.fact_returns_enriched LIMIT 100"
    ),
    "returns_by_product": (
        "SELECT * FROM lakehouse_prod.gold.returns_by_product "
        "ORDER BY total_returns DESC"
    ),
}

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class StepResult:
    layer: str
    file: str
    status: str       # "ok" | "skipped" | "failed" | "dry_run"
    duration_s: float = 0.0
    error: str | None = None


@dataclass
class DQCheck:
    check_name: str
    issue_count: int
    check_status: str  # "PASS" | "FAIL"
    expected: bool = True  # False = unexpected failure


@dataclass
class RunResult:
    run_id: str
    started_at: str
    completed_at: str = ""
    execution_status: str = "PENDING"  # SUCCESS | FAILED | DQ_WARNING | DRY_RUN
    steps: list[StepResult] = field(default_factory=list)
    dq_checks: list[DQCheck] = field(default_factory=list)
    artifacts_dir: str = ""
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger("run_full_pipeline")


# ---------------------------------------------------------------------------
# Asset discovery
# ---------------------------------------------------------------------------
def build_plan(skip_bronze: bool) -> list[tuple[str, Path]]:
    """Return ordered (layer, sql_path) tuples for all SQL assets."""
    plan: list[tuple[str, Path]] = []
    for layer in _SQL_LAYERS:
        layer_dir = SQL_ROOT / layer
        if not layer_dir.exists():
            log.warning("SQL layer directory not found: %s — skipping.", layer_dir)
            continue
        for path in sorted(layer_dir.glob("*.sql")):
            plan.append((layer, path))
    if skip_bronze:
        plan = [(lyr, p) for lyr, p in plan if lyr != "bronze"]
    return plan


# ---------------------------------------------------------------------------
# Databricks connection
# ---------------------------------------------------------------------------
def open_connection():
    """Open and return a Databricks SQL connection. Raises SystemExit on failure."""
    try:
        from databricks import sql as dbsql  # noqa: PLC0415
    except ImportError:
        raise SystemExit(
            "databricks-sql-connector is not installed.\n"
            "  Install: uv sync --group databricks\n"
            "  Or use --dry-run to validate assets without a live connection."
        )

    host = os.environ.get("DATABRICKS_HOST", "").removeprefix("https://").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")

    missing = [k for k, v in {
        "DATABRICKS_HOST": host,
        "DATABRICKS_TOKEN": token,
        "DATABRICKS_HTTP_PATH": http_path,
    }.items() if not v]

    if missing:
        raise SystemExit(
            f"Missing environment variable(s): {', '.join(missing)}\n"
            "Set DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH.\n"
            "Use --dry-run to run without a live connection."
        )

    return dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------
def _exec_file(cursor, path: Path) -> tuple[float, str | None]:
    """Execute one SQL file. Returns (elapsed_seconds, error_or_None)."""
    sql = path.read_text(encoding="utf-8").strip()
    t0 = time.perf_counter()
    try:
        cursor.execute(sql)
        return time.perf_counter() - t0, None
    except Exception as exc:  # noqa: BLE001
        return time.perf_counter() - t0, str(exc)


def _fetch_rows(cursor, query: str) -> list[dict[str, Any]]:
    """Execute a SELECT and return rows as a list of dicts."""
    cursor.execute(query)
    if not cursor.description:
        return []
    cols = [col[0] for col in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _write_csv(rows: list[dict[str, Any]], dest: Path) -> None:
    if not rows:
        dest.write_text("(no rows)\n", encoding="utf-8")
        return
    with dest.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Summary output
# ---------------------------------------------------------------------------
def _print_summary(result: RunResult) -> None:
    log.info("=" * 60)
    log.info("PIPELINE SUMMARY")
    log.info("  run_id            : %s", result.run_id)
    log.info("  execution_status  : %s", result.execution_status)
    log.info("  started_at        : %s", result.started_at)
    log.info("  completed_at      : %s", result.completed_at)

    if result.steps:
        n_ok = sum(1 for s in result.steps if s.status == "ok")
        n_skip = sum(1 for s in result.steps if s.status in ("skipped", "dry_run"))
        n_fail = sum(1 for s in result.steps if s.status == "failed")
        log.info("  steps             : %d ok  %d skipped  %d failed",
                 n_ok, n_skip, n_fail)

    if result.dq_checks:
        n_pass = sum(1 for c in result.dq_checks if c.check_status == "PASS")
        log.info("  dq checks         : %d / %d PASS", n_pass, len(result.dq_checks))
        for c in result.dq_checks:
            flag = "  (expected)" if c.check_name in _EXPECTED_NONZERO else ""
            log.info("    %-46s %s  count=%d%s",
                     c.check_name, c.check_status, c.issue_count, flag)

    if result.artifacts_dir:
        log.info("  artifacts         : %s", result.artifacts_dir)

    if result.errors:
        log.info("  errors:")
        for err in result.errors:
            log.info("    - %s", err)

    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------
def run_pipeline(skip_bronze: bool, dry_run: bool) -> RunResult:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    result = RunResult(
        run_id=run_id,
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    log.info("=" * 60)
    log.info("Azure Lakehouse ETL Platform — Full Pipeline Run")
    log.info("  run_id : %s", run_id)
    log.info("  mode   : %s", "DRY RUN" if dry_run else "LIVE EXECUTION")
    if skip_bronze:
        log.info("  bronze : SKIPPED (--skip-bronze)")
    log.info("=" * 60)

    plan = build_plan(skip_bronze)
    log.info("Execution plan: %d SQL scripts", len(plan))
    for layer, path in plan:
        log.info("  [%-9s] %s", layer, path.name)

    # --- Dry run: validate assets only, no execution ---
    if dry_run:
        missing = [path for _, path in plan if not path.exists()]
        if missing:
            for p in missing:
                log.error("  MISSING: %s", p)
            result.errors = [f"Missing SQL asset: {p.name}" for p in missing]
            result.execution_status = "FAILED"
        else:
            log.info("All %d SQL assets found. No execution performed.", len(plan))
            result.execution_status = "DRY_RUN"
        for layer, path in plan:
            result.steps.append(StepResult(
                layer=layer, file=path.name, status="dry_run"
            ))
        result.completed_at = datetime.now(timezone.utc).isoformat()
        _print_summary(result)
        return result

    # --- Live execution ---
    artifact_dir = ARTIFACTS_ROOT / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    result.artifacts_dir = str(artifact_dir)

    log.info("Connecting to Databricks SQL warehouse...")
    conn = open_connection()
    log.info("Connected.")

    try:
        with conn.cursor() as cursor:

            # --- STEP 1: Execute SQL assets ---
            log.info("─" * 60)
            log.info("STEP 1  SQL asset execution")
            log.info("─" * 60)

            pipeline_ok = True
            for layer, path in plan:
                if not pipeline_ok:
                    result.steps.append(
                        StepResult(layer=layer, file=path.name, status="skipped")
                    )
                    continue

                log.info("[%-9s] %s ...", layer, path.name)
                elapsed, err = _exec_file(cursor, path)

                if err:
                    log.error("  FAILED (%.2fs): %s", elapsed, err)
                    result.steps.append(StepResult(
                        layer=layer, file=path.name, status="failed",
                        duration_s=round(elapsed, 2), error=err,
                    ))
                    result.errors.append(f"{path.name}: {err}")
                    pipeline_ok = False
                else:
                    log.info("  OK (%.2fs)", elapsed)
                    result.steps.append(StepResult(
                        layer=layer, file=path.name, status="ok",
                        duration_s=round(elapsed, 2),
                    ))

            if not pipeline_ok:
                result.execution_status = "FAILED"
                return result

            # --- STEP 2: DQ validation ---
            log.info("─" * 60)
            log.info("STEP 2  DQ validation  (v_dq_status)")
            log.info("─" * 60)

            dq_rows = _fetch_rows(cursor, _EXPORT_QUERIES["dq_status"])
            unexpected_failures: list[str] = []

            for row in dq_rows:
                name = str(row["check_name"])
                count = int(row["issue_count"])
                status = str(row["check_status"])
                expected = status == "PASS" or name in _EXPECTED_NONZERO

                result.dq_checks.append(DQCheck(
                    check_name=name,
                    issue_count=count,
                    check_status=status,
                    expected=expected,
                ))

                note = "  (expected by design)" if name in _EXPECTED_NONZERO and status == "FAIL" else ""
                indicator = "✓" if expected else "✗"
                log.info("  %s %-46s %s  count=%d%s",
                         indicator, name, status, count, note)

                if not expected:
                    unexpected_failures.append(name)

            if unexpected_failures:
                log.warning("Unexpected DQ failures: %s", unexpected_failures)
                result.errors.extend(
                    f"Unexpected DQ failure: {c}" for c in unexpected_failures
                )

            # --- STEP 3: Export artifacts ---
            log.info("─" * 60)
            log.info("STEP 3  Export artifacts")
            log.info("─" * 60)

            for name, query in _EXPORT_QUERIES.items():
                try:
                    rows = _fetch_rows(cursor, query)
                    dest = artifact_dir / f"{name}.csv"
                    _write_csv(rows, dest)
                    log.info("  %-36s → %d rows  →  %s", name, len(rows), dest.name)
                except Exception as exc:  # noqa: BLE001
                    log.error("  Export failed  %s: %s", name, exc)
                    result.errors.append(f"Export {name}: {exc}")

    finally:
        conn.close()
        result.completed_at = datetime.now(timezone.utc).isoformat()

        # Determine final status
        has_step_failure = any(s.status == "failed" for s in result.steps)
        has_unexpected_dq = any(not c.expected for c in result.dq_checks)
        if has_step_failure:
            result.execution_status = "FAILED"
        elif has_unexpected_dq:
            result.execution_status = "DQ_WARNING"
        else:
            result.execution_status = "SUCCESS"

        # Write run log
        log_path = artifact_dir / "run_log.json"
        log_path.write_text(
            json.dumps(asdict(result), indent=2, default=str),
            encoding="utf-8",
        )
        log.info("Run log written: %s", log_path)

        _print_summary(result)

    return result


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------
def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full Azure Lakehouse ETL pipeline against Databricks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip Bronze ingestion (use when Bronze tables are already materialized).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate SQL assets exist and log the execution plan. No SQL is executed.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = run_pipeline(skip_bronze=args.skip_bronze, dry_run=args.dry_run)
    sys.exit(0 if result.execution_status in ("SUCCESS", "DRY_RUN") else 1)


if __name__ == "__main__":
    main()
