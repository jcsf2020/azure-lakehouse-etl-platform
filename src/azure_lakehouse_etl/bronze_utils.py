"""Shared helpers for Bronze-layer ingestion jobs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


DEFAULT_RUN_ID = "manual_dev_run"
DEFAULT_BRONZE_ROOT = "dbfs:/tmp/azure_lakehouse_etl/bronze"


def current_utc_timestamp() -> str:
    """Return the current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def build_bronze_output_path(entity_name: str, bronze_root: str = DEFAULT_BRONZE_ROOT) -> str:
    """Build the Bronze Delta output path for a given entity."""
    normalized_entity = entity_name.strip().lower()
    return f"{bronze_root}/{normalized_entity}_raw"


def build_bronze_table_name(entity_name: str) -> str:
    """Build the Bronze table name for a given entity."""
    normalized_entity = entity_name.strip().lower()
    return f"bronze.{normalized_entity}_raw"


def add_standard_bronze_metadata(
    df: DataFrame,
    source_name: str,
    pipeline_run_id: Optional[str] = None,
) -> DataFrame:
    """Append standard metadata columns used across Bronze datasets."""
    resolved_run_id = pipeline_run_id or DEFAULT_RUN_ID
    ingestion_ts = current_utc_timestamp()
    return (
        df.withColumn("_source_name", F.lit(source_name))
        .withColumn("_ingestion_ts", F.lit(ingestion_ts))
        .withColumn("_pipeline_run_id", F.lit(resolved_run_id))
        .withColumn("_input_file_name", F.input_file_name())
    )
