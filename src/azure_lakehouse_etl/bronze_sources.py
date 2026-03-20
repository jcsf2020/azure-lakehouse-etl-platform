"""Bronze source configuration helpers derived from the project catalog."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from azure_lakehouse_etl.bronze_utils import (
    build_bronze_output_path,
    build_bronze_table_name,
)
from azure_lakehouse_etl.source_catalog import list_sources


@dataclass(frozen=True)
class BronzeSourceConfig:
    """Configuration for a single Bronze ingestion source."""

    source_name: str
    source_format: str
    input_path: str
    output_table: str
    output_path: str


def build_source_config(source: dict[str, Any]) -> BronzeSourceConfig:
    """Convert a catalog source entry into a Bronze source config."""
    source_name = source["name"]
    return BronzeSourceConfig(
        source_name=source_name,
        source_format=source["format"],
        input_path=f"data_samples/seed_files/{source_name}.{source['format']}",
        output_table=build_bronze_table_name(source_name),
        output_path=build_bronze_output_path(source_name),
    )


def default_sources() -> Iterable[BronzeSourceConfig]:
    """Return Bronze source definitions derived from the project catalog."""
    catalog_sources = list_sources()
    return [build_source_config(source) for source in catalog_sources]
