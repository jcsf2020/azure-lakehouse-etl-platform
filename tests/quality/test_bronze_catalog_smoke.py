"""Local smoke checks for Bronze source catalog integration."""

from __future__ import annotations

from pathlib import Path

from azure_lakehouse_etl.bronze_sources import default_sources
from azure_lakehouse_etl.source_catalog import list_sources


SUPPORTED_FORMATS = {"csv", "json"}


def test_source_catalog_contains_expected_number_of_sources() -> None:
    """The source catalog should expose the six project seed sources."""
    sources = list_sources()
    assert len(sources) == 6


def test_all_catalog_sources_have_supported_formats() -> None:
    """Each source in the catalog must use a supported ingestion format."""
    sources = list_sources()
    for source in sources:
        assert source["format"] in SUPPORTED_FORMATS


def test_default_sources_build_bronze_configs_for_all_catalog_entries() -> None:
    """Bronze config generation should cover every source catalog entry."""
    bronze_sources = list(default_sources())
    assert len(bronze_sources) == 6


def test_all_seed_input_files_exist() -> None:
    """Each Bronze source config should resolve to an existing local seed file."""
    bronze_sources = list(default_sources())
    for source in bronze_sources:
        assert Path(source.input_path).exists()


def test_bronze_output_names_follow_expected_convention() -> None:
    """Bronze table names and paths should follow the project naming convention."""
    bronze_sources = list(default_sources())
    for source in bronze_sources:
        assert source.output_table.startswith("bronze.")
        assert source.output_table.endswith("_raw")
        assert source.output_path.startswith("dbfs:/tmp/azure_lakehouse_etl/bronze/")
        assert source.output_path.endswith("_raw")
