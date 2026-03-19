"""Helpers for loading the project source catalog."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


DEFAULT_SOURCE_CATALOG_PATH = Path("config/source_catalog.yml")


def load_source_catalog(
    path: Path | str = DEFAULT_SOURCE_CATALOG_PATH,
) -> dict[str, Any]:
    """Load the YAML source catalog into a Python dictionary."""
    resolved_path = Path(path)
    with resolved_path.open("r", encoding="utf-8") as file:
        catalog = yaml.safe_load(file)

    if not isinstance(catalog, dict):
        raise ValueError("Source catalog must load as a dictionary.")

    return catalog


def list_sources(
    path: Path | str = DEFAULT_SOURCE_CATALOG_PATH,
) -> list[dict[str, Any]]:
    """Return the list of source definitions from the catalog."""
    catalog = load_source_catalog(path)
    sources = catalog.get("sources", [])

    if not isinstance(sources, list):
        raise ValueError("The 'sources' section must be a list.")

    return sources
