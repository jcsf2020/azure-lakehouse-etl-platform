"""Bronze ingestion foundation for the Azure Lakehouse ETL Platform.

This module is written as a Databricks-friendly PySpark job entrypoint.
It standardizes raw landed files into Bronze-layer datasets with minimal
transformation and ingestion metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.bronze_utils import (
    add_standard_bronze_metadata,
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


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("bronze_ingestion").getOrCreate()


def read_raw_source(spark: SparkSession, config: BronzeSourceConfig) -> DataFrame:
    """Read a raw landed source using the configured file format."""
    return (
        spark.read.format(config.source_format)
        .option("header", "true")
        .load(config.input_path)
    )


def write_bronze_delta(df: DataFrame, config: BronzeSourceConfig) -> None:
    """Write the Bronze dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", config.output_path)
        .saveAsTable(config.output_table)
    )


def ingest_source(spark: SparkSession, config: BronzeSourceConfig) -> None:
    """Execute the Bronze ingestion flow for a single source."""
    raw_df = read_raw_source(spark, config)
    bronze_df = add_standard_bronze_metadata(raw_df, config.source_name)
    write_bronze_delta(bronze_df, config)


def default_sources() -> Iterable[BronzeSourceConfig]:
    """Return Bronze source definitions derived from the project catalog."""
    catalog_sources = list_sources()
    file_sources = [
        source for source in catalog_sources if source.get("source_type") == "file"
    ]
    return [build_source_config(source) for source in file_sources]


def main() -> None:
    """Run Bronze ingestion for the default development sources."""
    spark = get_spark()
    for config in default_sources():
        ingest_source(spark, config)


if __name__ == "__main__":
    main()
