"""Bronze ingestion foundation for the Azure Lakehouse ETL Platform.

This module is written as a Databricks-friendly PySpark job entrypoint.
It standardizes raw landed files into Bronze-layer datasets with minimal
transformation and ingestion metadata.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.bronze_sources import BronzeSourceConfig, default_sources
from azure_lakehouse_etl.bronze_utils import add_standard_bronze_metadata


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("bronze_ingestion").getOrCreate()


def read_raw_source(spark: SparkSession, config: BronzeSourceConfig) -> DataFrame:
    """Read a raw landed source using the configured file format."""
    if config.source_format == "csv":
        return (
            spark.read.format("csv")
            .option("header", "true")
            .load(config.input_path)
        )

    if config.source_format == "json":
        return (
            spark.read.format("json")
            .option("multiline", "true")
            .load(config.input_path)
        )

    raise ValueError(f"Unsupported source format: {config.source_format}")


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


def main() -> None:
    """Run Bronze ingestion for the default development sources."""
    spark = get_spark()
    for config in default_sources():
        ingest_source(spark, config)


if __name__ == "__main__":
    main()
