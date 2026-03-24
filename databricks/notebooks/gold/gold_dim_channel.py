"""Gold dimensional table for dim_channel — Databricks notebook entrypoint.

Derives the conformed channel dimension from the project normalization contract.
No Silver source is required; the dimension is fully static and deterministic.
Writes the final gold.dim_channel Delta table.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_dim_channel import (
    GOLD_PATH,
    GOLD_TABLE,
    build_dim_channel,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_dim_channel").getOrCreate()


def write_gold_dim_channel(df: DataFrame) -> None:
    """Write the Gold dim_channel dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold dim_channel build."""
    spark = get_spark()
    gold_df = build_dim_channel(spark)
    write_gold_dim_channel(gold_df)


if __name__ == "__main__":
    main()
