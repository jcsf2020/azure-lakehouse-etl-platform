"""Gold dimensional table for dim_date — Databricks notebook entrypoint.

Reads no Silver source; the date dimension is generated programmatically from
the foundation helper.  Writes the final gold.dim_date Delta table covering
the project's default calendar range.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.gold_dim_date import (
    GOLD_PATH,
    GOLD_TABLE,
    build_dim_date,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("gold_dim_date").getOrCreate()


def write_gold_dim_date(df: DataFrame) -> None:
    """Write the Gold dim_date dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", GOLD_PATH)
        .saveAsTable(GOLD_TABLE)
    )


def main() -> None:
    """Run the Gold dim_date build."""
    spark = get_spark()
    gold_df = build_dim_date(spark)
    write_gold_dim_date(gold_df)


if __name__ == "__main__":
    main()
