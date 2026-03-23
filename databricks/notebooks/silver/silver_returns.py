"""Silver transformation for returns.

This module takes the Bronze returns dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from azure_lakehouse_etl.silver_returns import (
    BRONZE_TABLE,
    SILVER_PATH,
    SILVER_TABLE,
    apply_schema_and_basic_quality,
)


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_returns").getOrCreate()


def read_bronze_returns(spark: SparkSession) -> DataFrame:
    """Read the Bronze returns dataset."""
    return spark.table(BRONZE_TABLE)


def write_silver_returns(df: DataFrame) -> None:
    """Write the Silver returns dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for returns."""
    spark = get_spark()
    bronze_df = read_bronze_returns(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_returns(silver_df)


if __name__ == "__main__":
    main()
