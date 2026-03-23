"""Silver transformation for customers.

This module takes the Bronze customers dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.customers_raw"
SILVER_TABLE = "silver.customers"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/customers"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_customers").getOrCreate()


def read_bronze_customers(spark: SparkSession) -> DataFrame:
    """Read the Bronze customers dataset."""
    return spark.table(BRONZE_TABLE)


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("first_name", F.col("first_name").cast("string"))
        .withColumn("last_name", F.col("last_name").cast("string"))
        .withColumn("email", F.col("email").cast("string"))
        .withColumn("country", F.col("country").cast("string"))
        .withColumn("city", F.col("city").cast("string"))
        .withColumn("registration_date", F.to_date("registration_date"))
        .withColumn("customer_status", F.col("customer_status").cast("string"))
    )

    filtered_df = typed_df.filter(
        F.col("customer_id").isNotNull()
        & F.col("first_name").isNotNull()
        & F.col("last_name").isNotNull()
        & F.col("email").isNotNull()
        & F.col("country").isNotNull()
        & F.col("city").isNotNull()
        & F.col("registration_date").isNotNull()
        & F.col("customer_status").isNotNull()
    )

    return filtered_df.dropDuplicates(["customer_id"])


def write_silver_customers(df: DataFrame) -> None:
    """Write the Silver customers dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for customers."""
    spark = get_spark()
    bronze_df = read_bronze_customers(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_customers(silver_df)


if __name__ == "__main__":
    main()
