"""Silver transformation for order items.

This module takes the Bronze order_items dataset and applies the first
Silver-layer rules: schema enforcement, type casting, null filtering, and
simple deduplication.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


BRONZE_TABLE = "bronze.order_items_raw"
SILVER_TABLE = "silver.order_items"
SILVER_PATH = "dbfs:/tmp/azure_lakehouse_etl/silver/order_items"


def get_spark() -> SparkSession:
    """Create or retrieve the Spark session."""
    return SparkSession.builder.appName("silver_order_items").getOrCreate()


def read_bronze_order_items(spark: SparkSession) -> DataFrame:
    """Read the Bronze order_items dataset."""
    return spark.table(BRONZE_TABLE)


def apply_schema_and_basic_quality(df: DataFrame) -> DataFrame:
    """Apply type casting and basic Silver-layer quality rules."""
    typed_df = (
        df.withColumn("order_item_id", F.col("order_item_id").cast("string"))
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("discount_amount", F.col("discount_amount").cast("double"))
        .withColumn("line_total", F.col("line_total").cast("double"))
    )

    filtered_df = typed_df.filter(
        F.col("order_item_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & F.col("discount_amount").isNotNull()
        & F.col("line_total").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & (F.col("discount_amount") >= 0)
        & (F.col("line_total") >= 0)
    )

    return filtered_df.dropDuplicates(["order_item_id"])


def write_silver_order_items(df: DataFrame) -> None:
    """Write the Silver order_items dataset as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", SILVER_PATH)
        .saveAsTable(SILVER_TABLE)
    )


def main() -> None:
    """Run the Silver transformation for order items."""
    spark = get_spark()
    bronze_df = read_bronze_order_items(spark)
    silver_df = apply_schema_and_basic_quality(bronze_df)
    write_silver_order_items(silver_df)


if __name__ == "__main__":
    main()
