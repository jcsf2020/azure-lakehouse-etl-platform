"""Local smoke checks for the Gold dim_product SCD2 dimensional table."""

from __future__ import annotations

import inspect
import subprocess
from datetime import date

import pytest

from azure_lakehouse_etl.gold_dim_product import (
    DIM_PRODUCT_COLUMNS,
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    TRACKED_ATTRIBUTES,
    build_gold_dim_product,
)
from azure_lakehouse_etl.gold_dimensional_foundations import (
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
    SCD2_END_SENTINEL,
    SCD2_IS_CURRENT,
    SCD2_ROW_HASH,
    SCD2_TECHNICAL_COLUMNS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _java_available() -> bool:
    """Return True only when a working JRE is reachable."""
    try:
        result = subprocess.run(
            ["java", "-version"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_java = pytest.mark.skipif(
    not _java_available(),
    reason="Java is not installed — Spark-dependent tests skipped",
)


def _local_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("test_gold_dim_product_smoke")
        .getOrCreate()
    )


def _make_silver_df(spark, rows=None):
    """Create a minimal Silver products DataFrame for testing."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("subcategory", T.StringType()),
        T.StructField("brand", T.StringType()),
        T.StructField("list_price", T.DoubleType()),
        T.StructField("cost_price", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("is_active", T.BooleanType()),
        T.StructField("last_updated", T.TimestampType()),
    ])

    if rows is None:
        rows = [
            ("PROD-001", "Widget A", "Electronics", "Gadgets", "BrandX", 29.99, 12.50, "USD", True, None),
            ("PROD-002", "Widget B", "Apparel", "Tops", "BrandY", 49.99, 20.00, "USD", True, None),
        ]

    return spark.createDataFrame(rows, schema=schema)


_EFFECTIVE_DATE = date(2024, 1, 1)


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_dim_product_module_exports_table_constants() -> None:
    """Module must expose table name, path, and Silver source constants."""
    assert GOLD_TABLE == "gold.dim_product"
    assert "gold/dim_product" in GOLD_PATH
    assert SILVER_TABLE == "silver.products"


def test_dim_product_columns_includes_business_key() -> None:
    """DIM_PRODUCT_COLUMNS must contain the product_id business key."""
    assert "product_id" in DIM_PRODUCT_COLUMNS


def test_dim_product_columns_includes_surrogate_key() -> None:
    """DIM_PRODUCT_COLUMNS must contain the product_key surrogate key."""
    assert "product_key" in DIM_PRODUCT_COLUMNS


def test_dim_product_columns_includes_all_scd2_technical_columns() -> None:
    """DIM_PRODUCT_COLUMNS must include all four SCD2 technical columns."""
    for col in SCD2_TECHNICAL_COLUMNS:
        assert col in DIM_PRODUCT_COLUMNS, f"SCD2 column {col!r} missing from DIM_PRODUCT_COLUMNS"


def test_tracked_attributes_are_non_empty() -> None:
    """TRACKED_ATTRIBUTES must list at least one column for change detection."""
    assert len(TRACKED_ATTRIBUTES) > 0


def test_tracked_attributes_include_expected_fields() -> None:
    """TRACKED_ATTRIBUTES must include the core product lifecycle fields."""
    expected = {"product_name", "category", "subcategory", "brand", "list_price", "cost_price", "is_active"}
    assert expected.issubset(set(TRACKED_ATTRIBUTES))


def test_build_gold_dim_product_signature() -> None:
    """build_gold_dim_product must accept silver_df and effective_start_date."""
    sig = inspect.signature(build_gold_dim_product)
    assert list(sig.parameters) == ["silver_df", "effective_start_date"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_gold_dim_product_returns_expected_columns() -> None:
    """Output must contain exactly the columns in DIM_PRODUCT_COLUMNS."""
    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    assert set(df.columns) == set(DIM_PRODUCT_COLUMNS)


@requires_java
def test_build_gold_dim_product_business_key_is_present_and_non_null() -> None:
    """product_id must be non-null in every row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    null_count = df.filter(F.col("product_id").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_gold_dim_product_surrogate_key_is_non_null() -> None:
    """product_key must be non-null in every row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    null_count = df.filter(F.col("product_key").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_gold_dim_product_surrogate_key_is_unique() -> None:
    """product_key must be unique across all rows."""
    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    total = df.count()
    distinct = df.select("product_key").distinct().count()
    assert distinct == total


@requires_java
def test_build_gold_dim_product_scd2_technical_columns_exist() -> None:
    """All four SCD2 technical columns must be present in the output."""
    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    for col in SCD2_TECHNICAL_COLUMNS:
        assert col in df.columns, f"SCD2 column {col!r} missing"


@requires_java
def test_build_gold_dim_product_row_hash_is_non_null() -> None:
    """scd_row_hash must be non-null in every row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    null_count = df.filter(F.col(SCD2_ROW_HASH).isNull()).count()
    assert null_count == 0


@requires_java
def test_build_gold_dim_product_all_rows_are_current() -> None:
    """On an initial load every row must have scd_is_current = True."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    non_current = df.filter(F.col(SCD2_IS_CURRENT) == False).count()  # noqa: E712
    assert non_current == 0


@requires_java
def test_build_gold_dim_product_effective_start_matches_input_date() -> None:
    """scd_effective_start must equal the supplied effective_start_date for every row."""
    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    rows = df.select(SCD2_EFFECTIVE_START).collect()
    for row in rows:
        assert str(row[SCD2_EFFECTIVE_START]) == str(_EFFECTIVE_DATE)


@requires_java
def test_build_gold_dim_product_effective_end_is_open_sentinel() -> None:
    """scd_effective_end must be the open-ended sentinel (9999-12-31) for all rows."""
    spark = _local_spark()
    df = build_gold_dim_product(_make_silver_df(spark), _EFFECTIVE_DATE)
    rows = df.select(SCD2_EFFECTIVE_END).collect()
    for row in rows:
        assert str(row[SCD2_EFFECTIVE_END]) == str(SCD2_END_SENTINEL)


@requires_java
def test_build_gold_dim_product_row_count_matches_input() -> None:
    """Output must contain exactly one row per Silver input product."""
    spark = _local_spark()
    silver_df = _make_silver_df(spark)
    dim_df = build_gold_dim_product(silver_df, _EFFECTIVE_DATE)
    assert dim_df.count() == silver_df.count()


@requires_java
def test_build_gold_dim_product_is_deterministic() -> None:
    """Two calls with the same input and date must produce identical product_key values."""
    spark = _local_spark()
    silver_df = _make_silver_df(spark)

    run_a = {
        r["product_id"]: r["product_key"]
        for r in build_gold_dim_product(silver_df, _EFFECTIVE_DATE).collect()
    }
    run_b = {
        r["product_id"]: r["product_key"]
        for r in build_gold_dim_product(silver_df, _EFFECTIVE_DATE).collect()
    }
    assert run_a == run_b


@requires_java
def test_build_gold_dim_product_changed_tracked_attribute_produces_different_hash() -> None:
    """A change in a tracked attribute must produce a different scd_row_hash.

    Simulates two versions of the same product: version A has list_price=29.99,
    version B has list_price=34.99.  The row hashes must differ, signalling to a
    future incremental merge pipeline that a new SCD2 version must be opened.
    """
    from pyspark.sql import types as T

    spark = _local_spark()

    schema = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("subcategory", T.StringType()),
        T.StructField("brand", T.StringType()),
        T.StructField("list_price", T.DoubleType()),
        T.StructField("cost_price", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("is_active", T.BooleanType()),
        T.StructField("last_updated", T.TimestampType()),
    ])

    version_a = spark.createDataFrame(
        [("PROD-001", "Widget A", "Electronics", "Gadgets", "BrandX", 29.99, 12.50, "USD", True, None)],
        schema=schema,
    )
    version_b = spark.createDataFrame(
        # list_price changed: 29.99 → 34.99 — tracked, must produce a different hash
        [("PROD-001", "Widget A", "Electronics", "Gadgets", "BrandX", 34.99, 12.50, "USD", True, None)],
        schema=schema,
    )

    hash_a = (
        build_gold_dim_product(version_a, date(2024, 1, 1))
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )
    hash_b = (
        build_gold_dim_product(version_b, date(2024, 6, 1))
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )

    assert hash_a != hash_b


@requires_java
def test_build_gold_dim_product_changed_product_name_produces_different_hash() -> None:
    """A change in product_name must produce a different scd_row_hash.

    product_name is in TRACKED_ATTRIBUTES.  A rename (e.g. a rebrand) must open
    a new SCD2 version so analysts can query the historical product name at any
    point in time.
    """
    from pyspark.sql import types as T

    spark = _local_spark()

    schema = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("subcategory", T.StringType()),
        T.StructField("brand", T.StringType()),
        T.StructField("list_price", T.DoubleType()),
        T.StructField("cost_price", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("is_active", T.BooleanType()),
        T.StructField("last_updated", T.TimestampType()),
    ])

    version_a = spark.createDataFrame(
        [("PROD-001", "Widget A", "Electronics", "Gadgets", "BrandX", 29.99, 12.50, "USD", True, None)],
        schema=schema,
    )
    version_b = spark.createDataFrame(
        # product_name changed: "Widget A" → "Widget A Pro" — tracked, must produce a different hash
        [("PROD-001", "Widget A Pro", "Electronics", "Gadgets", "BrandX", 29.99, 12.50, "USD", True, None)],
        schema=schema,
    )

    hash_a = (
        build_gold_dim_product(version_a, _EFFECTIVE_DATE)
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )
    hash_b = (
        build_gold_dim_product(version_b, _EFFECTIVE_DATE)
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )

    assert hash_a != hash_b


@requires_java
def test_build_gold_dim_product_non_tracked_change_does_not_affect_hash() -> None:
    """A change in a non-tracked attribute must NOT change the scd_row_hash.

    currency is not in TRACKED_ATTRIBUTES.  Two rows identical except for
    currency must produce the same hash, meaning no new SCD2 version would
    be opened for a currency correction alone.
    """
    from pyspark.sql import types as T

    spark = _local_spark()

    schema = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("product_name", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("subcategory", T.StringType()),
        T.StructField("brand", T.StringType()),
        T.StructField("list_price", T.DoubleType()),
        T.StructField("cost_price", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("is_active", T.BooleanType()),
        T.StructField("last_updated", T.TimestampType()),
    ])

    version_a = spark.createDataFrame(
        [("PROD-001", "Widget A", "Electronics", "Gadgets", "BrandX", 29.99, 12.50, "USD", True, None)],
        schema=schema,
    )
    version_b = spark.createDataFrame(
        # currency changed: "USD" → "EUR" — non-tracked, must not affect hash
        [("PROD-001", "Widget A", "Electronics", "Gadgets", "BrandX", 29.99, 12.50, "EUR", True, None)],
        schema=schema,
    )

    hash_a = (
        build_gold_dim_product(version_a, _EFFECTIVE_DATE)
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )
    hash_b = (
        build_gold_dim_product(version_b, _EFFECTIVE_DATE)
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )

    assert hash_a == hash_b
