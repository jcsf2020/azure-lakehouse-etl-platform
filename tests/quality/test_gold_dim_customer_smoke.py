"""Local smoke checks for the Gold dim_customer SCD2 dimensional table."""

from __future__ import annotations

import inspect
import subprocess
from datetime import date

import pytest

from azure_lakehouse_etl.gold_dim_customer import (
    DIM_CUSTOMER_COLUMNS,
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_TABLE,
    TRACKED_ATTRIBUTES,
    build_gold_dim_customer,
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
        .appName("test_gold_dim_customer_smoke")
        .getOrCreate()
    )


def _make_silver_df(spark, rows=None):
    """Create a minimal Silver customers DataFrame for testing."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("first_name", T.StringType()),
        T.StructField("last_name", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("registration_date", T.DateType()),
        T.StructField("customer_status", T.StringType()),
    ])

    if rows is None:
        rows = [
            ("CUST-001", "Alice", "Smith", "alice@example.com", "London", "GB", date(2022, 1, 15), "active"),
            ("CUST-002", "Bob", "Jones", "bob@example.com", "Paris", "FR", date(2021, 6, 10), "inactive"),
        ]

    return spark.createDataFrame(rows, schema=schema)


_EFFECTIVE_DATE = date(2024, 1, 1)


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_dim_customer_module_exports_table_constants() -> None:
    """Module must expose table name, path, and Silver source constants."""
    assert GOLD_TABLE == "gold.dim_customer"
    assert "gold/dim_customer" in GOLD_PATH
    assert SILVER_TABLE == "silver.customers"


def test_dim_customer_columns_includes_business_key() -> None:
    """DIM_CUSTOMER_COLUMNS must contain the customer_id business key."""
    assert "customer_id" in DIM_CUSTOMER_COLUMNS


def test_dim_customer_columns_includes_surrogate_key() -> None:
    """DIM_CUSTOMER_COLUMNS must contain the customer_key surrogate key."""
    assert "customer_key" in DIM_CUSTOMER_COLUMNS


def test_dim_customer_columns_includes_all_scd2_technical_columns() -> None:
    """DIM_CUSTOMER_COLUMNS must include all four SCD2 technical columns."""
    for col in SCD2_TECHNICAL_COLUMNS:
        assert col in DIM_CUSTOMER_COLUMNS, f"SCD2 column {col!r} missing from DIM_CUSTOMER_COLUMNS"


def test_tracked_attributes_are_non_empty() -> None:
    """TRACKED_ATTRIBUTES must list at least one column for change detection."""
    assert len(TRACKED_ATTRIBUTES) > 0


def test_tracked_attributes_include_expected_fields() -> None:
    """TRACKED_ATTRIBUTES must include email, city, country, and customer_status."""
    expected = {"email", "city", "country", "customer_status"}
    assert expected.issubset(set(TRACKED_ATTRIBUTES))


def test_build_gold_dim_customer_signature() -> None:
    """build_gold_dim_customer must accept silver_df and effective_start_date."""
    sig = inspect.signature(build_gold_dim_customer)
    assert list(sig.parameters) == ["silver_df", "effective_start_date"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_gold_dim_customer_returns_expected_columns() -> None:
    """Output must contain exactly the columns in DIM_CUSTOMER_COLUMNS."""
    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    assert set(df.columns) == set(DIM_CUSTOMER_COLUMNS)


@requires_java
def test_build_gold_dim_customer_business_key_is_present_and_non_null() -> None:
    """customer_id must be non-null in every row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    null_count = df.filter(F.col("customer_id").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_gold_dim_customer_surrogate_key_is_non_null() -> None:
    """customer_key must be non-null in every row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    null_count = df.filter(F.col("customer_key").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_gold_dim_customer_surrogate_key_is_unique() -> None:
    """customer_key must be unique across all rows."""
    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    total = df.count()
    distinct = df.select("customer_key").distinct().count()
    assert distinct == total


@requires_java
def test_build_gold_dim_customer_scd2_technical_columns_exist() -> None:
    """All four SCD2 technical columns must be present in the output."""
    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    for col in SCD2_TECHNICAL_COLUMNS:
        assert col in df.columns, f"SCD2 column {col!r} missing"


@requires_java
def test_build_gold_dim_customer_row_hash_is_non_null() -> None:
    """scd_row_hash must be non-null in every row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    null_count = df.filter(F.col(SCD2_ROW_HASH).isNull()).count()
    assert null_count == 0


@requires_java
def test_build_gold_dim_customer_all_rows_are_current() -> None:
    """On an initial load every row must have scd_is_current = True."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    non_current = df.filter(F.col(SCD2_IS_CURRENT) == False).count()  # noqa: E712
    assert non_current == 0


@requires_java
def test_build_gold_dim_customer_effective_start_matches_input_date() -> None:
    """scd_effective_start must equal the supplied effective_start_date for every row."""
    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    rows = df.select(SCD2_EFFECTIVE_START).collect()
    for row in rows:
        assert str(row[SCD2_EFFECTIVE_START]) == str(_EFFECTIVE_DATE)


@requires_java
def test_build_gold_dim_customer_effective_end_is_open_sentinel() -> None:
    """scd_effective_end must be the open-ended sentinel (9999-12-31) for all rows."""
    spark = _local_spark()
    df = build_gold_dim_customer(_make_silver_df(spark), _EFFECTIVE_DATE)
    rows = df.select(SCD2_EFFECTIVE_END).collect()
    for row in rows:
        assert str(row[SCD2_EFFECTIVE_END]) == str(SCD2_END_SENTINEL)


@requires_java
def test_build_gold_dim_customer_row_count_matches_input() -> None:
    """Output must contain exactly one row per Silver input customer."""
    spark = _local_spark()
    silver_df = _make_silver_df(spark)
    dim_df = build_gold_dim_customer(silver_df, _EFFECTIVE_DATE)
    assert dim_df.count() == silver_df.count()


@requires_java
def test_build_gold_dim_customer_is_deterministic() -> None:
    """Two calls with the same input and date must produce identical customer_key values."""
    spark = _local_spark()
    silver_df = _make_silver_df(spark)

    run_a = {
        r["customer_id"]: r["customer_key"]
        for r in build_gold_dim_customer(silver_df, _EFFECTIVE_DATE).collect()
    }
    run_b = {
        r["customer_id"]: r["customer_key"]
        for r in build_gold_dim_customer(silver_df, _EFFECTIVE_DATE).collect()
    }
    assert run_a == run_b


@requires_java
def test_build_gold_dim_customer_changed_tracked_attribute_produces_different_hash() -> None:
    """A change in a tracked attribute must produce a different scd_row_hash.

    Simulates two versions of the same customer: version A has country='GB',
    version B has country='US'.  The row hashes must differ, signalling to a
    future incremental merge pipeline that a new SCD2 version must be opened.
    """
    from pyspark.sql import types as T

    spark = _local_spark()

    schema = T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("first_name", T.StringType()),
        T.StructField("last_name", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("registration_date", T.DateType()),
        T.StructField("customer_status", T.StringType()),
    ])

    version_a = spark.createDataFrame(
        [("CUST-001", "Alice", "Smith", "alice@example.com", "London", "GB", date(2022, 1, 15), "active")],
        schema=schema,
    )
    version_b = spark.createDataFrame(
        [("CUST-001", "Alice", "Smith", "alice@example.com", "London", "US", date(2022, 1, 15), "active")],
        schema=schema,
    )

    hash_a = (
        build_gold_dim_customer(version_a, date(2024, 1, 1))
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )
    hash_b = (
        build_gold_dim_customer(version_b, date(2024, 6, 1))
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )

    assert hash_a != hash_b


@requires_java
def test_build_gold_dim_customer_non_tracked_change_does_not_affect_hash() -> None:
    """A change in a non-tracked attribute must NOT change the scd_row_hash.

    first_name is not in TRACKED_ATTRIBUTES.  Two rows identical except for
    first_name must produce the same hash, meaning no new SCD2 version would
    be opened for a name correction alone.
    """
    from pyspark.sql import types as T

    spark = _local_spark()

    schema = T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("first_name", T.StringType()),
        T.StructField("last_name", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("registration_date", T.DateType()),
        T.StructField("customer_status", T.StringType()),
    ])

    version_a = spark.createDataFrame(
        [("CUST-001", "Alice", "Smith", "alice@example.com", "London", "GB", date(2022, 1, 15), "active")],
        schema=schema,
    )
    version_b = spark.createDataFrame(
        # first_name changed: "Alice" → "Alicia" — non-tracked, must not affect hash
        [("CUST-001", "Alicia", "Smith", "alice@example.com", "London", "GB", date(2022, 1, 15), "active")],
        schema=schema,
    )

    hash_a = (
        build_gold_dim_customer(version_a, _EFFECTIVE_DATE)
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )
    hash_b = (
        build_gold_dim_customer(version_b, _EFFECTIVE_DATE)
        .select(SCD2_ROW_HASH)
        .collect()[0][SCD2_ROW_HASH]
    )

    assert hash_a == hash_b
