"""Local smoke checks for the Gold fact_inventory_daily fact table."""

from __future__ import annotations

import inspect
import subprocess
from datetime import date

import pytest

from azure_lakehouse_etl.gold_fact_inventory_daily import (
    FACT_INVENTORY_DAILY_COLUMNS,
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_INVENTORY_SNAPSHOTS_TABLE,
    build_fact_inventory_daily,
)
from azure_lakehouse_etl.gold_dimensional_foundations import (
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
    SCD2_END_SENTINEL,
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
        .appName("test_gold_fact_inventory_daily_smoke")
        .getOrCreate()
    )


def _make_silver_inventory_snapshots_df(spark, rows=None):
    """Create a minimal Silver inventory_snapshots DataFrame for testing."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("snapshot_date", T.DateType()),
        T.StructField("warehouse_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("stock_on_hand", T.IntegerType()),
        T.StructField("units_received", T.IntegerType()),
        T.StructField("units_sold", T.IntegerType()),
        T.StructField("units_returned", T.IntegerType()),
    ])

    if rows is None:
        rows = [
            (date(2025, 3, 1), "WH-01", "PROD-A", 100, 20, 15, 2),
            (date(2025, 3, 1), "WH-01", "PROD-B", 50, 10, 8, 1),
            (date(2025, 3, 2), "WH-01", "PROD-A", 107, 30, 20, 3),
        ]

    return spark.createDataFrame(rows, schema=schema)


def _make_dim_date_df(spark, start="2025-01-01", end="2025-12-31"):
    """Build a dim_date DataFrame covering the given range."""
    from azure_lakehouse_etl.gold_dim_date import build_dim_date

    return build_dim_date(spark, start, end)


def _make_dim_product_df(spark, rows=None):
    """Create a minimal dim_product DataFrame with current SCD2 versions."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("product_key", T.StringType()),
        T.StructField(SCD2_EFFECTIVE_START, T.DateType()),
        T.StructField(SCD2_EFFECTIVE_END, T.DateType()),
        T.StructField("scd_is_current", T.BooleanType()),
    ])

    if rows is None:
        rows = [
            ("PROD-A", "pkey_a", date(2025, 1, 1), date(9999, 12, 31), True),
            ("PROD-B", "pkey_b", date(2025, 1, 1), date(9999, 12, 31), True),
        ]

    return spark.createDataFrame(rows, schema=schema)


def _build_default_fact(spark):
    """Build fact_inventory_daily with default test fixtures."""
    return build_fact_inventory_daily(
        silver_inventory_snapshots_df=_make_silver_inventory_snapshots_df(spark),
        dim_date_df=_make_dim_date_df(spark),
        dim_product_df=_make_dim_product_df(spark),
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_fact_inventory_daily_module_exports_table_constants() -> None:
    """Module must expose table name, path, and Silver source constants."""
    assert GOLD_TABLE == "gold.fact_inventory_daily"
    assert "gold/fact_inventory_daily" in GOLD_PATH
    assert SILVER_INVENTORY_SNAPSHOTS_TABLE == "silver.inventory_snapshots"


def test_fact_inventory_daily_columns_includes_grain_components() -> None:
    """FACT_INVENTORY_DAILY_COLUMNS must contain all three grain components."""
    for col in ("snapshot_date", "warehouse_id", "product_id"):
        assert col in FACT_INVENTORY_DAILY_COLUMNS, f"Grain column {col!r} missing"


def test_fact_inventory_daily_columns_includes_dimension_keys() -> None:
    """FACT_INVENTORY_DAILY_COLUMNS must contain date_key and product_key."""
    for key in ("date_key", "product_key"):
        assert key in FACT_INVENTORY_DAILY_COLUMNS, f"FK column {key!r} missing"


def test_fact_inventory_daily_columns_includes_all_measures() -> None:
    """FACT_INVENTORY_DAILY_COLUMNS must contain all four inventory measures."""
    for measure in ("stock_on_hand", "units_received", "units_sold", "units_returned"):
        assert measure in FACT_INVENTORY_DAILY_COLUMNS, f"Measure {measure!r} missing"


def test_build_fact_inventory_daily_signature() -> None:
    """build_fact_inventory_daily must accept three DataFrame parameters."""
    sig = inspect.signature(build_fact_inventory_daily)
    expected = [
        "silver_inventory_snapshots_df",
        "dim_date_df",
        "dim_product_df",
    ]
    assert list(sig.parameters) == expected


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_fact_inventory_daily_returns_expected_columns() -> None:
    """Output must contain exactly the columns in FACT_INVENTORY_DAILY_COLUMNS."""
    spark = _local_spark()
    df = _build_default_fact(spark)
    assert set(df.columns) == set(FACT_INVENTORY_DAILY_COLUMNS)


@requires_java
def test_build_fact_inventory_daily_row_count_matches_silver() -> None:
    """Output must have exactly one row per Silver inventory_snapshots input row.

    With LEFT joins, no fact rows are duplicated or dropped due to dimension
    lookups (unresolved lookups produce NULL keys, not dropped rows).
    """
    spark = _local_spark()
    silver_df = _make_silver_inventory_snapshots_df(spark)
    fact_df = build_fact_inventory_daily(
        silver_inventory_snapshots_df=silver_df,
        dim_date_df=_make_dim_date_df(spark),
        dim_product_df=_make_dim_product_df(spark),
    )
    assert fact_df.count() == silver_df.count()


@requires_java
def test_build_fact_inventory_daily_date_key_is_non_null() -> None:
    """date_key must be non-null when a matching dim_date row exists."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    null_count = df.filter(F.col("date_key").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_fact_inventory_daily_grain_columns_are_non_null() -> None:
    """snapshot_date, warehouse_id, and product_id must be non-null in every row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    for col in ("snapshot_date", "warehouse_id", "product_id"):
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"{col!r} has {null_count} null rows"


@requires_java
def test_build_fact_inventory_daily_product_key_is_non_null() -> None:
    """product_key must resolve when matching dimension rows exist."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    null_count = df.filter(F.col("product_key").isNull()).count()
    assert null_count == 0, f"product_key has {null_count} null rows"


@requires_java
def test_build_fact_inventory_daily_measures_are_non_null() -> None:
    """All four inventory measures must be non-null for valid Silver input."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    for measure in ("stock_on_hand", "units_received", "units_sold", "units_returned"):
        null_count = df.filter(F.col(measure).isNull()).count()
        assert null_count == 0, f"{measure!r} has {null_count} null rows"


@requires_java
def test_build_fact_inventory_daily_is_deterministic() -> None:
    """Two calls with the same inputs must produce identical outputs."""
    spark = _local_spark()
    silver_df = _make_silver_inventory_snapshots_df(spark)
    date_df = _make_dim_date_df(spark)
    prod_df = _make_dim_product_df(spark)

    kwargs = dict(
        silver_inventory_snapshots_df=silver_df,
        dim_date_df=date_df,
        dim_product_df=prod_df,
    )

    def _keyed(df):
        return {
            (r["snapshot_date"], r["warehouse_id"], r["product_id"]): r.asDict()
            for r in df.collect()
        }

    run_a = _keyed(build_fact_inventory_daily(**kwargs))
    run_b = _keyed(build_fact_inventory_daily(**kwargs))
    assert run_a == run_b


@requires_java
def test_build_fact_inventory_daily_pit_join_resolves_correct_product_version() -> None:
    """The SCD2 point-in-time join must select the product version active on the snapshot date.

    Setup:
    - PROD-X has two SCD2 versions:
        version A  effective 2024-01-01 → 2024-06-30  (product_key='pkey_x_v1')
        version B  effective 2024-07-01 → 9999-12-31  (product_key='pkey_x_v2')
    - Two inventory snapshots reference PROD-X on different dates:
        snapshot 2024-03-15 → falls in version A window → expect 'pkey_x_v1'
        snapshot 2024-09-01 → falls in version B window → expect 'pkey_x_v2'

    The fact table must resolve the correct surrogate key for each snapshot date.
    """
    from pyspark.sql import types as T

    spark = _local_spark()

    # Two versions of PROD-X — non-overlapping windows.
    prod_schema = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("product_key", T.StringType()),
        T.StructField(SCD2_EFFECTIVE_START, T.DateType()),
        T.StructField(SCD2_EFFECTIVE_END, T.DateType()),
        T.StructField("scd_is_current", T.BooleanType()),
    ])
    dim_product = spark.createDataFrame(
        [
            ("PROD-X", "pkey_x_v1", date(2024, 1, 1), date(2024, 6, 30), False),
            ("PROD-X", "pkey_x_v2", date(2024, 7, 1), date(9999, 12, 31), True),
        ],
        schema=prod_schema,
    )

    inv_schema = T.StructType([
        T.StructField("snapshot_date", T.DateType()),
        T.StructField("warehouse_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("stock_on_hand", T.IntegerType()),
        T.StructField("units_received", T.IntegerType()),
        T.StructField("units_sold", T.IntegerType()),
        T.StructField("units_returned", T.IntegerType()),
    ])
    silver_inv = spark.createDataFrame(
        [
            (date(2024, 3, 15), "WH-01", "PROD-X", 80, 10, 5, 0),
            (date(2024, 9, 1),  "WH-01", "PROD-X", 60, 15, 8, 1),
        ],
        schema=inv_schema,
    )

    fact_df = build_fact_inventory_daily(
        silver_inventory_snapshots_df=silver_inv,
        dim_date_df=_make_dim_date_df(spark, "2024-01-01", "2024-12-31"),
        dim_product_df=dim_product,
    )

    rows = {r["snapshot_date"]: r["product_key"] for r in fact_df.collect()}

    assert rows[date(2024, 3, 15)] == "pkey_x_v1", (
        "Snapshot on 2024-03-15 must resolve to version A (scd_effective_start=2024-01-01)"
    )
    assert rows[date(2024, 9, 1)] == "pkey_x_v2", (
        "Snapshot on 2024-09-01 must resolve to version B (scd_effective_start=2024-07-01)"
    )
