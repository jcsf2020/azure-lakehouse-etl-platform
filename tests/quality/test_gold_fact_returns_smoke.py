"""Local smoke checks for the Gold fact_returns fact table."""

from __future__ import annotations

import inspect
import subprocess
from datetime import date

import pytest

from azure_lakehouse_etl.gold_fact_returns import (
    FACT_RETURNS_COLUMNS,
    GOLD_PATH,
    GOLD_TABLE,
    SILVER_ORDER_ITEMS_TABLE,
    SILVER_ORDERS_TABLE,
    SILVER_RETURNS_TABLE,
    build_fact_returns,
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
        .appName("test_gold_fact_returns_smoke")
        .getOrCreate()
    )


def _make_silver_returns_df(spark, rows=None):
    """Create a minimal Silver returns DataFrame for testing."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("return_id", T.StringType()),
        T.StructField("return_date", T.DateType()),
        T.StructField("order_item_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("return_reason", T.StringType()),
        T.StructField("refund_amount", T.DoubleType()),
    ])

    if rows is None:
        rows = [
            ("RET-001", date(2025, 3, 20), "ITEM-001", "PROD-A", "CUST-001", "defective", 19.99),
            ("RET-002", date(2025, 4, 15), "ITEM-002", "PROD-B", "CUST-002", "wrong_item", 44.99),
        ]

    return spark.createDataFrame(rows, schema=schema)


def _make_silver_order_items_df(spark, rows=None):
    """Create a minimal Silver order_items DataFrame for testing."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("order_item_id", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
        T.StructField("line_total", T.DoubleType()),
    ])

    if rows is None:
        rows = [
            ("ITEM-001", "ORD-001", "PROD-A", 2, 19.99, 0.0, 39.98),
            ("ITEM-002", "ORD-002", "PROD-B", 1, 49.99, 5.0, 44.99),
        ]

    return spark.createDataFrame(rows, schema=schema)


def _make_silver_orders_df(spark, rows=None):
    """Create a minimal Silver orders DataFrame for testing."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_date", T.DateType()),
        T.StructField("channel", T.StringType()),
        T.StructField("order_status", T.StringType()),
        T.StructField("order_total", T.DoubleType()),
        T.StructField("shipping_amount", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("payment_method", T.StringType()),
    ])

    if rows is None:
        rows = [
            ("ORD-001", "CUST-001", date(2025, 3, 15), "web", "completed", 39.98, 0.0, 0.0, "USD", "card"),
            ("ORD-002", "CUST-002", date(2025, 4, 10), "marketplace", "completed", 44.99, 5.0, 0.0, "USD", "paypal"),
        ]

    return spark.createDataFrame(rows, schema=schema)


def _make_dim_date_df(spark, start="2025-01-01", end="2025-12-31"):
    """Build a dim_date DataFrame covering the given range."""
    from azure_lakehouse_etl.gold_dim_date import build_dim_date

    return build_dim_date(spark, start, end)


def _make_dim_channel_df(spark):
    """Build the conformed dim_channel DataFrame."""
    from azure_lakehouse_etl.gold_dim_channel import build_dim_channel

    return build_dim_channel(spark)


def _make_dim_customer_df(spark, rows=None):
    """Create a minimal dim_customer DataFrame with current SCD2 versions."""
    from pyspark.sql import types as T

    schema = T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("customer_key", T.StringType()),
        T.StructField(SCD2_EFFECTIVE_START, T.DateType()),
        T.StructField(SCD2_EFFECTIVE_END, T.DateType()),
        T.StructField("scd_is_current", T.BooleanType()),
    ])

    if rows is None:
        rows = [
            ("CUST-001", "ckey_001", date(2025, 1, 1), date(9999, 12, 31), True),
            ("CUST-002", "ckey_002", date(2025, 1, 1), date(9999, 12, 31), True),
        ]

    return spark.createDataFrame(rows, schema=schema)


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
    """Build fact_returns with default test fixtures."""
    return build_fact_returns(
        silver_returns_df=_make_silver_returns_df(spark),
        silver_order_items_df=_make_silver_order_items_df(spark),
        silver_orders_df=_make_silver_orders_df(spark),
        dim_date_df=_make_dim_date_df(spark),
        dim_channel_df=_make_dim_channel_df(spark),
        dim_customer_df=_make_dim_customer_df(spark),
        dim_product_df=_make_dim_product_df(spark),
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_fact_returns_module_exports_table_constants() -> None:
    """Module must expose table name, path, and Silver source constants."""
    assert GOLD_TABLE == "gold.fact_returns"
    assert "gold/fact_returns" in GOLD_PATH
    assert SILVER_RETURNS_TABLE == "silver.returns"
    assert SILVER_ORDER_ITEMS_TABLE == "silver.order_items"
    assert SILVER_ORDERS_TABLE == "silver.orders"


def test_fact_returns_columns_includes_grain_key() -> None:
    """FACT_RETURNS_COLUMNS must contain the return_id grain key."""
    assert "return_id" in FACT_RETURNS_COLUMNS


def test_fact_returns_columns_includes_degenerate_identifiers() -> None:
    """FACT_RETURNS_COLUMNS must carry both order_item_id and order_id."""
    assert "order_item_id" in FACT_RETURNS_COLUMNS
    assert "order_id" in FACT_RETURNS_COLUMNS


def test_fact_returns_columns_includes_all_dimension_keys() -> None:
    """FACT_RETURNS_COLUMNS must contain all four dimension foreign keys."""
    for key in ("date_key", "channel_key", "customer_key", "product_key"):
        assert key in FACT_RETURNS_COLUMNS, f"FK column {key!r} missing"


def test_fact_returns_columns_includes_measure() -> None:
    """FACT_RETURNS_COLUMNS must contain refund_amount."""
    assert "refund_amount" in FACT_RETURNS_COLUMNS


def test_build_fact_returns_signature() -> None:
    """build_fact_returns must accept seven DataFrame parameters."""
    sig = inspect.signature(build_fact_returns)
    expected = [
        "silver_returns_df",
        "silver_order_items_df",
        "silver_orders_df",
        "dim_date_df",
        "dim_channel_df",
        "dim_customer_df",
        "dim_product_df",
    ]
    assert list(sig.parameters) == expected


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_fact_returns_returns_expected_columns() -> None:
    """Output must contain exactly the columns in FACT_RETURNS_COLUMNS."""
    spark = _local_spark()
    df = _build_default_fact(spark)
    assert set(df.columns) == set(FACT_RETURNS_COLUMNS)


@requires_java
def test_build_fact_returns_row_count_matches_silver_returns() -> None:
    """Output must have exactly one row per Silver returns input row.

    With LEFT joins, no fact rows are duplicated or dropped due to dimension
    lookups (unresolved lookups produce NULL keys, not dropped rows).
    """
    spark = _local_spark()
    returns_df = _make_silver_returns_df(spark)
    fact_df = build_fact_returns(
        silver_returns_df=returns_df,
        silver_order_items_df=_make_silver_order_items_df(spark),
        silver_orders_df=_make_silver_orders_df(spark),
        dim_date_df=_make_dim_date_df(spark),
        dim_channel_df=_make_dim_channel_df(spark),
        dim_customer_df=_make_dim_customer_df(spark),
        dim_product_df=_make_dim_product_df(spark),
    )
    assert fact_df.count() == returns_df.count()


@requires_java
def test_build_fact_returns_date_key_is_non_null() -> None:
    """date_key must be non-null when a matching dim_date row exists."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    null_count = df.filter(F.col("date_key").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_fact_returns_grain_identifiers_are_non_null() -> None:
    """return_id and order_item_id must be non-null in every output row."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    for col in ("return_id", "order_item_id"):
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"{col!r} has {null_count} null rows"


@requires_java
def test_build_fact_returns_measure_is_non_null() -> None:
    """refund_amount must be non-null when sourced from valid Silver input."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    null_count = df.filter(F.col("refund_amount").isNull()).count()
    assert null_count == 0, f"refund_amount has {null_count} null rows"


@requires_java
def test_build_fact_returns_customer_and_product_keys_non_null() -> None:
    """customer_key and product_key must resolve when matching dimension rows exist."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    for col in ("customer_key", "product_key"):
        null_count = df.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"{col!r} has {null_count} null rows"


@requires_java
def test_build_fact_returns_is_deterministic() -> None:
    """Two calls with the same inputs must produce identical outputs."""
    spark = _local_spark()
    returns_df = _make_silver_returns_df(spark)
    order_items_df = _make_silver_order_items_df(spark)
    orders_df = _make_silver_orders_df(spark)
    date_df = _make_dim_date_df(spark)
    channel_df = _make_dim_channel_df(spark)
    cust_df = _make_dim_customer_df(spark)
    prod_df = _make_dim_product_df(spark)

    kwargs = dict(
        silver_returns_df=returns_df,
        silver_order_items_df=order_items_df,
        silver_orders_df=orders_df,
        dim_date_df=date_df,
        dim_channel_df=channel_df,
        dim_customer_df=cust_df,
        dim_product_df=prod_df,
    )

    run_a = {r["return_id"]: r.asDict() for r in build_fact_returns(**kwargs).collect()}
    run_b = {r["return_id"]: r.asDict() for r in build_fact_returns(**kwargs).collect()}
    assert run_a == run_b


@requires_java
def test_build_fact_returns_channel_key_resolves_correctly() -> None:
    """channel_key must resolve to the correct dim_channel FK for known channels.

    'web' normalises to 'online' → channel_key 1.
    'marketplace' normalises to 'marketplace' → channel_key 3.
    """
    spark = _local_spark()
    df = _build_default_fact(spark)
    rows = {r["return_id"]: r["channel_key"] for r in df.collect()}
    assert rows["RET-001"] == 1, "web → online → channel_key 1"
    assert rows["RET-002"] == 3, "marketplace → marketplace → channel_key 3"


@requires_java
def test_build_fact_returns_order_id_resolves_from_order_items() -> None:
    """order_id must be populated by joining silver.order_items on order_item_id."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = _build_default_fact(spark)
    rows = {r["return_id"]: r["order_id"] for r in df.collect()}
    assert rows["RET-001"] == "ORD-001"
    assert rows["RET-002"] == "ORD-002"


@requires_java
def test_build_fact_returns_pit_join_resolves_correct_customer_version() -> None:
    """The SCD2 point-in-time join must select the dimension version active on the return date.

    Setup:
    - CUST-001 has two SCD2 versions:
        version A  effective 2023-01-01 → 2023-06-30  (customer_key='ckey_001_v1')
        version B  effective 2023-07-01 → 9999-12-31  (customer_key='ckey_001_v2')
    - Two returns reference CUST-001 on different return dates:
        RET-A  return_date=2023-03-15 → falls in version A window → expect 'ckey_001_v1'
        RET-B  return_date=2023-08-01 → falls in version B window → expect 'ckey_001_v2'

    The fact table must resolve the correct surrogate key for each return date.
    """
    from pyspark.sql import types as T

    spark = _local_spark()

    # Two versions of CUST-001 — non-overlapping windows.
    cust_schema = T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("customer_key", T.StringType()),
        T.StructField(SCD2_EFFECTIVE_START, T.DateType()),
        T.StructField(SCD2_EFFECTIVE_END, T.DateType()),
        T.StructField("scd_is_current", T.BooleanType()),
    ])
    dim_customer = spark.createDataFrame(
        [
            ("CUST-001", "ckey_001_v1", date(2023, 1, 1), date(2023, 6, 30), False),
            ("CUST-001", "ckey_001_v2", date(2023, 7, 1), date(9999, 12, 31), True),
        ],
        schema=cust_schema,
    )

    # Single product version covering the full year.
    prod_schema = T.StructType([
        T.StructField("product_id", T.StringType()),
        T.StructField("product_key", T.StringType()),
        T.StructField(SCD2_EFFECTIVE_START, T.DateType()),
        T.StructField(SCD2_EFFECTIVE_END, T.DateType()),
        T.StructField("scd_is_current", T.BooleanType()),
    ])
    dim_product = spark.createDataFrame(
        [("PROD-X", "pkey_x", date(2023, 1, 1), date(9999, 12, 31), True)],
        schema=prod_schema,
    )

    returns_schema = T.StructType([
        T.StructField("return_id", T.StringType()),
        T.StructField("return_date", T.DateType()),
        T.StructField("order_item_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("return_reason", T.StringType()),
        T.StructField("refund_amount", T.DoubleType()),
    ])
    silver_returns = spark.createDataFrame(
        [
            ("RET-A", date(2023, 3, 15), "ITEM-A", "PROD-X", "CUST-001", "defective", 10.0),
            ("RET-B", date(2023, 8, 1), "ITEM-B", "PROD-X", "CUST-001", "wrong_item", 20.0),
        ],
        schema=returns_schema,
    )

    order_items_schema = T.StructType([
        T.StructField("order_item_id", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
        T.StructField("line_total", T.DoubleType()),
    ])
    silver_order_items = spark.createDataFrame(
        [
            ("ITEM-A", "ORD-A", "PROD-X", 1, 10.0, 0.0, 10.0),
            ("ITEM-B", "ORD-B", "PROD-X", 1, 20.0, 0.0, 20.0),
        ],
        schema=order_items_schema,
    )

    orders_schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_date", T.DateType()),
        T.StructField("channel", T.StringType()),
        T.StructField("order_status", T.StringType()),
        T.StructField("order_total", T.DoubleType()),
        T.StructField("shipping_amount", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("payment_method", T.StringType()),
    ])
    silver_orders = spark.createDataFrame(
        [
            ("ORD-A", "CUST-001", date(2023, 2, 10), "web", "completed", 10.0, 0.0, 0.0, "USD", "card"),
            ("ORD-B", "CUST-001", date(2023, 7, 20), "web", "completed", 20.0, 0.0, 0.0, "USD", "card"),
        ],
        schema=orders_schema,
    )

    fact_df = build_fact_returns(
        silver_returns_df=silver_returns,
        silver_order_items_df=silver_order_items,
        silver_orders_df=silver_orders,
        dim_date_df=_make_dim_date_df(spark, "2023-01-01", "2023-12-31"),
        dim_channel_df=_make_dim_channel_df(spark),
        dim_customer_df=dim_customer,
        dim_product_df=dim_product,
    )

    rows = {r["return_id"]: r["customer_key"] for r in fact_df.collect()}

    assert rows["RET-A"] == "ckey_001_v1", (
        "Return on 2023-03-15 must resolve to version A (scd_effective_start=2023-01-01)"
    )
    assert rows["RET-B"] == "ckey_001_v2", (
        "Return on 2023-08-01 must resolve to version B (scd_effective_start=2023-07-01)"
    )
