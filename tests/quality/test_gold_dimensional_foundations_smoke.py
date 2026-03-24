"""Local smoke checks for the Gold dimensional foundation helpers."""

from __future__ import annotations

import inspect
import subprocess
from datetime import date

import pytest

from azure_lakehouse_etl.gold_dimensional_foundations import (
    CHANNEL_MAP,
    CHANNEL_UNKNOWN,
    CONFORMED_CHANNELS,
    DATE_DIM_COLUMNS,
    SCD2_EFFECTIVE_END,
    SCD2_EFFECTIVE_START,
    SCD2_END_SENTINEL,
    SCD2_IS_CURRENT,
    SCD2_ROW_HASH,
    SCD2_TECHNICAL_COLUMNS,
    add_scd2_columns,
    build_date_dimension,
    normalize_channel,
    normalize_channel_col,
    row_hash,
    surrogate_key,
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
        .appName("test_gold_dimensional_foundations_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_scd2_column_name_constants_are_strings() -> None:
    """SCD2 convention constants must be non-empty strings."""
    for name in (SCD2_EFFECTIVE_START, SCD2_EFFECTIVE_END, SCD2_IS_CURRENT, SCD2_ROW_HASH):
        assert isinstance(name, str) and name


def test_scd2_technical_columns_tuple_covers_all_four_fields() -> None:
    """SCD2_TECHNICAL_COLUMNS must contain all four technical column names."""
    assert set(SCD2_TECHNICAL_COLUMNS) == {
        SCD2_EFFECTIVE_START,
        SCD2_EFFECTIVE_END,
        SCD2_IS_CURRENT,
        SCD2_ROW_HASH,
    }


def test_scd2_end_sentinel_is_far_future() -> None:
    """SCD2_END_SENTINEL must be a date far in the future (year >= 9999)."""
    assert isinstance(SCD2_END_SENTINEL, date)
    assert SCD2_END_SENTINEL.year >= 9999


def test_date_dim_columns_contains_expected_fields() -> None:
    """DATE_DIM_COLUMNS must include at minimum the core calendar fields."""
    required = {"date_key", "full_date", "year", "quarter", "month", "day_of_month", "is_weekend"}
    assert required.issubset(set(DATE_DIM_COLUMNS))


def test_conformed_channels_covers_expected_labels() -> None:
    """CONFORMED_CHANNELS must include the four business channels plus 'other'."""
    assert CONFORMED_CHANNELS == {"online", "in_store", "marketplace", "wholesale", "other"}


def test_channel_map_values_are_all_conformed() -> None:
    """Every value in CHANNEL_MAP must be a member of CONFORMED_CHANNELS."""
    for raw, conformed in CHANNEL_MAP.items():
        assert conformed in CONFORMED_CHANNELS, (
            f"CHANNEL_MAP[{raw!r}] = {conformed!r} is not in CONFORMED_CHANNELS"
        )


def test_channel_unknown_is_in_conformed_channels() -> None:
    assert CHANNEL_UNKNOWN in CONFORMED_CHANNELS


# ---------------------------------------------------------------------------
# normalize_channel — pure Python, no Spark required
# ---------------------------------------------------------------------------


def test_normalize_channel_known_values() -> None:
    """Known raw channel strings must map to their conformed labels."""
    assert normalize_channel("web") == "online"
    assert normalize_channel("Website") == "online"       # case-insensitive
    assert normalize_channel("  store  ") == "in_store"   # whitespace stripped
    assert normalize_channel("amazon") == "marketplace"
    assert normalize_channel("wholesale") == "wholesale"
    assert normalize_channel("b2b") == "wholesale"


def test_normalize_channel_unknown_returns_other() -> None:
    assert normalize_channel("postal") == CHANNEL_UNKNOWN
    assert normalize_channel("fax") == CHANNEL_UNKNOWN


def test_normalize_channel_null_returns_other() -> None:
    assert normalize_channel(None) == CHANNEL_UNKNOWN


def test_normalize_channel_is_deterministic() -> None:
    """Same input must always produce the same output."""
    assert normalize_channel("online") == normalize_channel("online")
    assert normalize_channel("ONLINE") == normalize_channel("Online")


# ---------------------------------------------------------------------------
# Function signature contracts (no Spark)
# ---------------------------------------------------------------------------


def test_surrogate_key_accepts_varargs() -> None:
    sig = inspect.signature(surrogate_key)
    params = list(sig.parameters.values())
    assert len(params) == 1
    import inspect as _inspect
    assert params[0].kind == _inspect.Parameter.VAR_POSITIONAL


def test_row_hash_accepts_varargs() -> None:
    sig = inspect.signature(row_hash)
    params = list(sig.parameters.values())
    assert len(params) == 1
    import inspect as _inspect
    assert params[0].kind == _inspect.Parameter.VAR_POSITIONAL


def test_add_scd2_columns_signature() -> None:
    sig = inspect.signature(add_scd2_columns)
    assert list(sig.parameters) == ["df", "effective_start_date", "tracked_col_names"]


def test_build_date_dimension_signature() -> None:
    sig = inspect.signature(build_date_dimension)
    assert list(sig.parameters) == ["spark", "start_date", "end_date"]


def test_normalize_channel_col_signature() -> None:
    sig = inspect.signature(normalize_channel_col)
    assert list(sig.parameters) == ["col_name"]


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_surrogate_key_is_deterministic_and_non_null() -> None:
    """surrogate_key must return identical non-null hex strings for the same input."""
    spark = _local_spark()
    rows = [{"id": "CUST-001", "source": "erp"}]
    df = spark.createDataFrame(rows)

    result = (
        df.withColumn("sk", surrogate_key("id", "source"))
        .select("sk")
        .collect()
    )
    value = result[0]["sk"]
    assert value is not None
    assert len(value) == 64  # SHA-256 produces 64 hex chars

    # Same input must yield the same key
    result2 = (
        df.withColumn("sk", surrogate_key("id", "source"))
        .select("sk")
        .collect()
    )
    assert result2[0]["sk"] == value


@requires_java
def test_surrogate_key_null_column_does_not_produce_null() -> None:
    """A NULL natural key column must not produce a NULL surrogate key."""
    spark = _local_spark()
    rows = [{"id": None, "source": "erp"}]
    df = spark.createDataFrame(rows)
    result = df.withColumn("sk", surrogate_key("id", "source")).select("sk").collect()
    assert result[0]["sk"] is not None


@requires_java
def test_row_hash_is_deterministic_and_non_null() -> None:
    """row_hash must return identical non-null hex strings for the same input."""
    spark = _local_spark()
    rows = [{"name": "Alice", "country": "US"}]
    df = spark.createDataFrame(rows)

    result = (
        df.withColumn("rh", row_hash("name", "country"))
        .select("rh")
        .collect()
    )
    value = result[0]["rh"]
    assert value is not None
    assert len(value) == 32  # MD5 produces 32 hex chars

    result2 = (
        df.withColumn("rh", row_hash("name", "country"))
        .select("rh")
        .collect()
    )
    assert result2[0]["rh"] == value


@requires_java
def test_row_hash_changes_when_tracked_column_changes() -> None:
    """Different values must produce different row hashes."""
    spark = _local_spark()
    row_a = spark.createDataFrame([{"name": "Alice", "country": "US"}])
    row_b = spark.createDataFrame([{"name": "Alice", "country": "UK"}])

    hash_a = row_a.withColumn("rh", row_hash("name", "country")).collect()[0]["rh"]
    hash_b = row_b.withColumn("rh", row_hash("name", "country")).collect()[0]["rh"]
    assert hash_a != hash_b


@requires_java
def test_add_scd2_columns_adds_all_technical_fields() -> None:
    """add_scd2_columns must add scd_effective_start/end, scd_is_current, scd_row_hash."""
    spark = _local_spark()
    df = spark.createDataFrame([{"customer_id": "C1", "name": "Alice", "country": "US"}])
    effective = date(2024, 1, 15)

    result = add_scd2_columns(df, effective, tracked_col_names=["name", "country"])
    col_names = result.columns

    for col in SCD2_TECHNICAL_COLUMNS:
        assert col in col_names, f"Expected column {col!r} missing from result"

    row = result.collect()[0]
    assert str(row[SCD2_EFFECTIVE_START]) == "2024-01-15"
    assert str(row[SCD2_EFFECTIVE_END]) == str(SCD2_END_SENTINEL)
    assert row[SCD2_IS_CURRENT] is True
    assert row[SCD2_ROW_HASH] is not None
    assert len(row[SCD2_ROW_HASH]) == 32  # MD5


@requires_java
def test_add_scd2_row_hash_matches_row_hash_function() -> None:
    """The hash written by add_scd2_columns must equal the row_hash Column output."""
    spark = _local_spark()
    df = spark.createDataFrame([{"name": "Bob", "country": "CA"}])

    scd_result = add_scd2_columns(df, date(2024, 1, 1), ["name", "country"])
    scd_hash = scd_result.select(SCD2_ROW_HASH).collect()[0][SCD2_ROW_HASH]

    rh_result = df.withColumn("rh", row_hash("name", "country")).select("rh").collect()
    rh_hash = rh_result[0]["rh"]

    assert scd_hash == rh_hash


@requires_java
def test_build_date_dimension_returns_expected_columns() -> None:
    """build_date_dimension must return a DataFrame with all DATE_DIM_COLUMNS."""
    spark = _local_spark()
    df = build_date_dimension(spark, "2024-01-01", "2024-01-07")

    assert set(df.columns) == set(DATE_DIM_COLUMNS)


@requires_java
def test_build_date_dimension_row_count_and_keys() -> None:
    """Date dimension must contain exactly one row per calendar day in the range."""
    spark = _local_spark()
    df = build_date_dimension(spark, "2024-01-01", "2024-01-03")
    rows = {r["date_key"]: r for r in df.collect()}

    assert set(rows.keys()) == {20240101, 20240102, 20240103}

    jan1 = rows[20240101]
    assert jan1["year"] == 2024
    assert jan1["quarter"] == 1
    assert jan1["month"] == 1
    assert jan1["day_of_month"] == 1
    assert jan1["month_name"] == "January"
    assert jan1["day_name"] == "Monday"
    assert jan1["is_weekend"] is False


@requires_java
def test_build_date_dimension_weekend_flag() -> None:
    """is_weekend must be True for Saturday (day_of_week=7) and Sunday (day_of_week=1)."""
    spark = _local_spark()
    # 2024-01-06 = Saturday, 2024-01-07 = Sunday
    df = build_date_dimension(spark, "2024-01-06", "2024-01-07")
    rows = df.collect()

    for row in rows:
        assert row["is_weekend"] is True, (
            f"Expected is_weekend=True for {row['full_date']}"
        )


@requires_java
def test_build_date_dimension_raises_when_end_before_start() -> None:
    """build_date_dimension must raise ValueError for an inverted date range."""
    spark = _local_spark()
    with pytest.raises(ValueError, match="end_date"):
        build_date_dimension(spark, "2024-03-01", "2024-01-01")


@requires_java
def test_normalize_channel_col_maps_known_values() -> None:
    """normalize_channel_col must produce conformed labels via native Spark expression."""
    spark = _local_spark()
    rows = [
        {"raw": "web"},
        {"raw": "STORE"},
        {"raw": "Amazon"},
        {"raw": "b2b"},
        {"raw": "unknown_xyz"},
        {"raw": None},
    ]
    df = spark.createDataFrame(rows)
    result = (
        df.withColumn("channel", normalize_channel_col("raw"))
        .select("raw", "channel")
        .collect()
    )
    result_map = {r["raw"]: r["channel"] for r in result}

    assert result_map["web"] == "online"
    assert result_map["STORE"] == "in_store"
    assert result_map["Amazon"] == "marketplace"
    assert result_map["b2b"] == "wholesale"
    assert result_map["unknown_xyz"] == CHANNEL_UNKNOWN
    assert result_map[None] == CHANNEL_UNKNOWN


@requires_java
def test_normalize_channel_col_output_always_in_conformed_channels() -> None:
    """Every output from normalize_channel_col must be a member of CONFORMED_CHANNELS."""
    spark = _local_spark()
    raw_values = list(CHANNEL_MAP.keys()) + ["garbage", None]
    rows = [{"raw": v} for v in raw_values]
    df = spark.createDataFrame(rows)
    result = (
        df.withColumn("channel", normalize_channel_col("raw"))
        .select("channel")
        .distinct()
        .collect()
    )
    for row in result:
        assert row["channel"] in CONFORMED_CHANNELS, (
            f"Unexpected channel value: {row['channel']!r}"
        )
