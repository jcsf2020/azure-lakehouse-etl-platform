"""Local smoke checks for the Gold dim_channel dimensional table."""

from __future__ import annotations

import inspect
import subprocess

import pytest

from azure_lakehouse_etl.gold_dim_channel import (
    DIM_CHANNEL_COLUMNS,
    GOLD_PATH,
    GOLD_TABLE,
    _CHANNEL_KEY_MAP,
    build_dim_channel,
)
from azure_lakehouse_etl.gold_dimensional_foundations import CONFORMED_CHANNELS


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
        .appName("test_gold_dim_channel_smoke")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Non-Spark structural tests (always run)
# ---------------------------------------------------------------------------


def test_gold_dim_channel_module_exports_expected_constants() -> None:
    """dim_channel module must expose the table/path constants and column tuple."""
    assert GOLD_TABLE == "gold.dim_channel"
    assert "gold/dim_channel" in GOLD_PATH
    assert "channel_key" in DIM_CHANNEL_COLUMNS
    assert "channel_name" in DIM_CHANNEL_COLUMNS


def test_build_dim_channel_signature() -> None:
    """build_dim_channel must accept exactly one parameter: spark."""
    sig = inspect.signature(build_dim_channel)
    assert list(sig.parameters) == ["spark"]


def test_channel_key_map_covers_exactly_conformed_channels() -> None:
    """_CHANNEL_KEY_MAP must cover every member of CONFORMED_CHANNELS, no more."""
    assert set(_CHANNEL_KEY_MAP.keys()) == CONFORMED_CHANNELS


def test_channel_key_map_values_are_unique_integers() -> None:
    """Each conformed channel must have a distinct integer surrogate key."""
    values = list(_CHANNEL_KEY_MAP.values())
    assert len(values) == len(set(values))
    for v in values:
        assert isinstance(v, int)


def test_channel_key_map_has_no_null_keys() -> None:
    """No channel name in _CHANNEL_KEY_MAP may be None or empty."""
    for ch in _CHANNEL_KEY_MAP:
        assert ch is not None and ch != ""


# ---------------------------------------------------------------------------
# Spark-dependent smoke tests (skipped when Java is unavailable)
# ---------------------------------------------------------------------------


@requires_java
def test_build_dim_channel_returns_expected_columns() -> None:
    """build_dim_channel must return exactly DIM_CHANNEL_COLUMNS."""
    spark = _local_spark()
    df = build_dim_channel(spark)
    assert set(df.columns) == set(DIM_CHANNEL_COLUMNS)


@requires_java
def test_build_dim_channel_one_row_per_conformed_channel() -> None:
    """Output must contain exactly one row per member of CONFORMED_CHANNELS."""
    spark = _local_spark()
    df = build_dim_channel(spark)
    assert df.count() == len(CONFORMED_CHANNELS)


@requires_java
def test_build_dim_channel_only_conformed_channel_names() -> None:
    """channel_name values must be exactly the set of CONFORMED_CHANNELS."""
    spark = _local_spark()
    df = build_dim_channel(spark)
    channel_names = {r["channel_name"] for r in df.select("channel_name").collect()}
    assert channel_names == CONFORMED_CHANNELS


@requires_java
def test_build_dim_channel_channel_key_is_unique() -> None:
    """channel_key must be unique across all rows."""
    spark = _local_spark()
    df = build_dim_channel(spark)
    total = df.count()
    distinct = df.select("channel_key").distinct().count()
    assert distinct == total


@requires_java
def test_build_dim_channel_no_null_channel_key() -> None:
    """channel_key must never be null."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_dim_channel(spark)
    null_count = df.filter(F.col("channel_key").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_dim_channel_no_null_channel_name() -> None:
    """channel_name must never be null."""
    from pyspark.sql import functions as F

    spark = _local_spark()
    df = build_dim_channel(spark)
    null_count = df.filter(F.col("channel_name").isNull()).count()
    assert null_count == 0


@requires_java
def test_build_dim_channel_is_deterministic() -> None:
    """Two calls must produce identical channel_key → channel_name mappings."""
    spark = _local_spark()

    def collect_map(df):
        return {r["channel_key"]: r["channel_name"] for r in df.collect()}

    run_a = collect_map(build_dim_channel(spark))
    run_b = collect_map(build_dim_channel(spark))
    assert run_a == run_b


@requires_java
def test_build_dim_channel_ordered_by_channel_key() -> None:
    """Output must be ordered ascending by channel_key."""
    spark = _local_spark()
    df = build_dim_channel(spark)
    keys = [r["channel_key"] for r in df.collect()]
    assert keys == sorted(keys)
