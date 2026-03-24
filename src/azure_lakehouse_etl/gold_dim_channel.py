"""Gold dimensional table helpers for dim_channel."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from azure_lakehouse_etl.gold_dimensional_foundations import (
    CHANNEL_UNKNOWN,
    CONFORMED_CHANNELS,
)


GOLD_TABLE = "gold.dim_channel"
GOLD_PATH = "dbfs:/tmp/azure_lakehouse_etl/gold/dim_channel"

DIM_CHANNEL_COLUMNS = (
    "channel_key",
    "channel_name",
    "channel_label",
    "channel_group",
)

# Stable integer surrogate keys — one per conformed channel value.
# These must never be reassigned; new channels receive new keys.
_CHANNEL_KEY_MAP: dict[str, int] = {
    "online": 1,
    "in_store": 2,
    "marketplace": 3,
    "wholesale": 4,
    CHANNEL_UNKNOWN: 5,
}

# Business-facing display labels aligned with channel_name values.
_CHANNEL_LABEL_MAP: dict[str, str] = {
    "online": "Online",
    "in_store": "In Store",
    "marketplace": "Marketplace",
    "wholesale": "Wholesale",
    CHANNEL_UNKNOWN: "Other",
}

# Business groupings for higher-level reporting.
_CHANNEL_GROUP_MAP: dict[str, str] = {
    "online": "digital",
    "in_store": "physical",
    "marketplace": "digital",
    "wholesale": "b2b",
    CHANNEL_UNKNOWN: "other",
}

assert set(_CHANNEL_KEY_MAP) == CONFORMED_CHANNELS, (
    f"_CHANNEL_KEY_MAP keys {set(_CHANNEL_KEY_MAP)} must match CONFORMED_CHANNELS {CONFORMED_CHANNELS}"
)

_SCHEMA = T.StructType([
    T.StructField("channel_key", T.IntegerType(), nullable=False),
    T.StructField("channel_name", T.StringType(), nullable=False),
    T.StructField("channel_label", T.StringType(), nullable=False),
    T.StructField("channel_group", T.StringType(), nullable=False),
])


def build_dim_channel(spark: SparkSession) -> DataFrame:
    """Build the Gold dim_channel conformed dimension.

    Derives one row per conformed channel from the project channel normalization
    contract (CONFORMED_CHANNELS / CHANNEL_UNKNOWN).  Output is compact,
    deterministic, and ordered by channel_key ascending.

    Parameters
    ----------
    spark : active SparkSession

    Returns
    -------
    DataFrame with columns: channel_key, channel_name, channel_label,
    channel_group.  Exactly one row per member of CONFORMED_CHANNELS.
    """
    rows = [
        (
            _CHANNEL_KEY_MAP[ch],
            ch,
            _CHANNEL_LABEL_MAP[ch],
            _CHANNEL_GROUP_MAP[ch],
        )
        for ch in sorted(_CHANNEL_KEY_MAP, key=lambda c: _CHANNEL_KEY_MAP[c])
    ]

    return (
        spark.createDataFrame(rows, schema=_SCHEMA)
        .select(*DIM_CHANNEL_COLUMNS)
        .orderBy("channel_key")
    )
