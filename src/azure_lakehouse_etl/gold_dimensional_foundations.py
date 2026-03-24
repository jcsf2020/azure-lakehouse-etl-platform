"""Reusable dimensional foundation helpers for the Gold serving layer.

Provides building blocks for:
- Deterministic surrogate key generation
- SCD2 technical columns and conventions
- Row hash for change detection
- Date dimension generation
- Channel normalization (conformed dimension)
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# SCD2 column name conventions
# ---------------------------------------------------------------------------

SCD2_EFFECTIVE_START = "scd_effective_start"
SCD2_EFFECTIVE_END = "scd_effective_end"
SCD2_IS_CURRENT = "scd_is_current"
SCD2_ROW_HASH = "scd_row_hash"
SCD2_END_SENTINEL = date(9999, 12, 31)

SCD2_TECHNICAL_COLUMNS = (
    SCD2_EFFECTIVE_START,
    SCD2_EFFECTIVE_END,
    SCD2_IS_CURRENT,
    SCD2_ROW_HASH,
)

# ---------------------------------------------------------------------------
# Date dimension column contract
# ---------------------------------------------------------------------------

DATE_DIM_COLUMNS = (
    "date_key",       # int YYYYMMDD — compact integer primary key
    "full_date",      # DateType
    "year",
    "quarter",
    "month",
    "month_name",
    "day_of_month",
    "day_of_week",    # 1=Sunday … 7=Saturday (Spark dayofweek convention)
    "day_name",
    "week_of_year",
    "is_weekend",
)

# ---------------------------------------------------------------------------
# Channel normalization contract
# ---------------------------------------------------------------------------

CHANNEL_MAP: dict[str, str] = {
    "web": "online",
    "website": "online",
    "online": "online",
    "e-commerce": "online",
    "ecommerce": "online",
    "store": "in_store",
    "in-store": "in_store",
    "in_store": "in_store",
    "retail": "in_store",
    "physical": "in_store",
    "marketplace": "marketplace",
    "amazon": "marketplace",
    "ebay": "marketplace",
    "third_party": "marketplace",
    "wholesale": "wholesale",
    "b2b": "wholesale",
    "distributor": "wholesale",
}

CHANNEL_UNKNOWN = "other"
CONFORMED_CHANNELS = frozenset({"online", "in_store", "marketplace", "wholesale", CHANNEL_UNKNOWN})


# ---------------------------------------------------------------------------
# Surrogate key
# ---------------------------------------------------------------------------


def surrogate_key(*col_names: str) -> F.Column:
    """Return a deterministic surrogate key Column (SHA-256 hex string).

    Concatenates natural key column values separated by '|', casting each to
    string and treating NULL as empty string so the hash is always non-null.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in col_names]
    return F.sha2(F.concat_ws("|", *parts), 256)


# ---------------------------------------------------------------------------
# Row hash for SCD2 change detection
# ---------------------------------------------------------------------------


def row_hash(*col_names: str) -> F.Column:
    """Return a stable MD5 hash Column over the given columns.

    Used for SCD2 change detection: if the hash of an incoming record differs
    from the hash of the current active record, a new SCD2 version is opened.
    NULL values are coerced to empty string before hashing so the result is
    always non-null.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in col_names]
    return F.md5(F.concat_ws("|", *parts))


# ---------------------------------------------------------------------------
# SCD2 helper — stamp technical columns onto an incoming DataFrame
# ---------------------------------------------------------------------------


def add_scd2_columns(
    df: DataFrame,
    effective_start_date: date,
    tracked_col_names: list[str],
) -> DataFrame:
    """Add SCD2 technical columns to an incoming DataFrame of new record versions.

    Columns added:
    - scd_effective_start  DateType — when this version becomes active
    - scd_effective_end    DateType — sentinel 9999-12-31 (open / current)
    - scd_is_current       BooleanType — True for all incoming rows
    - scd_row_hash         StringType — MD5 of tracked_col_names

    Parameters
    ----------
    df                  : incoming DataFrame (new/changed records)
    effective_start_date: the business date on which these versions go live
    tracked_col_names   : columns included in change-detection hash
    """
    hash_expr = F.md5(
        F.concat_ws(
            "|",
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked_col_names],
        )
    )

    return (
        df.withColumn(
            SCD2_EFFECTIVE_START,
            F.lit(effective_start_date).cast(T.DateType()),
        )
        .withColumn(
            SCD2_EFFECTIVE_END,
            F.lit(SCD2_END_SENTINEL).cast(T.DateType()),
        )
        .withColumn(SCD2_IS_CURRENT, F.lit(True))
        .withColumn(SCD2_ROW_HASH, hash_expr)
    )


# ---------------------------------------------------------------------------
# Date dimension
# ---------------------------------------------------------------------------


def build_date_dimension(
    spark: SparkSession,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """Generate a date dimension DataFrame for the given inclusive date range.

    Parameters
    ----------
    spark      : active SparkSession
    start_date : ISO string 'YYYY-MM-DD' (inclusive)
    end_date   : ISO string 'YYYY-MM-DD' (inclusive)

    Returns
    -------
    DataFrame with columns: date_key, full_date, year, quarter, month,
    month_name, day_of_month, day_of_week, day_name, week_of_year, is_weekend.
    Ordered by full_date ascending.

    Raises
    ------
    ValueError if end_date is before start_date.
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError(
            f"end_date {end_date!r} must not be before start_date {start_date!r}"
        )

    dates = []
    current = start
    while current <= end:
        dates.append((current,))
        current += timedelta(days=1)

    schema = T.StructType([T.StructField("full_date", T.DateType())])
    date_df = spark.createDataFrame(dates, schema=schema)

    return (
        date_df
        .withColumn(
            "date_key",
            F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"),
        )
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))   # 1=Sun, 7=Sat
        .withColumn("day_name", F.date_format(F.col("full_date"), "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .select(*DATE_DIM_COLUMNS)
        .orderBy("full_date")
    )


# ---------------------------------------------------------------------------
# Channel normalization
# ---------------------------------------------------------------------------


def normalize_channel(raw: str | None) -> str:
    """Return the conformed channel label for a raw channel string.

    Lookup is case-insensitive and strips surrounding whitespace.
    Unknown or null values map to CHANNEL_UNKNOWN ('other').
    """
    if raw is None:
        return CHANNEL_UNKNOWN
    return CHANNEL_MAP.get(raw.strip().lower(), CHANNEL_UNKNOWN)


def normalize_channel_col(col_name: str) -> F.Column:
    """Return a Spark Column expression mapping raw channel values to conformed labels.

    Builds a native CASE WHEN expression from CHANNEL_MAP — no Python UDF,
    so it runs entirely on the executor.  The match is case-insensitive and
    trims surrounding whitespace.  Unrecognised values and NULLs map to
    CHANNEL_UNKNOWN ('other').
    """
    expr = F.when(F.col(col_name).isNull(), F.lit(CHANNEL_UNKNOWN))
    for raw, conformed in CHANNEL_MAP.items():
        expr = expr.when(
            F.lower(F.trim(F.col(col_name))) == F.lit(raw),
            F.lit(conformed),
        )
    return expr.otherwise(F.lit(CHANNEL_UNKNOWN))
