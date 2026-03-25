"""Local Gold demo runner — materialises the Gold layer from seed fixtures.

Builds a small set of deterministic Gold outputs to a local directory using
the real Gold build functions. Suitable for local inspection, KPI queries,
and portfolio demonstration without Databricks or cloud services.

Public API
----------
build_local_spark()       — SparkSession with macOS/local binding fix
build_gold_datasets()     — returns dict[name, DataFrame] for all seven Gold tables
write_demo_outputs()      — writes each DataFrame to artifacts/demo_gold/<name>/
run_demo()                — convenience: build + write in one call
"""

from __future__ import annotations

import csv
import json
import os
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEMO_DATE_START = "2026-01-01"
DEMO_DATE_END = "2026-12-31"
DEMO_EFFECTIVE_DATE = date(2026, 3, 15)

_REPO_ROOT = Path(__file__).resolve().parents[2]
SEED_DIR = _REPO_ROOT / "data_samples" / "seed_files"
DEFAULT_OUTPUT_DIR = _REPO_ROOT / "artifacts" / "demo_gold"

DEMO_DATASETS = (
    "dim_date",
    "dim_channel",
    "dim_customer",
    "dim_product",
    "fact_order_items",
    "fact_returns",
    "fact_inventory_daily",
)


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------


def build_local_spark(app_name: str = "gold_demo_runner") -> SparkSession:
    """Return a local SparkSession configured for macOS/local execution.

    Forces driver.host and driver.bindAddress to 127.0.0.1 to avoid the
    network-interface binding issue observed on this Mac when multiple
    interfaces are present. SPARK_LOCAL_IP is set for the same reason.

    Safe to call multiple times — getOrCreate() returns the active session
    if one is already running.
    """
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Silver fixtures — load seed files and apply Silver transformations
# ---------------------------------------------------------------------------


def _load_customers_silver(spark: SparkSession) -> DataFrame:
    from azure_lakehouse_etl.silver_customers import apply_schema_and_basic_quality

    with open(SEED_DIR / "customers.json") as fh:
        records = json.load(fh)

    schema = T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("first_name", T.StringType()),
        T.StructField("last_name", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("registration_date", T.StringType()),
        T.StructField("customer_status", T.StringType()),
    ])

    rows = [
        (r["customer_id"], r["first_name"], r["last_name"], r["email"],
         r["country"], r["city"], r["registration_date"], r["customer_status"])
        for r in records
    ]

    return apply_schema_and_basic_quality(spark.createDataFrame(rows, schema=schema))


def _load_products_silver(spark: SparkSession) -> DataFrame:
    from azure_lakehouse_etl.silver_products import apply_schema_and_basic_quality

    with open(SEED_DIR / "products.json") as fh:
        records = json.load(fh)

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
        T.StructField("last_updated", T.StringType()),
    ])

    rows = [
        (r["product_id"], r["product_name"], r["category"], r["subcategory"],
         r["brand"], float(r["list_price"]), float(r["cost_price"]),
         r["currency"], bool(r["is_active"]), r["last_updated"])
        for r in records
    ]

    return apply_schema_and_basic_quality(spark.createDataFrame(rows, schema=schema))


def _load_orders_silver(spark: SparkSession) -> DataFrame:
    """Load seed orders.

    The seed file omits payment_method, shipping_amount, and discount_amount
    (fields added by later pipeline stages). Defaults are injected here so
    the Silver quality function receives a complete schema.
    """
    from azure_lakehouse_etl.silver_orders import apply_schema_and_basic_quality

    with open(SEED_DIR / "orders.json") as fh:
        records = json.load(fh)

    schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("order_date", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("order_status", T.StringType()),
        T.StructField("currency", T.StringType()),
        T.StructField("order_total", T.DoubleType()),
        T.StructField("payment_method", T.StringType()),
        T.StructField("shipping_amount", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
    ])

    rows = [
        (r["order_id"], r["customer_id"], r["order_date"], r["channel"],
         r["order_status"], r["currency"], float(r["order_total"]),
         "card", 0.0, 0.0)
        for r in records
    ]

    return apply_schema_and_basic_quality(spark.createDataFrame(rows, schema=schema))


def _load_order_items_silver(spark: SparkSession) -> DataFrame:
    from azure_lakehouse_etl.silver_order_items import apply_schema_and_basic_quality

    with open(SEED_DIR / "order_items.json") as fh:
        records = json.load(fh)

    schema = T.StructType([
        T.StructField("order_item_id", T.StringType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()),
        T.StructField("discount_amount", T.DoubleType()),
        T.StructField("line_total", T.DoubleType()),
    ])

    rows = [
        (r["order_item_id"], r["order_id"], r["product_id"],
         int(r["quantity"]), float(r["unit_price"]),
         float(r["discount_amount"]), float(r["line_total"]))
        for r in records
    ]

    return apply_schema_and_basic_quality(spark.createDataFrame(rows, schema=schema))


def _load_returns_silver(spark: SparkSession) -> DataFrame:
    from azure_lakehouse_etl.silver_returns import apply_schema_and_basic_quality

    rows = []
    with open(SEED_DIR / "returns.csv") as fh:
        non_blank = (line for line in fh if line.strip())
        reader = csv.DictReader(non_blank)
        for r in reader:
            rows.append((
                r["return_id"], r["return_date"], r["order_item_id"],
                r["product_id"], r["customer_id"], r["return_reason"],
                float(r["refund_amount"]),
            ))

    schema = T.StructType([
        T.StructField("return_id", T.StringType()),
        T.StructField("return_date", T.StringType()),
        T.StructField("order_item_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("return_reason", T.StringType()),
        T.StructField("refund_amount", T.DoubleType()),
    ])

    return apply_schema_and_basic_quality(spark.createDataFrame(rows, schema=schema))


def _load_inventory_snapshots_silver(spark: SparkSession) -> DataFrame:
    from azure_lakehouse_etl.silver_inventory_snapshots import apply_schema_and_basic_quality

    rows = []
    with open(SEED_DIR / "inventory_snapshots.csv") as fh:
        non_blank = (line for line in fh if line.strip())
        reader = csv.DictReader(non_blank)
        for r in reader:
            rows.append((
                r["snapshot_date"], r["warehouse_id"], r["product_id"],
                int(r["stock_on_hand"]), int(r["units_received"]),
                int(r["units_sold"]), int(r["units_returned"]),
            ))

    schema = T.StructType([
        T.StructField("snapshot_date", T.StringType()),
        T.StructField("warehouse_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("stock_on_hand", T.IntegerType()),
        T.StructField("units_received", T.IntegerType()),
        T.StructField("units_sold", T.IntegerType()),
        T.StructField("units_returned", T.IntegerType()),
    ])

    return apply_schema_and_basic_quality(spark.createDataFrame(rows, schema=schema))


# ---------------------------------------------------------------------------
# Gold dataset builder
# ---------------------------------------------------------------------------


def build_gold_datasets(
    spark: SparkSession,
    *,
    date_start: str = DEMO_DATE_START,
    date_end: str = DEMO_DATE_END,
    effective_date: date = DEMO_EFFECTIVE_DATE,
) -> dict[str, DataFrame]:
    """Build all seven Gold demo datasets from seed fixtures.

    Calls the real Gold build functions — no transformation logic is duplicated
    here. The seed data covers 2026-03-15 to 2026-03-17; the default date range
    (2026-01-01 to 2026-12-31) spans the full demo year so all fact rows resolve
    their date_key FK.

    Returns a mapping of dataset name → DataFrame.
    """
    from azure_lakehouse_etl.gold_dim_date import build_dim_date
    from azure_lakehouse_etl.gold_dim_channel import build_dim_channel
    from azure_lakehouse_etl.gold_dim_customer import build_gold_dim_customer
    from azure_lakehouse_etl.gold_dim_product import build_gold_dim_product
    from azure_lakehouse_etl.gold_fact_order_items import build_fact_order_items
    from azure_lakehouse_etl.gold_fact_returns import build_fact_returns
    from azure_lakehouse_etl.gold_fact_inventory_daily import build_fact_inventory_daily

    silver_customers = _load_customers_silver(spark)
    silver_products = _load_products_silver(spark)
    silver_orders = _load_orders_silver(spark)
    silver_order_items = _load_order_items_silver(spark)
    silver_returns = _load_returns_silver(spark)
    silver_inventory = _load_inventory_snapshots_silver(spark)

    dim_date = build_dim_date(spark, date_start, date_end)
    dim_channel = build_dim_channel(spark)
    dim_customer = build_gold_dim_customer(silver_customers, effective_date)
    dim_product = build_gold_dim_product(silver_products, effective_date)

    fact_order_items = build_fact_order_items(
        silver_order_items_df=silver_order_items,
        silver_orders_df=silver_orders,
        dim_date_df=dim_date,
        dim_channel_df=dim_channel,
        dim_customer_df=dim_customer,
        dim_product_df=dim_product,
    )

    fact_returns = build_fact_returns(
        silver_returns_df=silver_returns,
        silver_order_items_df=silver_order_items,
        silver_orders_df=silver_orders,
        dim_date_df=dim_date,
        dim_channel_df=dim_channel,
        dim_customer_df=dim_customer,
        dim_product_df=dim_product,
    )

    fact_inventory_daily = build_fact_inventory_daily(
        silver_inventory_snapshots_df=silver_inventory,
        dim_date_df=dim_date,
        dim_product_df=dim_product,
    )

    return {
        "dim_date": dim_date,
        "dim_channel": dim_channel,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "fact_order_items": fact_order_items,
        "fact_returns": fact_returns,
        "fact_inventory_daily": fact_inventory_daily,
    }


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------


def write_demo_outputs(
    datasets: dict[str, DataFrame],
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> None:
    """Write each Gold DataFrame to output_dir/<name>/ as a single Parquet file.

    Existing data is overwritten. The directory is created if it does not exist.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in datasets.items():
        dest = str(output_dir / name)
        df.coalesce(1).write.mode("overwrite").parquet(dest)


# ---------------------------------------------------------------------------
# Top-level convenience entry point
# ---------------------------------------------------------------------------


def run_demo(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, DataFrame]:
    """Build and write all Gold demo outputs.

    Returns the datasets dict so callers can inspect or query results.
    """
    spark = build_local_spark()
    datasets = build_gold_datasets(spark)
    write_demo_outputs(datasets, output_dir)
    return datasets
