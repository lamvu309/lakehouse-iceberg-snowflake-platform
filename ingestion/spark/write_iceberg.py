"""Write Kline data to Apache Iceberg table on S3 (Hadoop Catalog)."""

from __future__ import annotations

from enum import Enum

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Iceberg table full name: <catalog>.<database>.<table>
_DATABASE = "binance"
_TABLE = "klines"


class WriteMode(str, Enum):
    APPEND = "append"      # Initial load / append-only
    UPSERT = "upsert"      # Idempotent re-runs (MERGE INTO)


# ── Iceberg Schema ───────────────────────────────────────────────────────────

KLINE_SCHEMA = StructType([
    StructField("open_time", TimestampType(), nullable=False),
    StructField("close_time", TimestampType(), nullable=False),
    StructField("symbol", StringType(), nullable=False),
    StructField("interval", StringType(), nullable=False),
    StructField("open", DoubleType(), nullable=False),
    StructField("high", DoubleType(), nullable=False),
    StructField("low", DoubleType(), nullable=False),
    StructField("close", DoubleType(), nullable=False),
    StructField("volume", DoubleType(), nullable=False),
    StructField("quote_asset_volume", DoubleType(), nullable=True),
    StructField("number_of_trades", IntegerType(), nullable=True),
    StructField("taker_buy_base_volume", DoubleType(), nullable=True),
    StructField("taker_buy_quote_volume", DoubleType(), nullable=True),
])


# ── Table Management ─────────────────────────────────────────────────────────

def ensure_table_exists(spark: SparkSession, catalog: str = "lakehouse") -> None:
    """Create Iceberg table if it doesn't exist.

    Partition strategy: days(open_time) → year=.../month=.../day=...
    This enables partition pruning for time-range queries.
    """
    full_table = f"{catalog}.{_DATABASE}.{_TABLE}"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{_DATABASE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            open_time     TIMESTAMP NOT NULL,
            close_time    TIMESTAMP NOT NULL,
            symbol        STRING    NOT NULL,
            interval      STRING    NOT NULL,
            open          DOUBLE    NOT NULL,
            high          DOUBLE    NOT NULL,
            low           DOUBLE    NOT NULL,
            close         DOUBLE    NOT NULL,
            volume        DOUBLE    NOT NULL,
            quote_asset_volume    DOUBLE,
            number_of_trades      INT,
            taker_buy_base_volume DOUBLE,
            taker_buy_quote_volume DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(open_time), symbol)
        TBLPROPERTIES (
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10',
            'write.parquet.compression-codec' = 'snappy',
            'history.expire.max-snapshot-age-ms' = '604800000'
        )
    """)
    logger.info("Iceberg table ready: {}", full_table)


# ── Write Operations ─────────────────────────────────────────────────────────

def write_klines(
    spark: SparkSession,
    df: DataFrame,
    mode: WriteMode = WriteMode.UPSERT,
    catalog: str = "lakehouse",
) -> int:
    """Write Kline DataFrame to Iceberg table.

    Args:
        spark: Active SparkSession.
        df: Validated Kline DataFrame.
        mode: APPEND for initial loads, UPSERT for idempotent re-runs.
        catalog: Iceberg catalog name.

    Returns:
        Number of rows written.
    """
    full_table = f"{catalog}.{_DATABASE}.{_TABLE}"
    row_count = df.count()
    logger.info("Writing {} rows to {} (mode={})", row_count, full_table, mode.value)

    ensure_table_exists(spark, catalog)

    if mode == WriteMode.APPEND:
        _write_append(df, full_table)
    else:
        _write_upsert(spark, df, full_table)

    logger.success("Write complete: {} rows → {}", row_count, full_table)
    return row_count


def _write_append(df: DataFrame, full_table: str) -> None:
    """Simple append — fastest, but duplicates possible on re-run."""
    df.writeTo(full_table).append()


def _write_upsert(spark: SparkSession, df: DataFrame, full_table: str) -> None:
    """Idempotent upsert via Iceberg MERGE INTO.

    Match key: (symbol, interval, open_time) — uniquely identifies a candle.
    On match: update all fields (handles late corrections from Binance).
    On no match: insert new row.
    """
    # Register new data as a temp view
    df.createOrReplaceTempView("_klines_staging")

    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING _klines_staging AS source
            ON  target.symbol    = source.symbol
            AND target.interval  = source.interval
            AND target.open_time = source.open_time
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)


# ── DataFrame Builder ────────────────────────────────────────────────────────

def records_to_dataframe(
    spark: SparkSession, records: list[dict]
) -> DataFrame:
    """Convert list of KlineRecord.to_dict() results to a typed Spark DataFrame."""
    df = spark.createDataFrame(records, schema=KLINE_SCHEMA)

    # Add derived partition helper columns (Iceberg handles partitioning,
    # but these help with debugging / manual S3 browsing)
    df = df.withColumn("year", F.year("open_time")) \
           .withColumn("month", F.month("open_time")) \
           .withColumn("day", F.dayofmonth("open_time"))

    return df
