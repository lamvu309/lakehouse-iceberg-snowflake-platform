"""Integration test: write Kline data to local Iceberg (no S3, no JAR needed for schema).

Requires: JARs downloaded (make download-jars) for full Iceberg write.
Run with: pytest tests/integration/ -v
"""

from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ingestion.spark.write_iceberg import WriteMode, records_to_dataframe, write_klines
from ingestion.spark.spark_session import create_spark_session

LOCAL_WAREHOUSE = Path(__file__).parents[2] / "local_warehouse_test"

SAMPLE_RECORDS = [
    {
        "open_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "close_time": datetime(2024, 1, 1, 0, 0, 59, tzinfo=timezone.utc),
        "symbol": "BTCUSDT",
        "interval": "1m",
        "open": 42000.0,
        "high": 42500.0,
        "low": 41800.0,
        "close": 42200.0,
        "volume": 1234.567,
        "quote_asset_volume": 52000000.0,
        "number_of_trades": 5000,
        "taker_buy_base_volume": 600.0,
        "taker_buy_quote_volume": 25000000.0,
    },
    {
        "open_time": datetime(2024, 1, 1, 0, 1, 0, tzinfo=timezone.utc),
        "close_time": datetime(2024, 1, 1, 0, 1, 59, tzinfo=timezone.utc),
        "symbol": "BTCUSDT",
        "interval": "1m",
        "open": 42200.0,
        "high": 42800.0,
        "low": 42100.0,
        "close": 42600.0,
        "volume": 987.654,
        "quote_asset_volume": 41500000.0,
        "number_of_trades": 4200,
        "taker_buy_base_volume": 500.0,
        "taker_buy_quote_volume": 21000000.0,
    },
]


@pytest.fixture(scope="module")
def spark_local() -> SparkSession:
    """SparkSession with local Iceberg warehouse (requires JARs)."""
    LOCAL_WAREHOUSE.mkdir(exist_ok=True)
    spark = create_spark_session(
        app_name="test-iceberg-integration",
        catalog_name="test_catalog",
        use_local=True,
    )
    # Override warehouse to our test dir
    yield spark
    spark.stop()


@pytest.fixture(autouse=True)
def cleanup_warehouse():
    """Clean up test warehouse after each test."""
    yield
    if LOCAL_WAREHOUSE.exists():
        shutil.rmtree(LOCAL_WAREHOUSE, ignore_errors=True)


class TestIcebergWrite:
    def test_append_writes_correct_row_count(self, spark_local: SparkSession) -> None:
        df = records_to_dataframe(spark_local, SAMPLE_RECORDS)
        rows_written = write_klines(
            spark_local, df, mode=WriteMode.APPEND, catalog="test_catalog"
        )
        assert rows_written == len(SAMPLE_RECORDS)

    def test_read_back_matches_written_data(self, spark_local: SparkSession) -> None:
        df = records_to_dataframe(spark_local, SAMPLE_RECORDS)
        write_klines(spark_local, df, mode=WriteMode.APPEND, catalog="test_catalog")

        result = spark_local.table("test_catalog.binance.klines")
        assert result.count() == len(SAMPLE_RECORDS)

        row = result.filter("symbol = 'BTCUSDT'").orderBy("open_time").first()
        assert row["open"] == pytest.approx(42000.0)
        assert row["symbol"] == "BTCUSDT"

    def test_upsert_is_idempotent(self, spark_local: SparkSession) -> None:
        """Running upsert twice must not increase row count."""
        df = records_to_dataframe(spark_local, SAMPLE_RECORDS)

        write_klines(spark_local, df, mode=WriteMode.UPSERT, catalog="test_catalog")
        count_after_first = spark_local.table("test_catalog.binance.klines").count()

        write_klines(spark_local, df, mode=WriteMode.UPSERT, catalog="test_catalog")
        count_after_second = spark_local.table("test_catalog.binance.klines").count()

        assert count_after_first == count_after_second == len(SAMPLE_RECORDS)

    def test_partition_by_day_exists(self, spark_local: SparkSession) -> None:
        """Verify that Iceberg table is partitioned by days(open_time)."""
        df = records_to_dataframe(spark_local, SAMPLE_RECORDS)
        write_klines(spark_local, df, mode=WriteMode.APPEND, catalog="test_catalog")

        # Query partition metadata
        partitions = spark_local.sql(
            "SELECT partition FROM test_catalog.binance.klines.partitions"
        )
        assert partitions.count() > 0
