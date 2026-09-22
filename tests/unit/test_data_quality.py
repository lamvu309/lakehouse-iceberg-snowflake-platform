"""Unit tests for data quality checks (pure Python, no S3 needed)."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from ingestion.spark.data_quality import QualityReport, run_quality_checks
from ingestion.spark.write_iceberg import KLINE_SCHEMA

# Fixtures use a minimal local SparkSession (no JARs needed for quality checks)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("test-data-quality")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def _make_df(spark: SparkSession, rows: list[dict]):
    """Create a DataFrame from row dicts using the Kline schema."""
    from datetime import datetime, timezone
    from ingestion.spark.write_iceberg import KLINE_SCHEMA
    return spark.createDataFrame(rows, schema=KLINE_SCHEMA)


VALID_ROW = {
    "open_time": "2024-01-01T00:00:00+00:00",
    "close_time": "2024-01-01T00:00:59+00:00",
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
}


class TestDataQuality:
    def test_valid_data_passes_all_checks(self, spark: SparkSession) -> None:
        from datetime import datetime, timezone
        row = {
            **VALID_ROW,
            "open_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 1, 0, 0, 59, tzinfo=timezone.utc),
        }
        df = spark.createDataFrame([row], schema=KLINE_SCHEMA)
        report = run_quality_checks(df, fail_fast=False)

        assert report.passed
        assert report.total_rows == 1
        assert len(report.errors) == 0

    def test_ohlc_violation_detected(self, spark: SparkSession) -> None:
        from datetime import datetime, timezone
        # high < open (violation)
        row = {
            **VALID_ROW,
            "open_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 1, 0, 0, 59, tzinfo=timezone.utc),
            "high": 41000.0,   # lower than open=42000 → violation
        }
        df = spark.createDataFrame([row], schema=KLINE_SCHEMA)
        report = run_quality_checks(df, fail_fast=False)

        assert not report.passed
        assert any("OHLC" in e or "ohlc" in e.lower() for e in report.errors)

    def test_negative_volume_detected(self, spark: SparkSession) -> None:
        from datetime import datetime, timezone
        row = {
            **VALID_ROW,
            "open_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 1, 0, 0, 59, tzinfo=timezone.utc),
            "volume": -1.0,
        }
        df = spark.createDataFrame([row], schema=KLINE_SCHEMA)
        report = run_quality_checks(df, fail_fast=False)

        assert not report.passed
        assert any("volume" in e.lower() for e in report.errors)

    def test_duplicate_keys_detected(self, spark: SparkSession) -> None:
        from datetime import datetime, timezone
        row = {
            **VALID_ROW,
            "open_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 1, 0, 0, 59, tzinfo=timezone.utc),
        }
        # Same row twice = duplicate
        df = spark.createDataFrame([row, row], schema=KLINE_SCHEMA)
        report = run_quality_checks(df, fail_fast=False)

        assert not report.passed
        assert any("duplicate" in e.lower() for e in report.errors)

    def test_fail_fast_raises_on_first_error(self, spark: SparkSession) -> None:
        from datetime import datetime, timezone
        row = {
            **VALID_ROW,
            "open_time": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "close_time": datetime(2024, 1, 1, 0, 0, 59, tzinfo=timezone.utc),
            "volume": -999.0,
        }
        df = spark.createDataFrame([row], schema=KLINE_SCHEMA)

        with pytest.raises(ValueError, match="Data quality check failed"):
            run_quality_checks(df, fail_fast=True)
