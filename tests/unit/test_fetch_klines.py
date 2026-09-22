"""Unit tests for Binance Kline schema and API client."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from pytest_httpx import HTTPXMock

from ingestion.binance.fetch_klines import BinanceAPIError, BinanceKlineClient
from ingestion.binance.schema import IngestionConfig, KlineInterval, KlineRecord

# ── Sample data matching Binance API format ───────────────────────────────────

SAMPLE_ROW = [
    1704067200000,   # open_time (2024-01-01 00:00:00 UTC)
    "42000.00",      # open
    "42500.50",      # high
    "41800.00",      # low
    "42200.00",      # close
    "1234.567",      # volume
    1704067259999,   # close_time
    "52000000.00",   # quote_asset_volume
    5000,            # number_of_trades
    "600.000",       # taker_buy_base_volume
    "25000000.00",   # taker_buy_quote_volume
    "0",             # ignore
]

SAMPLE_RESPONSE = [SAMPLE_ROW, SAMPLE_ROW]  # 2 identical candles


# ── KlineRecord tests ─────────────────────────────────────────────────────────

class TestKlineRecord:
    def test_from_api_row_parses_correctly(self) -> None:
        record = KlineRecord.from_api_row(SAMPLE_ROW, symbol="BTCUSDT", interval="1m")

        assert record.symbol == "BTCUSDT"
        assert record.interval == "1m"
        assert record.open == Decimal("42000.00")
        assert record.high == Decimal("42500.50")
        assert record.low == Decimal("41800.00")
        assert record.close == Decimal("42200.00")
        assert record.volume == Decimal("1234.567")
        assert record.number_of_trades == 5000

    def test_timestamps_converted_to_utc_datetime(self) -> None:
        record = KlineRecord.from_api_row(SAMPLE_ROW, symbol="BTCUSDT", interval="1m")

        expected_open_time = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert record.open_time == expected_open_time
        assert record.open_time.tzinfo == timezone.utc
        assert record.close_time.tzinfo == timezone.utc

    def test_to_dict_returns_floats_not_decimal(self) -> None:
        record = KlineRecord.from_api_row(SAMPLE_ROW, symbol="BTCUSDT", interval="1m")
        d = record.to_dict()

        assert isinstance(d["open"], float)
        assert isinstance(d["high"], float)
        assert isinstance(d["volume"], float)
        assert d["symbol"] == "BTCUSDT"

    def test_symbol_preserved_as_passed(self) -> None:
        record = KlineRecord.from_api_row(SAMPLE_ROW, symbol="ETHUSDT", interval="5m")
        assert record.symbol == "ETHUSDT"
        assert record.interval == "5m"


# ── IngestionConfig tests ─────────────────────────────────────────────────────

class TestIngestionConfig:
    def test_valid_config(self) -> None:
        config = IngestionConfig(
            symbol="btcusdt",  # lowercase → should be uppercased
            interval="1m",
            start_ms=1704067200000,
            end_ms=1704153600000,
        )
        assert config.symbol == "BTCUSDT"

    def test_invalid_interval_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid interval"):
            IngestionConfig(
                symbol="BTCUSDT",
                interval="2m",  # not valid
                start_ms=1704067200000,
                end_ms=1704153600000,
            )

    def test_all_valid_intervals_accepted(self) -> None:
        for interval in KlineInterval.ALL:
            config = IngestionConfig(
                symbol="BTCUSDT",
                interval=interval,
                start_ms=1704067200000,
                end_ms=1704153600000,
            )
            assert config.interval == interval


# ── BinanceKlineClient tests ──────────────────────────────────────────────────

class TestBinanceKlineClient:
    @pytest.mark.asyncio
    async def test_fetch_returns_kline_records(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url__contains="/api/v3/klines",
            json=SAMPLE_RESPONSE,
            status_code=200,
        )

        config = IngestionConfig(
            symbol="BTCUSDT",
            interval="1m",
            start_ms=1704067200000,
            end_ms=1704067260000,  # narrow range → single batch
        )

        async with BinanceKlineClient(base_url="https://api.binance.com") as client:
            batches = []
            async for batch in client.iter_klines(config):
                batches.append(batch)

        assert len(batches) >= 1
        assert all(isinstance(r, KlineRecord) for r in batches[0])

    @pytest.mark.asyncio
    async def test_api_error_raises_binance_api_error(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url__contains="/api/v3/klines",
            status_code=400,
            json={"code": -1121, "msg": "Invalid symbol."},
        )

        config = IngestionConfig(
            symbol="INVALID",
            interval="1m",
            start_ms=1704067200000,
            end_ms=1704067260000,
        )

        async with BinanceKlineClient(base_url="https://api.binance.com") as client:
            with pytest.raises(BinanceAPIError):
                async for _ in client.iter_klines(config):
                    pass

    @pytest.mark.asyncio
    async def test_empty_response_stops_pagination(self, httpx_mock: HTTPXMock) -> None:
        httpx_mock.add_response(
            url__contains="/api/v3/klines",
            json=[],  # empty → should stop
            status_code=200,
        )

        config = IngestionConfig(
            symbol="BTCUSDT",
            interval="1m",
            start_ms=1704067200000,
            end_ms=1704153600000,
        )

        async with BinanceKlineClient(base_url="https://api.binance.com") as client:
            batches = [b async for b in client.iter_klines(config)]

        assert batches == []
