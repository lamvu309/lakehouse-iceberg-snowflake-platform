"""Pydantic data models for Binance Kline/Candlestick data."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class KlineRecord(BaseModel):
    """Single Kline (candlestick) record from Binance API.

    Raw API response format (list):
    [open_time, open, high, low, close, volume, close_time,
     quote_asset_volume, number_of_trades,
     taker_buy_base_asset_volume, taker_buy_quote_asset_volume, ignore]
    """

    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    close_time: datetime
    quote_asset_volume: Decimal
    number_of_trades: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    symbol: str
    interval: str

    @field_validator("open_time", "close_time", mode="before")
    @classmethod
    def parse_ms_timestamp(cls, v: int | datetime) -> datetime:
        """Convert Binance millisecond timestamp to UTC datetime."""
        if isinstance(v, int):
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        return v

    @classmethod
    def from_api_row(cls, row: list, symbol: str, interval: str) -> "KlineRecord":
        """Parse a single row from Binance klines API response.

        Binance returns a list of 12 elements per kline.
        """
        return cls(
            open_time=int(row[0]),
            open=Decimal(str(row[1])),
            high=Decimal(str(row[2])),
            low=Decimal(str(row[3])),
            close=Decimal(str(row[4])),
            volume=Decimal(str(row[5])),
            close_time=int(row[6]),
            quote_asset_volume=Decimal(str(row[7])),
            number_of_trades=int(row[8]),
            taker_buy_base_volume=Decimal(str(row[9])),
            taker_buy_quote_volume=Decimal(str(row[10])),
            symbol=symbol,
            interval=interval,
        )

    def to_dict(self) -> dict:
        """Serialize to plain dict with string decimals (Spark-safe)."""
        return {
            "open_time": self.open_time,
            "close_time": self.close_time,
            "symbol": self.symbol,
            "interval": self.interval,
            "open": float(self.open),
            "high": float(self.high),
            "low": float(self.low),
            "close": float(self.close),
            "volume": float(self.volume),
            "quote_asset_volume": float(self.quote_asset_volume),
            "number_of_trades": self.number_of_trades,
            "taker_buy_base_volume": float(self.taker_buy_base_volume),
            "taker_buy_quote_volume": float(self.taker_buy_quote_volume),
        }


class KlineInterval:
    """Valid Binance kline intervals."""

    SECONDS = ["1s"]
    MINUTES = ["1m", "3m", "5m", "15m", "30m"]
    HOURS = ["1h", "2h", "4h", "6h", "8h", "12h"]
    DAYS = ["1d", "3d"]
    WEEKS = ["1w"]
    MONTHS = ["1M"]

    ALL = SECONDS + MINUTES + HOURS + DAYS + WEEKS + MONTHS


class IngestionConfig(BaseModel):
    """Runtime configuration for a single ingestion run."""

    symbol: str = Field(..., description="Trading pair, e.g. BTCUSDT")
    interval: str = Field(..., description="Kline interval, e.g. 1m")
    start_ms: int = Field(..., description="Start time in milliseconds (UTC)")
    end_ms: int = Field(..., description="End time in milliseconds (UTC)")
    batch_size: int = Field(default=1000, ge=1, le=1000)

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: str) -> str:
        if v not in KlineInterval.ALL:
            raise ValueError(f"Invalid interval '{v}'. Valid: {KlineInterval.ALL}")
        return v

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, v: str) -> str:
        return v.upper()
