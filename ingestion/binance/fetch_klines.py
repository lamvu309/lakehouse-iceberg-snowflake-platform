"""Binance REST API client for Kline/Candlestick data.

Uses httpx (async-capable) + tenacity for retry with exponential backoff.
No Binance SDK dependency — direct HTTP calls to public endpoints.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.binance.schema import IngestionConfig, KlineRecord

# Binance REST API limits
_RATE_LIMIT_REQUESTS_PER_MINUTE = 1200
_MIN_DELAY_BETWEEN_REQUESTS_S = 60 / _RATE_LIMIT_REQUESTS_PER_MINUTE  # ~0.05s


class BinanceAPIError(Exception):
    """Raised when Binance API returns a non-2xx response."""

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        super().__init__(f"Binance API error {status_code}: {body}")


class BinanceKlineClient:
    """Async Binance REST client for Kline data.

    Usage:
        async with BinanceKlineClient() as client:
            async for batch in client.iter_klines(config):
                process(batch)
    """

    BASE_URL = "https://api.binance.com"
    KLINES_ENDPOINT = "/api/v3/klines"

    def __init__(
        self,
        base_url: str = BASE_URL,
        api_key: str | None = None,
        timeout_s: float = 30.0,
    ) -> None:
        self._base_url = base_url
        self._timeout = timeout_s
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["X-MBX-APIKEY"] = api_key
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers=headers,
            timeout=timeout_s,
        )

    async def __aenter__(self) -> "BinanceKlineClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self._client.aclose()

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def _fetch_batch(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[KlineRecord]:
        """Fetch a single batch of klines (up to 1000 candles)."""
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        logger.debug(
            "Fetching klines: symbol={} interval={} start={} end={} limit={}",
            symbol,
            interval,
            datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
            datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat(),
            limit,
        )

        response = await self._client.get(self.KLINES_ENDPOINT, params=params)

        if response.status_code == 429:
            # Rate limited — tenacity will retry after backoff
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning("Rate limited. Waiting {}s...", retry_after)
            await asyncio.sleep(retry_after)
            raise httpx.TimeoutException("Rate limited", request=response.request)

        if response.status_code != 200:
            raise BinanceAPIError(response.status_code, response.text)

        rows: list[list] = response.json()
        return [KlineRecord.from_api_row(row, symbol=symbol, interval=interval) for row in rows]

    async def iter_klines(
        self,
        config: IngestionConfig,
    ) -> AsyncIterator[list[KlineRecord]]:
        """Paginate through the full time range and yield batches of KlineRecords.

        Binance returns max 1000 candles per request. This method handles
        pagination automatically by advancing the start_ms pointer.

        Yields:
            list[KlineRecord] — one batch per API call (up to batch_size records)
        """
        current_start_ms = config.start_ms
        total_fetched = 0

        while current_start_ms < config.end_ms:
            batch = await self._fetch_batch(
                symbol=config.symbol,
                interval=config.interval,
                start_ms=current_start_ms,
                end_ms=config.end_ms,
                limit=config.batch_size,
            )

            if not batch:
                logger.info("No more data returned. Stopping pagination.")
                break

            total_fetched += len(batch)
            logger.info(
                "Fetched {} candles (total: {}). Last candle: {}",
                len(batch),
                total_fetched,
                batch[-1].open_time.isoformat(),
            )
            yield batch

            # Advance pointer: next batch starts AFTER the last close_time
            last_close_ms = int(batch[-1].close_time.timestamp() * 1000)
            next_start_ms = last_close_ms + 1

            if next_start_ms >= config.end_ms:
                break

            current_start_ms = next_start_ms

            # Polite delay to stay within rate limits
            await asyncio.sleep(_MIN_DELAY_BETWEEN_REQUESTS_S)

        logger.success(
            "Completed fetching {} total candles for {} {}",
            total_fetched,
            config.symbol,
            config.interval,
        )

    async def fetch_all(self, config: IngestionConfig) -> list[KlineRecord]:
        """Convenience method — fetch all klines into memory.

        Warning: Use iter_klines() for large date ranges to avoid OOM.
        """
        all_records: list[KlineRecord] = []
        async for batch in self.iter_klines(config):
            all_records.extend(batch)
        return all_records
