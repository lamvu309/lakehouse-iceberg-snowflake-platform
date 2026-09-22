"""Entrypoint: Ingest Binance Kline data → Apache Iceberg on S3.

Usage:
    python -m ingestion.jobs.ingest_binance_klines \\
        --symbol BTCUSDT \\
        --interval 1m \\
        --start-date 2024-01-01 \\
        --end-date 2024-01-02 \\
        --mode upsert

    # Local dev (no S3, no JARs needed for quick test):
    python -m ingestion.jobs.ingest_binance_klines \\
        --symbol BTCUSDT --interval 1m \\
        --start-date 2024-01-01 --end-date 2024-01-01 \\
        --mode append --local
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import typer
from dotenv import load_dotenv
from loguru import logger
from rich.console import Console
from rich.table import Table

from ingestion.binance.fetch_klines import BinanceKlineClient
from ingestion.binance.schema import IngestionConfig
from ingestion.spark.data_quality import run_quality_checks
from ingestion.spark.spark_session import create_spark_session
from ingestion.spark.write_iceberg import WriteMode, records_to_dataframe, write_klines

load_dotenv()

app = typer.Typer(help="Ingest Binance Kline data into Apache Iceberg on S3.")
console = Console()


@app.command()
def main(
    symbol: str = typer.Option("BTCUSDT", help="Trading pair symbol"),
    interval: str = typer.Option("1m", help="Kline interval (1m, 5m, 1h, ...)"),
    start_date: str = typer.Option(..., help="Start date (YYYY-MM-DD, UTC)"),
    end_date: str = typer.Option(..., help="End date (YYYY-MM-DD, UTC)"),
    mode: WriteMode = typer.Option(WriteMode.UPSERT, help="Write mode: append or upsert"),
    catalog: str = typer.Option("lakehouse", help="Iceberg catalog name"),
    local: bool = typer.Option(False, help="Use local filesystem warehouse (dev/test)"),
    dry_run: bool = typer.Option(False, help="Fetch data but skip writing to Iceberg"),
    batch_size: int = typer.Option(1000, min=1, max=1000, help="API batch size"),
) -> None:
    """Fetch Binance Klines and write to Apache Iceberg."""

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    config = IngestionConfig(
        symbol=symbol,
        interval=interval,
        start_ms=int(start_dt.timestamp() * 1000),
        end_ms=int(end_dt.timestamp() * 1000),
        batch_size=batch_size,
    )

    console.rule("[bold blue]Lakehouse Iceberg Ingestion")
    console.print(f"  Symbol   : [cyan]{config.symbol}[/cyan]")
    console.print(f"  Interval : [cyan]{config.interval}[/cyan]")
    console.print(f"  Range    : [cyan]{start_date}[/cyan] -> [cyan]{end_date}[/cyan]")
    console.print(f"  Mode     : [yellow]{mode.value}[/yellow]")
    console.print(f"  Dry run  : {'[red]YES[/red]' if dry_run else '[green]NO[/green]'}")
    console.rule()

    # ── Step 1: Fetch from Binance ────────────────────────────────────────
    logger.info("Step 1/3 - Fetching from Binance API...")
    all_records = asyncio.run(_fetch_all_records(config))

    if not all_records:
        console.print("[red]No data returned from Binance. Check date range.[/red]")
        raise typer.Exit(code=1)

    console.print(f"✅ Fetched [bold]{len(all_records)}[/bold] candles")

    if dry_run:
        console.print("[yellow]Dry run mode - skipping Spark write.[/yellow]")
        _print_sample(all_records[:5])
        return

    # ── Step 2: Spark + Data Quality ─────────────────────────────────────
    logger.info("Step 2/3 - Running Spark + Data Quality checks...")
    spark = create_spark_session(
        app_name=f"ingest-{symbol}-{interval}",
        catalog_name=catalog,
        use_local=local,
    )

    raw_dicts = [r.to_dict() for r in all_records]
    df = records_to_dataframe(spark, raw_dicts)

    report = run_quality_checks(df, fail_fast=True)
    console.print(f"✅ Quality checks passed ({report.total_rows} rows, {len(report.checks)} checks)")

    # ── Step 3: Write to Iceberg ──────────────────────────────────────────
    logger.info("Step 3/3 - Writing to Iceberg ({})...", mode.value)
    rows_written = write_klines(spark, df, mode=mode, catalog=catalog)

    spark.stop()

    console.rule("[bold green]Ingestion Complete")
    console.print(f"  Rows written : [bold green]{rows_written}[/bold green]")
    console.print(f"  Symbol       : {symbol}")
    console.print(f"  Period       : {start_date} -> {end_date}")
    console.rule()


async def _fetch_all_records(config: IngestionConfig) -> list:
    """Async wrapper for Binance client pagination."""
    all_records = []
    async with BinanceKlineClient() as client:
        async for batch in client.iter_klines(config):
            all_records.extend(batch)
    return all_records


def _print_sample(records: list) -> None:
    """Print first N records as a Rich table."""
    table = Table(title="Sample Records")
    fields = ["symbol", "interval", "open_time", "open", "high", "low", "close", "volume"]
    for f in fields:
        table.add_column(f, style="cyan")
    for r in records:
        d = r.to_dict()
        table.add_row(*[str(d[f]) for f in fields])
    console.print(table)


if __name__ == "__main__":
    app()
