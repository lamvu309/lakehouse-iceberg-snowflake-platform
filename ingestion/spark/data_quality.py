"""Data quality checks for Binance Kline data before writing to Iceberg."""

from __future__ import annotations

from dataclasses import dataclass, field

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class QualityReport:
    """Result of a data quality run."""

    total_rows: int = 0
    failed_rows: int = 0
    checks: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    @property
    def pass_rate(self) -> float:
        if self.total_rows == 0:
            return 0.0
        return (self.total_rows - self.failed_rows) / self.total_rows


def run_quality_checks(df: DataFrame, fail_fast: bool = True) -> QualityReport:
    """Run all data quality checks on Kline DataFrame.

    Args:
        df: Spark DataFrame with Kline schema.
        fail_fast: If True, raise ValueError on first failed check.

    Returns:
        QualityReport with pass/fail summary.

    Raises:
        ValueError: If fail_fast=True and any check fails.
    """
    report = QualityReport(total_rows=df.count())
    logger.info("Running data quality checks on {} rows...", report.total_rows)

    checks = [
        _check_no_nulls,
        _check_ohlc_consistency,
        _check_timestamps_ordered,
        _check_positive_volume,
        _check_no_duplicates,
    ]

    for check_fn in checks:
        check_name, error_msg = check_fn(df)
        report.checks.append(check_name)
        if error_msg:
            report.errors.append(error_msg)
            logger.error("❌ FAILED: {} — {}", check_name, error_msg)
            if fail_fast:
                raise ValueError(f"Data quality check failed: {error_msg}")
        else:
            logger.info("✅ PASSED: {}", check_name)

    if report.passed:
        logger.success("All {} quality checks passed.", len(checks))
    else:
        logger.warning(
            "{}/{} checks failed.", len(report.errors), len(checks)
        )
    return report


# ── Individual checks ────────────────────────────────────────────────────────

def _check_no_nulls(df: DataFrame) -> tuple[str, str | None]:
    """Critical columns must not be null."""
    name = "no_nulls_in_critical_columns"
    critical_cols = ["open_time", "close_time", "symbol", "open", "high", "low", "close"]

    null_counts = df.select(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in critical_cols]
    ).collect()[0]

    violations = {c: null_counts[c] for c in critical_cols if null_counts[c] > 0}
    if violations:
        return name, f"Null values found: {violations}"
    return name, None


def _check_ohlc_consistency(df: DataFrame) -> tuple[str, str | None]:
    """High must be >= Open, Close, Low. Low must be <= Open, Close."""
    name = "ohlc_consistency"
    bad_count = df.filter(
        (F.col("high") < F.col("open"))
        | (F.col("high") < F.col("close"))
        | (F.col("high") < F.col("low"))
        | (F.col("low") > F.col("open"))
        | (F.col("low") > F.col("close"))
    ).count()

    if bad_count > 0:
        return name, f"{bad_count} rows violate OHLC consistency (high >= open/close/low)"
    return name, None


def _check_timestamps_ordered(df: DataFrame) -> tuple[str, str | None]:
    """open_time must be strictly before close_time."""
    name = "timestamps_ordered"
    bad_count = df.filter(F.col("open_time") >= F.col("close_time")).count()
    if bad_count > 0:
        return name, f"{bad_count} rows have open_time >= close_time"
    return name, None


def _check_positive_volume(df: DataFrame) -> tuple[str, str | None]:
    """Volume must be >= 0 (0 is valid for illiquid periods)."""
    name = "non_negative_volume"
    bad_count = df.filter(F.col("volume") < 0).count()
    if bad_count > 0:
        return name, f"{bad_count} rows have negative volume"
    return name, None


def _check_no_duplicates(df: DataFrame) -> tuple[str, str | None]:
    """No duplicate (symbol, interval, open_time) combinations."""
    name = "no_duplicate_keys"
    total = df.count()
    distinct = df.dropDuplicates(["symbol", "interval", "open_time"]).count()
    duplicates = total - distinct
    if duplicates > 0:
        return name, f"{duplicates} duplicate rows on (symbol, interval, open_time)"
    return name, None
