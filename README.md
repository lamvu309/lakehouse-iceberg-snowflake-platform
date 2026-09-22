# 🏔️ Modern Data Lakehouse — Apache Iceberg + Snowflake + OpenMetadata

[![Python](https://img.shields.io/badge/Python-3.11+-blue)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)](https://spark.apache.org)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5-green)](https://iceberg.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-External%20Volumes-29B5E8)](https://snowflake.com)

> **Architecture**: Complete storage-compute separation using Amazon S3 as the data lake (Apache Iceberg format), Snowflake as the query engine via External Volumes, and OpenMetadata for enterprise-grade data governance.

---

## Architecture

```
Binance API (Crypto Klines)
         │
         ▼
 PySpark Ingestion Job
 ├── Data Quality Checks
 └── Write → Apache Iceberg
         │
         ▼
  Amazon S3 (Data Lake)
  └── iceberg/binance/klines/
      partitioned by days(open_time) + symbol
         │
  ┌──────┴──────┐
  ▼             ▼
Snowflake   OpenMetadata
External    (Data Catalog
Volumes     & Governance)
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Ingestion | PySpark 3.5, httpx, tenacity |
| Storage | Amazon S3 + Apache Iceberg 1.5 |
| Catalog | Hadoop Catalog (Phase 1) → AWS Glue (Phase 3) |
| Compute | Snowflake External Volumes |
| Governance | OpenMetadata on EC2 |
| IaC | Terraform |

---

## Quick Start

### Prerequisites
- Python 3.11+
- Java 11 (for PySpark)
- AWS credentials configured
- Terraform (for infrastructure)

### 1. Setup environment

```bash
# Clone and install
git clone <repo-url>
cd lakehouse-iceberg-snowflake-platform
pip install -e ".[dev]"

# Copy and fill in your config
cp .env.example .env
```

### 2. Download Spark JARs

```bash
make download-jars
```

### 3. (Optional) Deploy AWS Infrastructure

```bash
make tf-init
make tf-plan
make tf-apply
# Copy ICEBERG_WAREHOUSE output to .env
```

### 4. Run ingestion (dry-run first)

```bash
# Test without writing (just fetch & validate)
python -m ingestion.jobs.ingest_binance_klines \
  --symbol BTCUSDT \
  --interval 1m \
  --start-date 2024-01-01 \
  --end-date 2024-01-01 \
  --dry-run

# Local dev mode (no S3, writes to ./local_warehouse)
python -m ingestion.jobs.ingest_binance_klines \
  --symbol BTCUSDT \
  --interval 1m \
  --start-date 2024-01-01 \
  --end-date 2024-01-01 \
  --mode upsert \
  --local

# Production (writes to S3)
python -m ingestion.jobs.ingest_binance_klines \
  --symbol BTCUSDT \
  --interval 1m \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --mode upsert
```

### 5. Run tests

```bash
make test-unit          # Fast (no JARs needed)
make test-integration   # Requires JARs (make download-jars first)
```

---

## Project Structure

```
├── ingestion/
│   ├── binance/        # Binance API client + Pydantic schemas
│   ├── spark/          # SparkSession, data quality, Iceberg write
│   └── jobs/           # CLI entrypoints
├── infrastructure/
│   ├── terraform/      # S3, IAM, EC2 provisioning
│   └── scripts/        # EC2 bootstrap
├── snowflake/          # External Volume + Iceberg table SQL (Phase 2)
├── governance/         # OpenMetadata Docker Compose (Phase 3)
└── tests/
    ├── unit/           # Pure Python, no Spark JARs needed
    └── integration/    # Local Iceberg filesystem tests
```

---

## Roadmap

- [x] **Phase 1** — Binance Klines → S3 Iceberg ingestion
- [ ] **Phase 2** — Snowflake External Volumes + Partition Pruning demo
- [ ] **Phase 3** — OpenMetadata on EC2 (Docker Compose)
- [ ] **Phase 4** — CI/CD (GitHub Actions), data dictionary, runbooks

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Iceberg Catalog | Hadoop → Glue | Simple to start; Glue needed for Snowflake integration |
| Partition Strategy | `days(open_time) + symbol` | Enables partition pruning on time-range + symbol queries |
| Write Mode | MERGE INTO (upsert) | Idempotent re-runs; handles Binance late data corrections |
| API Client | httpx + tenacity | No heavy Binance SDK; full control over retry/rate-limit |