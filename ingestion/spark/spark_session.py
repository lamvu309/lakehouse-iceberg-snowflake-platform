"""SparkSession factory configured for Apache Iceberg with S3 (Hadoop Catalog)."""

from __future__ import annotations

import os
from pathlib import Path

from loguru import logger
from pyspark.sql import SparkSession

# JAR files required for Iceberg + S3
_JARS_DIR = Path(__file__).parents[2] / "jars"
_REQUIRED_JARS = [
    "iceberg-spark-runtime.jar",
    "hadoop-aws.jar",
    "aws-java-sdk-bundle.jar",
]


def _build_jars_path() -> str:
    """Return comma-separated absolute paths to required JARs."""
    missing = [j for j in _REQUIRED_JARS if not (_JARS_DIR / j).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing JARs in {_JARS_DIR}: {missing}\n"
            "Run: make download-jars"
        )
    return ",".join(str(_JARS_DIR / j) for j in _REQUIRED_JARS)


def create_spark_session(
    app_name: str = "lakehouse-iceberg-ingestion",
    warehouse: str | None = None,
    catalog_name: str = "lakehouse",
    use_local: bool = False,
) -> SparkSession:
    """Create a SparkSession configured for Iceberg with Hadoop Catalog.

    Args:
        app_name: Spark application name.
        warehouse: Iceberg warehouse URI. Defaults to env var ICEBERG_WAREHOUSE.
            Use 'file:///path/to/local_warehouse' for local dev/testing.
        catalog_name: Iceberg catalog name (default: 'lakehouse').
        use_local: If True, use local filesystem warehouse (for testing).

    Returns:
        Configured SparkSession.
    """
    if use_local:
        local_dir = Path.cwd() / "local_warehouse"
        local_dir.mkdir(exist_ok=True)
        warehouse = f"file:///{local_dir.as_posix()}"
        logger.info("Using LOCAL Iceberg warehouse: {}", warehouse)
    else:
        warehouse = warehouse or os.environ.get("ICEBERG_WAREHOUSE")
        if not warehouse:
            raise ValueError(
                "ICEBERG_WAREHOUSE not set. "
                "Pass warehouse= or set the env var."
            )
        logger.info("Using S3 Iceberg warehouse: {}", warehouse)

    jars = _build_jars_path()
    logger.debug("Loading JARs: {}", jars)

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars", jars)
        # ── Iceberg extensions & catalog ──────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        # ── S3A filesystem config ──────────────────────────────────────
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            _resolve_credentials_provider(),
        )
        # ── Performance & reliability ──────────────────────────────────
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.sql.shuffle.partitions", "8")  # tuned down for dev
        .config("spark.default.parallelism", "8")
    )

    # Inject AWS credentials from env if using basic auth (not IAM role)
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if aws_key and aws_secret:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", aws_key).config(
            "spark.hadoop.fs.s3a.secret.key", aws_secret
        )
        region = os.environ.get("AWS_REGION", "ap-southeast-1")
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", region)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    logger.success("SparkSession created. Catalog: {}", catalog_name)
    return spark


def _resolve_credentials_provider() -> str:
    """Choose AWS credentials provider based on environment."""
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        return "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    # Default: EC2 instance profile (IAM Role)
    return "com.amazonaws.auth.InstanceProfileCredentialsProvider"
