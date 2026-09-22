#!/bin/bash
# Bootstrap script for EC2 instance running PySpark ingestion jobs
# Tested on Amazon Linux 2023

set -euo pipefail
exec > >(tee /var/log/bootstrap.log) 2>&1

echo "=== [1/5] System update ==="
dnf update -y
dnf install -y git wget curl unzip

echo "=== [2/5] Install Java 11 (required for Spark) ==="
dnf install -y java-11-amazon-corretto-headless
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment

echo "=== [3/5] Install Python 3.11 ==="
dnf install -y python3.11 python3.11-pip
ln -sf /usr/bin/python3.11 /usr/local/bin/python3
ln -sf /usr/bin/pip3.11 /usr/local/bin/pip3

echo "=== [4/5] Install PySpark + project dependencies ==="
cd /opt
git clone https://github.com/YOUR_ORG/lakehouse-iceberg-snowflake-platform.git lakehouse
cd lakehouse
pip3 install -e ".[dev]"

echo "=== [5/5] Download Iceberg + AWS JARs ==="
make download-jars

echo ""
echo "✅ Bootstrap complete. Verify with:"
echo "   python3 -c 'import pyspark; print(pyspark.__version__)'"
echo "   python3 -m ingestion.jobs.ingest_binance_klines --help"
