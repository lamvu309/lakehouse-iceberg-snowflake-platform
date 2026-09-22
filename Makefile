.PHONY: install dev-install lint format typecheck test test-unit test-integration ingest clean download-jars

# ─── Setup ──────────────────────────────────────────────────────────────
install:
	pip install -e .

dev-install:
	pip install -e ".[dev]"

# ─── Code Quality ───────────────────────────────────────────────────────
lint:
	ruff check ingestion/ tests/

format:
	ruff format ingestion/ tests/

typecheck:
	mypy ingestion/

# ─── Tests ──────────────────────────────────────────────────────────────
test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v --timeout=120

# ─── Ingestion ──────────────────────────────────────────────────────────
ingest:
	python -m ingestion.jobs.ingest_binance_klines \
		--symbol $(SYMBOL) \
		--interval $(INTERVAL) \
		--start-date $(START_DATE) \
		--end-date $(END_DATE)

# Example: make ingest SYMBOL=BTCUSDT INTERVAL=1m START_DATE=2024-01-01 END_DATE=2024-01-02

# ─── Spark JARs ─────────────────────────────────────────────────────────
JARS_DIR := jars
ICEBERG_VERSION := 1.5.2
HADOOP_VERSION := 3.3.4
AWS_SDK_VERSION := 1.12.262

download-jars:
	@mkdir -p $(JARS_DIR)
	@echo "Downloading Iceberg Spark runtime..."
	curl -L -o $(JARS_DIR)/iceberg-spark-runtime.jar \
		"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/$(ICEBERG_VERSION)/iceberg-spark-runtime-3.5_2.12-$(ICEBERG_VERSION).jar"
	@echo "Downloading hadoop-aws..."
	curl -L -o $(JARS_DIR)/hadoop-aws.jar \
		"https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$(HADOOP_VERSION)/hadoop-aws-$(HADOOP_VERSION).jar"
	@echo "Downloading aws-java-sdk-bundle..."
	curl -L -o $(JARS_DIR)/aws-java-sdk-bundle.jar \
		"https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$(AWS_SDK_VERSION)/aws-java-sdk-bundle-$(AWS_SDK_VERSION).jar"
	@echo "✅ All JARs downloaded to $(JARS_DIR)/"

# Windows alternative (PowerShell) — use when make is not available:
#   powershell -File scripts/download_jars.ps1

# ─── Terraform ──────────────────────────────────────────────────────────
tf-init:
	cd infrastructure/terraform && terraform init

tf-plan:
	cd infrastructure/terraform && terraform plan

tf-apply:
	cd infrastructure/terraform && terraform apply -auto-approve

tf-destroy:
	cd infrastructure/terraform && terraform destroy -auto-approve

# ─── Cleanup ────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache .ruff_cache local_warehouse/
