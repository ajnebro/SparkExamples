.PHONY: install test test-integration test-all lint format format-check check check-all clean

# Requires an active virtual environment (see README.md)
install:
	pip install -e ".[dev]"

# Unit tests: they share one local Spark session and take about a minute
test:
	pytest tests/ -x -m "not integration"

# Integration tests: each example runs as a separate process (several minutes, so in parallel)
test-integration:
	pytest tests/ -x -m integration -n 4

test-all: test test-integration

lint:
	ruff check src tests

format:
	ruff format src tests

format-check:
	ruff format --check src tests

check: lint format-check test

check-all: lint format-check test-all

clean:
	rm -rf output spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
