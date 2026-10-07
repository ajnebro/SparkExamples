.PHONY: install test test-fast lint format format-check check clean

# Requires an active virtual environment (see README.md)
install:
	pip install -e ".[dev]"

test:
	pytest tests/ -x

test-fast:
	pytest tests/ -x -m "not smoke"

lint:
	ruff check src tests

format:
	ruff format src tests

format-check:
	ruff format --check src tests

check: lint format-check test

clean:
	rm -rf output spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
