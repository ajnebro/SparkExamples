.PHONY: install test lint format

install:
	pip install -e ".[dev]"

test:
	pytest tests/ -x

lint:
	ruff check src tests

format:
	ruff format src tests
