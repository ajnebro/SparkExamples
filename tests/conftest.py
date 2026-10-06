"""Shared pytest fixtures."""

from collections.abc import Iterator

import pytest
from pyspark import SparkConf, SparkContext


@pytest.fixture(scope="session")
def spark_context() -> Iterator[SparkContext]:
    """Provide a local SparkContext shared by the whole test session."""
    conf = SparkConf().setAppName("tests").setMaster("local[2]")
    context = SparkContext(conf=conf)
    context.setLogLevel("ERROR")
    yield context
    context.stop()
