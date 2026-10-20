"""Shared pytest fixtures."""

from collections.abc import Iterator

import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_context() -> Iterator[SparkContext]:
    """Provide a local SparkContext shared by the whole test session."""
    # fmt: off
    conf = (
        SparkConf()
        .setAppName("tests")
        .setMaster("local[2]")
    )
    # fmt: on
    context = SparkContext(conf=conf)
    context.setLogLevel("ERROR")
    yield context
    context.stop()


@pytest.fixture(scope="session")
def spark_session(spark_context: SparkContext) -> SparkSession:
    """Provide a SparkSession built on top of the shared SparkContext."""
    return SparkSession(spark_context)
