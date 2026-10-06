"""Tests for spark_apps.rdd.add_numbers."""

from collections.abc import Iterator

import pytest
from pyspark import SparkConf, SparkContext

from spark_apps.rdd.add_numbers import add_numbers


@pytest.fixture(scope="module")
def spark_context() -> Iterator[SparkContext]:
    conf = SparkConf().setAppName("test-add-numbers").setMaster("local[2]")
    context = SparkContext(conf=conf)
    context.setLogLevel("ERROR")
    yield context
    context.stop()


class TestAddNumbers:
    @pytest.mark.parametrize(
        ("numbers", "expected"),
        [
            ([1, 2, 3, 4, 5, 6, 7, 8], 36),
            ([5], 5),
            ([-1, 1, -2, 2], 0),
        ],
    )
    def test_should_return_sum_of_numbers(
        self, spark_context: SparkContext, numbers: list[int], expected: int
    ) -> None:
        """Given a list of integers, when adding them, then the sum is returned."""
        assert add_numbers(spark_context, numbers) == expected

    def test_should_raise_error_on_empty_list(self, spark_context: SparkContext) -> None:
        """Given an empty list, when adding, then ValueError is raised by reduce."""
        with pytest.raises(ValueError):
            add_numbers(spark_context, [])
