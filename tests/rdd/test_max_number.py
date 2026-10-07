"""Tests for rdd.max_number."""

import pytest
from pyspark import SparkContext

from rdd.max_number import max_number


class TestMaxNumber:
    @pytest.mark.parametrize(
        ("numbers", "expected"),
        [([1, 2, 3, 4, 5, 6, 7, 8], 8), ([5], 5), ([-3, -1, -2], -1), ([4, 4, 4], 4)],
    )
    def test_should_return_largest_number(
        self, spark_context: SparkContext, numbers: list[int], expected: int
    ) -> None:
        """The largest number is returned wherever it is placed."""
        result = max_number(spark_context, numbers)

        assert result == expected

    def test_should_raise_error_on_empty_list(self, spark_context: SparkContext) -> None:
        """Given an empty list, a ValueError is raised."""
        with pytest.raises(ValueError, match="empty"):
            max_number(spark_context, [])
