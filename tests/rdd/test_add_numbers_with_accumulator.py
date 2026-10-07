"""Tests for rdd.add_numbers_with_accumulator."""

import pytest
from pyspark import SparkContext

from rdd.add_numbers_with_accumulator import add_numbers_counting_operations


class TestAddNumbersCountingOperations:
    @pytest.mark.parametrize(
        ("numbers", "expected_total"),
        [([1, 2, 3, 4, 5, 6, 7, 8], 36), ([5], 5), ([-1, 1, -2, 2], 0)],
    )
    def test_should_return_sum_and_one_operation_per_merged_pair(
        self, spark_context: SparkContext, numbers: list[int], expected_total: int
    ) -> None:
        """Adding n numbers takes n - 1 additions, whatever the partitioning."""
        total, operations = add_numbers_counting_operations(spark_context, numbers)

        assert total == expected_total
        assert operations == len(numbers) - 1

    def test_should_raise_error_on_empty_list(self, spark_context: SparkContext) -> None:
        """Given an empty list, a ValueError is raised."""
        with pytest.raises(ValueError, match="empty"):
            add_numbers_counting_operations(spark_context, [])
