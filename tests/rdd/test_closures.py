"""Tests for the closure examples in rdd."""

import pytest
from pyspark import SparkContext

from rdd.closure_driver_variable import multiply_by_factor
from rdd.closure_reproducible_random import generate_reproducible_numbers
from rdd.closure_shared_generator import generate_with_captured_generator

DATA = list(range(1, 9))


class TestMultiplyByFactor:
    @pytest.mark.parametrize(
        ("data", "factor", "expected"),
        [([1, 2, 3, 4], 10, [10, 20, 30, 40]), ([1, 2], 0, [0, 0]), ([], 5, [])],
    )
    def test_should_multiply_every_element(
        self, spark_context: SparkContext, data: list[int], factor: int, expected: list[int]
    ) -> None:
        """The factor captured from the driver is applied to every element."""
        result = multiply_by_factor(spark_context, data, factor)

        assert result == expected


class TestCapturedGenerator:
    def test_should_repeat_sequence_in_every_partition(self, spark_context: SparkContext) -> None:
        """Each partition gets a copy of the generator, so both halves are identical."""
        result = generate_with_captured_generator(spark_context, DATA, 42, num_slices=2)

        assert result[:4] == result[4:]


class TestReproducibleNumbers:
    @pytest.mark.parametrize("num_slices", [1, 2, 4])
    def test_should_not_depend_on_partitioning(
        self, spark_context: SparkContext, num_slices: int
    ) -> None:
        """The result is the same whatever the number of partitions."""
        expected = generate_reproducible_numbers(spark_context, DATA, 42, num_slices=1)

        result = generate_reproducible_numbers(spark_context, DATA, 42, num_slices=num_slices)

        assert result == expected

    def test_should_change_with_seed(self, spark_context: SparkContext) -> None:
        """Different seeds give different sequences."""
        first = generate_reproducible_numbers(spark_context, DATA, 1)
        second = generate_reproducible_numbers(spark_context, DATA, 2)

        assert first != second
