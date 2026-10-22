"""Tests for ml.logistic_regression."""

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from ml.logistic_regression import accuracy, train
from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm


class TestTrain:
    def test_should_classify_the_iris_data_well(self, spark_session: SparkSession) -> None:
        """The model classifies most of the flowers it was trained with."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        assert accuracy(train(data), data) > 0.9

    def test_should_learn_one_row_of_coefficients_per_class(
        self, spark_session: SparkSession
    ) -> None:
        """With three classes and four features, the coefficient matrix is 3 x 4."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        matrix = train(data).coefficientMatrix

        assert (matrix.numRows, matrix.numCols) == (3, 4)

    def test_should_give_probabilities_that_add_up_to_one(
        self, spark_session: SparkSession
    ) -> None:
        """The column "probability" has one probability per class."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        rows = train(data).transform(data).limit(10).collect()

        assert all(sum(row["probability"]) == pytest.approx(1.0) for row in rows)

    def test_should_raise_error_if_regularization_is_negative(
        self, spark_session: SparkSession
    ) -> None:
        """The regularization parameter cannot be negative."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        with pytest.raises((PySparkException, ValueError)):
            train(data, regularization=-1.0)
