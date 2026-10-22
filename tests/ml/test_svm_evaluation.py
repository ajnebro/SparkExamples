"""Tests for ml.svm_evaluation."""

import pytest
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml.svm_evaluation import TEST_POINTS, evaluate
from ml.svm_training import train


class TestEvaluate:
    @pytest.fixture
    def model(self, spark_session: SparkSession):
        """Train a model that classifies by the first feature."""
        data = [
            (0.0, Vectors.dense([1.0, 1.0])),
            (0.0, Vectors.dense([1.5, 1.2])),
            (1.0, Vectors.dense([6.0, 1.0])),
            (1.0, Vectors.dense([5.5, 1.3])),
        ]

        return train(spark_session.createDataFrame(data, ["label", "features"]))

    def test_should_return_one_if_all_the_predictions_are_right(
        self, spark_session: SparkSession, model
    ) -> None:
        """The sample test points are well separated, so all of them are classified correctly."""
        test_data = spark_session.createDataFrame(TEST_POINTS, ["label", "features"])

        assert evaluate(model, test_data) == pytest.approx(1.0)

    def test_should_return_zero_if_all_the_predictions_are_wrong(
        self, spark_session: SparkSession, model
    ) -> None:
        """With the labels swapped, no prediction matches."""
        swapped = [(1.0 - label, features) for label, features in TEST_POINTS]
        test_data = spark_session.createDataFrame(swapped, ["label", "features"])

        assert evaluate(model, test_data) == pytest.approx(0.0)
