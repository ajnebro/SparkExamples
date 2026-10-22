"""Tests for ml.svm_training."""

import pytest
from pyspark.errors import PySparkException
from pyspark.ml.linalg import Vectors
from pyspark.sql import DataFrame, SparkSession

from ml.svm_training import train


def separable_points(spark_session: SparkSession) -> DataFrame:
    """Create points of two classes that a straight line separates."""
    data = [
        (0.0, Vectors.dense([1.0, 1.0])),
        (0.0, Vectors.dense([1.5, 1.2])),
        (1.0, Vectors.dense([6.0, 1.0])),
        (1.0, Vectors.dense([5.5, 1.3])),
    ]

    return spark_session.createDataFrame(data, ["label", "features"])


class TestTrain:
    def test_should_learn_one_coefficient_per_feature(self, spark_session: SparkSession) -> None:
        """The model has a coefficient for each feature of the vectors."""
        model = train(separable_points(spark_session))

        assert len(model.coefficients) == 2

    def test_should_classify_the_training_points_correctly(
        self, spark_session: SparkSession
    ) -> None:
        """Given separable classes, the model predicts the label of every training point."""
        points = separable_points(spark_session)

        rows = train(points).transform(points).collect()

        assert all(row["label"] == row["prediction"] for row in rows)

    def test_should_raise_error_if_there_is_no_features_column(
        self, spark_session: SparkSession
    ) -> None:
        """The training data must have the columns "label" and "features"."""
        data_frame = spark_session.createDataFrame([(0.0, 1.0)], ["label", "x"])

        with pytest.raises(PySparkException):
            train(data_frame)
