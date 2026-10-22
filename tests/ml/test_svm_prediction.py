"""Tests for ml.svm_prediction."""

import pytest
from pyspark.errors import PySparkException
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml.svm_prediction import NEW_POINTS, predict
from ml.svm_training import train


@pytest.fixture
def model(spark_session: SparkSession):
    """Train a model that classifies by the first feature."""
    data = [
        (0.0, Vectors.dense([1.0, 1.0])),
        (0.0, Vectors.dense([1.5, 1.2])),
        (1.0, Vectors.dense([6.0, 1.0])),
        (1.0, Vectors.dense([5.5, 1.3])),
    ]

    return train(spark_session.createDataFrame(data, ["label", "features"]))


class TestPredict:
    def test_should_predict_the_class_of_each_point(
        self, spark_session: SparkSession, model
    ) -> None:
        """Points close to each class get the label of that class."""
        points = spark_session.createDataFrame(NEW_POINTS, ["id", "features"])

        rows = predict(model, points).orderBy("id").collect()

        assert [row["prediction"] for row in rows] == [0.0, 1.0, 0.0, 1.0]

    def test_should_keep_the_original_columns(self, spark_session: SparkSession, model) -> None:
        """The result has the original columns and the new ones."""
        points = spark_session.createDataFrame(NEW_POINTS, ["id", "features"])

        assert predict(model, points).columns == ["id", "features", "rawPrediction", "prediction"]

    def test_should_raise_error_if_there_is_no_features_column(
        self, spark_session: SparkSession, model
    ) -> None:
        """The points must have a column "features"."""
        points = spark_session.createDataFrame([(0, 1.0)], ["id", "x"])

        with pytest.raises(PySparkException):
            predict(model, points).collect()
