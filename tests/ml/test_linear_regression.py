"""Tests for ml.linear_regression."""

import pytest
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml.linear_regression import prepare_data
from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm


class TestPrepareData:
    def test_should_use_the_fourth_element_as_label_and_the_rest_as_features(
        self, spark_session: SparkSession
    ) -> None:
        """The petal width becomes the label and the other three measurements, the features."""
        iris = spark_session.createDataFrame(
            [(0.0, Vectors.dense([5.1, 3.5, 1.4, 0.2]))], ["label", "features"]
        )

        row = prepare_data(iris).collect()[0]

        assert row["label"] == pytest.approx(0.2)
        assert row["features"].toArray().tolist() == pytest.approx([5.1, 3.5, 1.4])

    def test_should_keep_one_row_per_flower(self, spark_session: SparkSession) -> None:
        """No flower is lost."""
        iris = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        assert prepare_data(iris).count() == iris.count()
