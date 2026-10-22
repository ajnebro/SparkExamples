"""Tests for ml.kmeans_training."""

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from ml.kmeans_training import train
from ml.read_libsvm import read_libsvm
from ml.svm_training import DEFAULT_INPUT_FILE


class TestTrain:
    def test_should_find_the_requested_number_of_clusters(
        self, spark_session: SparkSession
    ) -> None:
        """The model has one center per cluster."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        assert len(train(data, k=3).clusterCenters()) == 3

    def test_should_group_the_points_as_their_real_classes(
        self, spark_session: SparkSession
    ) -> None:
        """The two classes are far from each other, so k-means finds them without the labels."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        rows = train(data, k=2).transform(data).collect()
        pairs = {(row["label"], row["prediction"]) for row in rows}

        assert len(pairs) == 2

    def test_should_raise_error_if_there_is_no_features_column(
        self, spark_session: SparkSession
    ) -> None:
        """The data must have a column "features"."""
        data = spark_session.createDataFrame([(0.0, 1.0)], ["label", "x"])

        with pytest.raises(PySparkException):
            train(data)
