"""Tests for ml.kmeans_choose_k."""

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from ml.kmeans_choose_k import silhouette_by_k
from ml.read_libsvm import read_libsvm
from ml.svm_training import DEFAULT_INPUT_FILE


class TestSilhouetteByK:
    def test_should_return_a_silhouette_for_each_value_of_k(
        self, spark_session: SparkSession
    ) -> None:
        """The result keeps the order of the values of k."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        results = silhouette_by_k(data, [2, 3, 4])

        assert [k for k, _ in results] == [2, 3, 4]

    def test_should_prefer_two_clusters_for_data_with_two_groups(
        self, spark_session: SparkSession
    ) -> None:
        """The data has two well separated classes, so k=2 has the highest silhouette."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        results = silhouette_by_k(data, [2, 3, 4, 5])

        assert max(results, key=lambda result: result[1])[0] == 2

    def test_should_raise_error_if_k_is_smaller_than_two(self, spark_session: SparkSession) -> None:
        """k-means needs at least two clusters to compute a silhouette."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        with pytest.raises(PySparkException):
            silhouette_by_k(data, [1])
