"""Tests for the Iris data file read by ml.read_libsvm."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm


class TestIrisDataSet:
    @pytest.mark.skipif(not Path(DEFAULT_INPUT_FILE).exists(), reason="data/iris.libsvm not found")
    def test_should_read_the_iris_data_set(self, spark_session: SparkSession) -> None:
        """The Iris data has 150 flowers, 3 classes of 50 and 4 features."""
        data_frame = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        classes = {
            row["label"]: row["count"] for row in data_frame.groupBy("label").count().collect()
        }
        assert data_frame.count() == 150
        assert classes == {0.0: 50, 1.0: 50, 2.0: 50}
        assert data_frame.first()["features"].size == 4

    @pytest.mark.skipif(not Path(DEFAULT_INPUT_FILE).exists(), reason="data/iris.libsvm not found")
    def test_should_keep_the_original_values_of_the_iris_data_set(
        self, spark_session: SparkSession
    ) -> None:
        """The features are the original measurements: the sepal length goes from 4.3 to 7.9."""
        data_frame = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        sepal_lengths = [row["features"][0] for row in data_frame.collect()]

        assert (min(sepal_lengths), max(sepal_lengths)) == pytest.approx((4.3, 7.9))
