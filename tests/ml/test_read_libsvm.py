"""Tests for ml.read_libsvm."""

from pathlib import Path

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm


class TestReadLibsvm:
    def test_should_read_labels_and_sparse_features(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Each line gives a label and a vector, in which the missing indices are zeros."""
        file = tmp_path / "data.libsvm"
        file.write_text("1 1:0.5 3:2.0\n0 2:-1.0\n")

        rows = read_libsvm(spark_session, str(file)).collect()

        assert sorted((row["label"], row["features"].toArray().tolist()) for row in rows) == [
            (0.0, [0.0, -1.0, 0.0]),
            (1.0, [0.5, 0.0, 2.0]),
        ]

    def test_should_return_label_and_features_columns(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """The data source returns a double label and a vector of features."""
        file = tmp_path / "data.libsvm"
        file.write_text("1 1:0.5\n")

        schema = read_libsvm(spark_session, str(file)).schema

        assert [(field.name, field.dataType.simpleString()) for field in schema] == [
            ("label", "double"),
            ("features", "vector"),
        ]

    def test_should_raise_error_if_file_does_not_exist(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Given a missing file, Spark raises its own error."""
        with pytest.raises(PySparkException):
            read_libsvm(spark_session, str(tmp_path / "missing.libsvm"))

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
