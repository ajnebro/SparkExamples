"""Tests for ml.preprocess_wine."""

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from ml.preprocess_wine import DEFAULT_INPUT_FILE, FEATURE_COLUMNS, preprocess, read_wine


class TestReadWine:
    def test_should_read_the_178_wines_with_14_columns(self, spark_session: SparkSession) -> None:
        """The class and the 13 measurements are read without header."""
        wine = read_wine(spark_session, DEFAULT_INPUT_FILE)

        assert wine.count() == 178
        assert len(wine.columns) == 14

    def test_should_raise_error_if_the_file_does_not_exist(
        self, spark_session: SparkSession
    ) -> None:
        """Spark raises its own error for a missing file."""
        with pytest.raises(PySparkException):
            read_wine(spark_session, "data/does_not_exist.data")


class TestPreprocess:
    def test_should_make_the_labels_start_at_zero(self, spark_session: SparkSession) -> None:
        """The classes 1, 2 and 3 become the labels 0.0, 1.0 and 2.0."""
        wine = read_wine(spark_session, DEFAULT_INPUT_FILE)

        counts = {
            row["label"]: row["count"]
            for row in preprocess(wine).groupBy("label").count().collect()
        }

        assert counts == {0.0: 59, 1.0: 71, 2.0: 48}

    def test_should_join_the_13_measurements_in_the_original_order(
        self, spark_session: SparkSession
    ) -> None:
        """The first wine of the file has the expected features."""
        wine = read_wine(spark_session, DEFAULT_INPUT_FILE).limit(1)

        features = preprocess(wine).collect()[0]["features"]

        assert len(features) == len(FEATURE_COLUMNS) == 13
        assert features[0] == pytest.approx(14.23)
        assert features[12] == pytest.approx(1065.0)

    def test_should_return_only_label_and_features(self, spark_session: SparkSession) -> None:
        """The original columns are dropped."""
        wine = read_wine(spark_session, DEFAULT_INPUT_FILE)

        assert preprocess(wine).columns == ["label", "features"]
