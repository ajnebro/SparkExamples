"""Tests for ml.preprocess_iris."""

import pytest
from pyspark.sql import SparkSession

from ml.preprocess_iris import DEFAULT_INPUT_FILE, preprocess, read_iris


class TestReadIris:
    def test_should_read_the_150_flowers_without_header(self, spark_session: SparkSession) -> None:
        """The empty last line of the original file is not read as a row."""
        assert read_iris(spark_session, DEFAULT_INPUT_FILE).count() == 150


class TestPreprocess:
    def test_should_number_the_species_in_alphabetical_order(
        self, spark_session: SparkSession
    ) -> None:
        """Setosa is 0.0, versicolor 1.0 and virginica 2.0, with 50 flowers each."""
        iris = read_iris(spark_session, DEFAULT_INPUT_FILE)

        counts = {
            row["label"]: row["count"]
            for row in preprocess(iris).groupBy("label").count().collect()
        }

        assert counts == {0.0: 50, 1.0: 50, 2.0: 50}

    def test_should_join_the_four_measurements_in_a_vector(
        self, spark_session: SparkSession
    ) -> None:
        """The first flower of the file has the expected label and features."""
        iris = read_iris(spark_session, DEFAULT_INPUT_FILE).limit(1)

        row = preprocess(iris).collect()[0]

        assert row["features"].toArray().tolist() == pytest.approx([5.1, 3.5, 1.4, 0.2])

    def test_should_return_only_label_and_features(self, spark_session: SparkSession) -> None:
        """The original columns are dropped."""
        iris = read_iris(spark_session, DEFAULT_INPUT_FILE)

        assert preprocess(iris).columns == ["label", "features"]
