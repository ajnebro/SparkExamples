"""Tests for ml.svm_classification_workflow."""

import pytest
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm
from ml.svm_classification_workflow import accuracy, build_pipeline, split_data


class TestSplitData:
    def test_should_split_all_the_rows_in_two_disjoint_sets(
        self, spark_session: SparkSession
    ) -> None:
        """No row is lost or repeated."""
        data = spark_session.range(1000)

        training_data, test_data = split_data(data)

        assert training_data.count() + test_data.count() == 1000
        assert training_data.intersect(test_data).count() == 0

    def test_should_give_the_same_split_with_the_same_seed(
        self, spark_session: SparkSession
    ) -> None:
        """The seed makes the split reproducible."""
        data = spark_session.range(1000)

        first, _ = split_data(data, seed=7)
        second, _ = split_data(data, seed=7)

        assert first.count() == second.count()


class TestAccuracy:
    @pytest.mark.parametrize(
        ("predictions", "expected"),
        [([0.0, 1.0, 1.0, 0.0], 1.0), ([0.0, 1.0, 0.0, 1.0], 0.5), ([1.0, 0.0, 0.0, 1.0], 0.0)],
    )
    def test_should_return_the_fraction_of_right_predictions(
        self, spark_session: SparkSession, predictions: list[float], expected: float
    ) -> None:
        """The accuracy is the number of matches divided by the number of rows."""
        labels = [0.0, 1.0, 1.0, 0.0]
        data_frame = spark_session.createDataFrame(
            list(zip(labels, predictions, strict=True)), ["label", "prediction"]
        )

        assert accuracy(data_frame) == pytest.approx(expected)


class TestBuildPipeline:
    def test_should_classify_the_iris_data_well(self, spark_session: SparkSession) -> None:
        """A model trained with the Iris data classifies most of the training flowers."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        model = build_pipeline().fit(data)

        assert accuracy(model.transform(data)) > 0.9

    def test_should_add_the_scaled_features_and_the_prediction(
        self, spark_session: SparkSession
    ) -> None:
        """The pipeline adds the output column of each stage."""
        data = spark_session.createDataFrame(
            [(0.0, Vectors.dense([1.0, 2.0])), (1.0, Vectors.dense([5.0, 1.0]))],
            ["label", "features"],
        )

        columns = build_pipeline().fit(data).transform(data).columns

        assert "scaled_features" in columns
        assert "prediction" in columns
