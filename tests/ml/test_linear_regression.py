"""Tests for ml.linear_regression."""

import pytest
from pyspark.errors import PySparkException
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml.linear_regression import evaluate, prepare_data, train
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


class TestTrainAndEvaluate:
    def test_should_explain_most_of_the_variance_of_the_petal_width(
        self, spark_session: SparkSession
    ) -> None:
        """The petal width depends almost linearly on the other measurements."""
        data = prepare_data(read_libsvm(spark_session, DEFAULT_INPUT_FILE))

        assert evaluate(train(data), data, "r2") > 0.9

    def test_should_have_a_small_error_in_centimeters(self, spark_session: SparkSession) -> None:
        """The RMSE is in the unit of the label, and it is a fraction of a centimeter."""
        data = prepare_data(read_libsvm(spark_session, DEFAULT_INPUT_FILE))

        assert evaluate(train(data), data, "rmse") < 0.3

    def test_should_raise_error_if_the_metric_is_unknown(self, spark_session: SparkSession) -> None:
        """Only the metrics of the evaluator are accepted."""
        data = prepare_data(read_libsvm(spark_session, DEFAULT_INPUT_FILE))
        model = train(data)

        with pytest.raises((PySparkException, ValueError)):
            evaluate(model, data, "unknown")
