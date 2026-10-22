"""Tests for ml.decision_tree."""

import pytest
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession

from ml.decision_tree import accuracy, train
from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm


class TestTrain:
    def test_should_classify_the_iris_data_well(self, spark_session: SparkSession) -> None:
        """A tree of depth 3 classifies most of the flowers it was trained with."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        assert accuracy(train(data), data) > 0.9

    @pytest.mark.parametrize("max_depth", [1, 2, 3])
    def test_should_not_exceed_the_maximum_depth(
        self, spark_session: SparkSession, max_depth: int
    ) -> None:
        """The depth of the tree is limited by the parameter."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        assert train(data, max_depth).depth <= max_depth

    def test_should_have_one_importance_per_feature(self, spark_session: SparkSession) -> None:
        """The importances of the four features add up to one."""
        data = read_libsvm(spark_session, DEFAULT_INPUT_FILE)

        importances = train(data).featureImportances

        assert len(importances) == 4
        assert sum(importances) == pytest.approx(1.0)

    def test_should_raise_error_if_there_is_no_features_column(
        self, spark_session: SparkSession
    ) -> None:
        """The data must have the columns "label" and "features"."""
        data = spark_session.createDataFrame([(0.0, 1.0)], ["label", "x"])

        with pytest.raises(PySparkException):
            train(data)
