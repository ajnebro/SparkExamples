"""Tests for ml.kmeans_prediction."""

import pytest
from pyspark.errors import PySparkException
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml.kmeans_prediction import NEW_POINTS, assign_clusters, silhouette
from ml.kmeans_training import train
from ml.read_libsvm import read_libsvm
from ml.svm_training import DEFAULT_INPUT_FILE


@pytest.fixture
def model(spark_session: SparkSession):
    """Train a model with two clusters."""
    return train(read_libsvm(spark_session, DEFAULT_INPUT_FILE))


class TestAssignClusters:
    def test_should_put_close_points_in_the_same_cluster(
        self, spark_session: SparkSession, model
    ) -> None:
        """The points 1, 2 and 3 are near each other and far from the point 4."""
        points = spark_session.createDataFrame(NEW_POINTS, ["id", "features"])

        rows = assign_clusters(model, points).orderBy("id").collect()
        clusters = [row["prediction"] for row in rows]

        assert clusters[0] == clusters[1] == clusters[2]
        assert clusters[3] != clusters[0]

    def test_should_keep_the_original_columns(self, spark_session: SparkSession, model) -> None:
        """The result has the original columns and the new one."""
        points = spark_session.createDataFrame(NEW_POINTS, ["id", "features"])

        assert assign_clusters(model, points).columns == ["id", "features", "prediction"]

    def test_should_raise_error_if_there_is_no_features_column(
        self, spark_session: SparkSession, model
    ) -> None:
        """The points must have a column "features"."""
        points = spark_session.createDataFrame([(1, 1.0)], ["id", "x"])

        with pytest.raises(PySparkException):
            assign_clusters(model, points).collect()


class TestSilhouette:
    def test_should_be_close_to_one_for_well_separated_clusters(
        self, spark_session: SparkSession, model
    ) -> None:
        """The two groups of points are far from each other."""
        points = spark_session.createDataFrame(
            [(1, Vectors.dense([1.0, 3.0])), (2, Vectors.dense([5.5, 1.5]))] * 3,
            ["id", "features"],
        )

        assert silhouette(assign_clusters(model, points)) > 0.9
