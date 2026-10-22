"""Assign new points to the clusters of a saved k-means model and measure the clustering.

It loads the model saved by `ml.kmeans_training`. `transform` adds the column "prediction" with
the cluster of each point. As there are no real classes to compare with, the quality of the
clustering is measured with the silhouette, between -1 and 1: the closer to 1, the closer each
point is to its own cluster and the farther from the others.
"""

from pyspark.ml.clustering import KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import DataFrame, SparkSession

from ml.kmeans_training import DEFAULT_MODEL_DIRECTORY

NEW_POINTS = [
    (1, Vectors.dense([4.2, 2.0])),
    (2, Vectors.dense([5.1, 1.4])),
    (3, Vectors.dense([5.2, 2.0])),
    (4, Vectors.dense([1.0, 4.0])),
]


def assign_clusters(model: KMeansModel, points: DataFrame) -> DataFrame:
    """Assign each point to the cluster with the closest center.

    Args:
        model: Trained model.
        points: DataFrame with a column "features" (vector).

    Returns:
        The same DataFrame with the extra column "prediction".
    """
    return model.transform(points)


def silhouette(clustered_points: DataFrame) -> float:
    """Measure the quality of a clustering with the silhouette (squared Euclidean distance).

    Args:
        clustered_points: DataFrame with the columns "features" and "prediction".

    Returns:
        The mean silhouette of the points, between -1 and 1.
    """
    evaluator = ClusteringEvaluator()

    return evaluator.evaluate(clustered_points)


def main(model_directory: str = DEFAULT_MODEL_DIRECTORY) -> None:
    """Run the example.

    Args:
        model_directory: Directory where `ml.kmeans_training` saved the model.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("KMeansPrediction")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        model = KMeansModel.load(model_directory)
        points = spark_session.createDataFrame(NEW_POINTS, ["id", "features"])

        clustered_points = assign_clusters(model, points)
        clustered_points.show()
        print(f"Silhouette = {silhouette(clustered_points):.3f}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
