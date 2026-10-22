"""Train a k-means clustering model and save it.

K-means groups the points in `k` clusters, so that each point is close to the center of its
cluster. The data (`data/classificationDataLibsvm.txt`) has a label, but k-means is an
unsupervised algorithm: it only uses the "features" column and ignores the label. As the labels
are known here, they allow to check that the clusters found match the real classes.

`KMeans` is an estimator: `fit` returns a model, which is a transformer that adds the column
"prediction" with the cluster of each point. The model is saved so that `ml.kmeans_prediction`
can use it: run this example first.
"""

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.sql import DataFrame, SparkSession

from ml.read_libsvm import read_libsvm
from ml.svm_training import DEFAULT_INPUT_FILE

DEFAULT_MODEL_DIRECTORY = "output/kmeans_model"
NUMBER_OF_CLUSTERS = 2
SEED = 42


def train(data: DataFrame, k: int = NUMBER_OF_CLUSTERS) -> KMeansModel:
    """Group the points in k clusters.

    Args:
        data: DataFrame with a column "features" (vector).
        k: Number of clusters.

    Returns:
        The trained model, with the center of each cluster.
    """
    kmeans = KMeans(k=k, maxIter=10, seed=SEED)

    return kmeans.fit(data)


def main(
    file_name: str = DEFAULT_INPUT_FILE, model_directory: str = DEFAULT_MODEL_DIRECTORY
) -> None:
    """Run the example.

    Args:
        file_name: Path of the data in LIBSVM format.
        model_directory: Directory where the model is saved. It is overwritten if it exists.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("KMeansTraining")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        data = read_libsvm(spark_session, file_name)
        model = train(data)

        print("Cluster centers:")
        for center in model.clusterCenters():
            print(center)

        print("Cluster of each point, next to its real label:")
        model.transform(data).select("label", "features", "prediction").show()

        model.write().overwrite().save(model_directory)
        print(f"Model saved in {model_directory}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
