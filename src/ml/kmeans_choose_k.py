"""Choose the number of clusters of k-means by comparing the silhouette for several values.

K-means needs the number of clusters `k` in advance. A common way to choose it is to train a
model for each candidate and keep the one with the highest silhouette. The silhouette of one
cluster is not defined, so the values start at 2.
"""

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame, SparkSession

from ml.read_libsvm import read_libsvm
from ml.svm_training import DEFAULT_INPUT_FILE

DEFAULT_VALUES_OF_K = [2, 3, 4, 5]
SEED = 42


def silhouette_by_k(data: DataFrame, values_of_k: list[int]) -> list[tuple[int, float]]:
    """Train a k-means model for each value of k and compute its silhouette.

    Args:
        data: DataFrame with a column "features" (vector).
        values_of_k: Numbers of clusters to try, each of them 2 or more.

    Returns:
        A list of pairs (k, silhouette) in the same order as `values_of_k`.
    """
    evaluator = ClusteringEvaluator()
    silhouettes = []

    for k in values_of_k:
        model = KMeans(k=k, maxIter=10, seed=SEED).fit(data)
        silhouettes.append((k, evaluator.evaluate(model.transform(data))))

    return silhouettes


def main(file_name: str = DEFAULT_INPUT_FILE, values_of_k: list[int] = DEFAULT_VALUES_OF_K) -> None:
    """Run the example.

    Args:
        file_name: Path of the data in LIBSVM format.
        values_of_k: Numbers of clusters to try.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("KMeansChooseK")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        data = read_libsvm(spark_session, file_name).cache()
        results = silhouette_by_k(data, values_of_k)

        for k, value in results:
            print(f"k = {k}: silhouette = {value:.3f}")

        best_k, best_value = max(results, key=lambda result: result[1])
        print(f"Best k: {best_k} (silhouette = {best_value:.3f})")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
