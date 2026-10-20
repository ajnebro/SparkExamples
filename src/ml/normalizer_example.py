"""Normalize the feature vectors of a DataFrame with the Spark ML `Normalizer`.

A normalizer divides each vector by its own norm, so that all the vectors have the same
length. The norm is set by the parameter `p`: with `p=1` (L1 norm) the vector is divided by the
sum of the absolute values of its elements, and with `p=inf` (infinity norm), by the largest
absolute value. Each row is normalized on its own, so Spark does not need to shuffle any data.
"""

from pyspark.ml.feature import Normalizer
from pyspark.ml.linalg import Vectors
from pyspark.sql import DataFrame, SparkSession

SAMPLE_VECTORS = [
    (0, Vectors.dense([1.0, 0.5, -1.0])),
    (1, Vectors.dense([2.0, 1.0, 1.0])),
    (2, Vectors.dense([4.0, 10.0, 2.0])),
]


def normalize(data_frame: DataFrame, p: float) -> DataFrame:
    """Divide each vector by its p-norm.

    Args:
        data_frame: DataFrame with a column "features" of type vector.
        p: Order of the norm. It must be greater than or equal to 1; `float("inf")` is the
            infinity norm.

    Returns:
        The same DataFrame with the extra column "normalized_features".

    Raises:
        PySparkException: When the query runs, if `p` is smaller than 1.
    """
    normalizer = Normalizer(inputCol="features", outputCol="normalized_features", p=p)

    return normalizer.transform(data_frame)


def main() -> None:
    """Run the example."""
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("NormalizerExample")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        data_frame = spark_session.createDataFrame(SAMPLE_VECTORS, ["id", "features"])

        print("Normalized using the L1 norm")
        normalize(data_frame, 1.0).show()

        print("Normalized using the infinity norm")
        normalize(data_frame, float("inf")).show()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
