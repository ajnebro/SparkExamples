"""Predict the class of new points with a saved SVM model.

Unlike `ml.svm_evaluation`, the points have no label (there is only an identifier), as happens
when a model is used in production: `transform` adds the columns "rawPrediction" (the distance to
the separating hyperplane, as a vector) and "prediction" (the class, 0.0 or 1.0).
"""

from pyspark.ml.classification import LinearSVCModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import DataFrame, SparkSession

from ml.svm_training import DEFAULT_MODEL_DIRECTORY

NEW_POINTS = [
    (0, Vectors.dense([1.0, 1.2])),
    (1, Vectors.dense([5.3, 2.4])),
    (2, Vectors.dense([1.2, 1.3])),
    (3, Vectors.dense([5.1, 2.3])),
]


def predict(model: LinearSVCModel, points: DataFrame) -> DataFrame:
    """Predict the class of some points.

    Args:
        model: Trained model.
        points: DataFrame with a column "features" (vector).

    Returns:
        The same DataFrame with the extra columns "rawPrediction" and "prediction".
    """
    return model.transform(points)


def main(model_directory: str = DEFAULT_MODEL_DIRECTORY) -> None:
    """Run the example.

    Args:
        model_directory: Directory where `ml.svm_training` saved the model.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("SVMPrediction")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        model = LinearSVCModel.load(model_directory)
        points = spark_session.createDataFrame(NEW_POINTS, ["id", "features"])

        predict(model, points).select("id", "features", "prediction").show()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
