"""Evaluate a saved SVM model with labeled test data.

It loads the model saved by `ml.svm_training`, predicts the class of some points whose class is
known and measures the accuracy: the fraction of points that are classified correctly. The test
points are not in the training data, so the accuracy shows how well the model generalizes.
"""

from pyspark.ml.classification import LinearSVCModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import DataFrame, SparkSession

from ml.svm_training import DEFAULT_MODEL_DIRECTORY

TEST_POINTS = [
    (0.0, Vectors.dense([1.0, 1.2])),
    (1.0, Vectors.dense([5.3, 2.4])),
    (0.0, Vectors.dense([1.2, 1.3])),
    (1.0, Vectors.dense([5.1, 2.3])),
]


def evaluate(model: LinearSVCModel, test_data: DataFrame) -> float:
    """Compute the accuracy of a model.

    Args:
        model: Trained model.
        test_data: DataFrame with the columns "label" and "features".

    Returns:
        The fraction of rows whose prediction is equal to the label, between 0 and 1.
    """
    predictions = model.transform(test_data)
    predictions.show()

    # fmt: off
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    # fmt: on

    return evaluator.evaluate(predictions)


def main(model_directory: str = DEFAULT_MODEL_DIRECTORY) -> None:
    """Run the example.

    Args:
        model_directory: Directory where `ml.svm_training` saved the model.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("SVMEvaluation")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        model = LinearSVCModel.load(model_directory)
        test_data = spark_session.createDataFrame(TEST_POINTS, ["label", "features"])

        accuracy = evaluate(model, test_data)
        print(f"Accuracy = {accuracy:g}, test error = {1.0 - accuracy:g}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
