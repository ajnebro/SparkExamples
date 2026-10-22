"""Classify the Iris flowers with a logistic regression and look at its probabilities.

`LogisticRegression` is an estimator: its parameters (`maxIter`, `regParam`, ...) are set when
it is created, `explainParams` documents them, and `fit` returns a model. With three classes
the model is multinomial: it learns a vector of coefficients per class (`coefficientMatrix`)
and, besides the class, `transform` adds the column "probability" with the probability of each
one. The class predicted is the most probable.
"""

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame, SparkSession

from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm

REGULARIZATION = 0.01
TRAINING_FRACTION = 0.7
SEED = 42


def train(
    training_data: DataFrame, regularization: float = REGULARIZATION
) -> LogisticRegressionModel:
    """Learn a logistic regression.

    Args:
        training_data: DataFrame with the columns "label" and "features" (vector).
        regularization: Strength of the penalty on large coefficients (`regParam`). A higher
            value gives a simpler model that is less prone to overfitting.

    Returns:
        The trained model.
    """
    logistic_regression = LogisticRegression(maxIter=100, regParam=regularization)

    return logistic_regression.fit(training_data)


def accuracy(model: LogisticRegressionModel, test_data: DataFrame) -> float:
    """Compute the fraction of rows of the test set that the model classifies correctly.

    Args:
        model: Trained model.
        test_data: DataFrame with the columns "label" and "features".

    Returns:
        The accuracy, between 0 and 1.
    """
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

    return evaluator.evaluate(model.transform(test_data))


def main(file_name: str = DEFAULT_INPUT_FILE) -> None:
    """Run the example.

    Args:
        file_name: Path of the Iris data in LIBSVM format.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("LogisticRegression")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        data = read_libsvm(spark_session, file_name)
        training_data, test_data = data.randomSplit(
            [TRAINING_FRACTION, 1.0 - TRAINING_FRACTION], SEED
        )

        print(LogisticRegression().explainParams())
        model = train(training_data)
        print(f"Coefficients, one row per class:\n{model.coefficientMatrix}")
        print(f"Intercepts: {model.interceptVector}")

        model.transform(test_data).select("label", "probability", "prediction").show(5, False)
        print(f"Test accuracy: {accuracy(model, test_data):.3f}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
