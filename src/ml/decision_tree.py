"""Classify the Iris flowers with a decision tree and show the tree that Spark learns.

A decision tree is a sequence of questions about the features ("is the petal length at most
2.45?") that ends in a class. Unlike most models, it can be read: `toDebugString` prints the
questions, and `featureImportances` says how much each feature contributes to the decisions.

The data is split at random into training (70%) and test (30%) sets, with a seed so that the
result can be reproduced. The tree is learned with the training set and its accuracy is
measured with the test set.
"""

from pyspark.ml.classification import DecisionTreeClassificationModel, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame, SparkSession

from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm

MAX_DEPTH = 3
TRAINING_FRACTION = 0.7
SEED = 42
FEATURE_NAMES = ["sepal length", "sepal width", "petal length", "petal width"]


def train(training_data: DataFrame, max_depth: int = MAX_DEPTH) -> DecisionTreeClassificationModel:
    """Learn a decision tree.

    Args:
        training_data: DataFrame with the columns "label" and "features" (vector).
        max_depth: Maximum number of questions between the root and a class. A deeper tree fits
            the training data better, but it can memorize it (overfitting).

    Returns:
        The trained model.
    """
    decision_tree = DecisionTreeClassifier(maxDepth=max_depth, seed=SEED)

    return decision_tree.fit(training_data)


def accuracy(model: DecisionTreeClassificationModel, test_data: DataFrame) -> float:
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
        .appName("DecisionTree")
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

        model = train(training_data)
        print(model.toDebugString)
        print("Importance of each feature:")
        for name, importance in zip(FEATURE_NAMES, model.featureImportances, strict=True):
            print(f"  {name}: {importance:.3f}")

        print(f"Test accuracy: {accuracy(model, test_data):.3f}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
