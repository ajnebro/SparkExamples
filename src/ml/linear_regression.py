"""Predict the petal width of the Iris flowers from the other three measurements.

Regression predicts a number instead of a class. The Iris file has the four measurements in the
vector "features", so the data is first rearranged: the petal width (the fourth element) becomes
the "label" to predict, and a `VectorSlicer` keeps the other three as the "features".

The model is evaluated with the root mean squared error (RMSE), in the same unit as the label
(centimeters), and with R2, the fraction of the variance of the label that the model explains
(1 is a perfect fit). Both are computed with the training set, which the model has seen, and
with the test set, which it has not.
"""

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.sql import DataFrame, SparkSession, functions

from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm

PETAL_WIDTH_INDEX = 3
TRAINING_FRACTION = 0.7
SEED = 42


def prepare_data(iris: DataFrame) -> DataFrame:
    """Use the petal width as label and the other measurements as features.

    Args:
        iris: DataFrame with a column "features" of four elements.

    Returns:
        A DataFrame with the columns "label" (the petal width) and "features" (the first three
        measurements).
    """
    slicer = VectorSlicer(inputCol="features", outputCol="selected", indices=[0, 1, 2])
    petal_width = vector_to_array("features")[PETAL_WIDTH_INDEX]

    # fmt: off
    return (
        slicer.transform(iris)
        .select(petal_width.alias("label"), functions.col("selected").alias("features"))
    )
    # fmt: on


def train(training_data: DataFrame) -> LinearRegressionModel:
    """Learn a linear regression.

    Args:
        training_data: DataFrame with the columns "label" (number) and "features" (vector).

    Returns:
        The trained model.
    """
    linear_regression = LinearRegression(maxIter=100)

    return linear_regression.fit(training_data)


def evaluate(model: LinearRegressionModel, data: DataFrame, metric: str) -> float:
    """Compute an error metric of the model.

    Args:
        model: Trained model.
        data: DataFrame with the columns "label" and "features".
        metric: "rmse" (root mean squared error) or "r2".

    Returns:
        The value of the metric.
    """
    evaluator = RegressionEvaluator(metricName=metric)

    return evaluator.evaluate(model.transform(data))


def main(file_name: str = DEFAULT_INPUT_FILE) -> None:
    """Run the example.

    Args:
        file_name: Path of the Iris data in LIBSVM format.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("LinearRegression")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        data = prepare_data(read_libsvm(spark_session, file_name))
        training_data, test_data = data.randomSplit(
            [TRAINING_FRACTION, 1.0 - TRAINING_FRACTION], SEED
        )

        model = train(training_data)
        print(f"Coefficients: {model.coefficients}")
        print(f"Intercept: {model.intercept:.3f}")

        for name, subset in [("training", training_data), ("test", test_data)]:
            rmse = evaluate(model, subset, "rmse")
            print(f"{name}: RMSE = {rmse:.3f}, R2 = {evaluate(model, subset, 'r2'):.3f}")

        model.transform(test_data).select("label", "prediction").show(5)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
