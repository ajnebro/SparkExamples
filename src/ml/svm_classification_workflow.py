"""Complete machine learning workflow: split, train, evaluate, save, load and predict.

It classifies the Iris flowers (3 classes, 4 features) with a `Pipeline` of two stages: a
`StandardScaler`, which scales each feature to unit standard deviation, and a
`LogisticRegression`. The steps are the ones of any Spark ML application:

1. Read the data and split it at random into training (80%) and test (20%) sets.
2. `fit` the pipeline with the training set. The result is a `PipelineModel` that applies the
   fitted stages in order. Fitting with the training set only avoids leaking information from
   the test set.
3. `transform` the test set and measure the accuracy with an evaluator.
4. Save the model and load it again, as a program that only makes predictions would do.
"""

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StandardScaler
from pyspark.sql import DataFrame, SparkSession

from ml.read_libsvm import DEFAULT_INPUT_FILE, read_libsvm

DEFAULT_MODEL_DIRECTORY = "output/iris_model"
TRAINING_FRACTION = 0.8
SEED = 42


def build_pipeline() -> Pipeline:
    """Create the pipeline: feature scaling followed by a logistic regression.

    Returns:
        An unfitted pipeline that reads "features" and adds the column "prediction".
    """
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    classifier = LogisticRegression(featuresCol="scaled_features", maxIter=100)

    return Pipeline(stages=[scaler, classifier])


def split_data(data: DataFrame, seed: int = SEED) -> tuple[DataFrame, DataFrame]:
    """Split the data at random into a training set and a test set.

    Args:
        data: DataFrame to split.
        seed: Seed of the random split, so that the result can be reproduced.

    Returns:
        The training set (about `TRAINING_FRACTION` of the rows) and the test set.
    """
    training_data, test_data = data.randomSplit([TRAINING_FRACTION, 1.0 - TRAINING_FRACTION], seed)

    return training_data, test_data


def accuracy(predictions: DataFrame) -> float:
    """Compute the fraction of rows whose prediction is equal to the label.

    Args:
        predictions: DataFrame with the columns "label" and "prediction".

    Returns:
        The accuracy, between 0 and 1.
    """
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

    return evaluator.evaluate(predictions)


def main(
    file_name: str = DEFAULT_INPUT_FILE, model_directory: str = DEFAULT_MODEL_DIRECTORY
) -> None:
    """Run the example.

    Args:
        file_name: Path of the Iris data in LIBSVM format.
        model_directory: Directory where the model is saved. It is overwritten if it exists.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("ClassificationWorkflow")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        training_data, test_data = split_data(read_libsvm(spark_session, file_name))
        print(f"Training rows: {training_data.count()}, test rows: {test_data.count()}")

        model = build_pipeline().fit(training_data)
        print(f"Test accuracy: {accuracy(model.transform(test_data)):.3f}")

        model.write().overwrite().save(model_directory)
        loaded_model = PipelineModel.load(model_directory)
        loaded_model.transform(test_data).select("label", "prediction").show(5)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
