"""Train a linear Support Vector Machine (SVM) and save the model.

The data (`data/classificationDataLibsvm.txt`) is a small set of points with two features, in
LIBSVM format, that belong to two classes (0 and 1). `LinearSVC` is an estimator: `fit` learns
from the data and returns a model, which is a transformer. The model is saved to a directory so
that other programs can use it: run this example first, and then `ml.svm_evaluation` and
`ml.svm_prediction`.
"""

from pyspark.ml.classification import LinearSVC, LinearSVCModel
from pyspark.sql import DataFrame, SparkSession

from ml.read_libsvm import read_libsvm

DEFAULT_INPUT_FILE = "data/classificationDataLibsvm.txt"
DEFAULT_MODEL_DIRECTORY = "output/svm_model"


def train(training_data: DataFrame) -> LinearSVCModel:
    """Learn a linear SVM from labeled data.

    Args:
        training_data: DataFrame with the columns "label" (0.0 or 1.0) and "features" (vector).

    Returns:
        The trained model, with its coefficients and intercept.
    """
    linear_svm = LinearSVC(maxIter=100, regParam=0.1)

    return linear_svm.fit(training_data)


def main(
    file_name: str = DEFAULT_INPUT_FILE, model_directory: str = DEFAULT_MODEL_DIRECTORY
) -> None:
    """Run the example.

    Args:
        file_name: Path of the training data in LIBSVM format.
        model_directory: Directory where the model is saved. It is overwritten if it exists.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("SVMTraining")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        training_data = read_libsvm(spark_session, file_name)
        training_data.show()
        model = train(training_data)
        print(f"Coefficients: {model.coefficients}")
        print(f"Intercept: {model.intercept}")

        model.write().overwrite().save(model_directory)
        print(f"Model saved in {model_directory}")
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
