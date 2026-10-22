"""Prepare the Iris data of the UCI repository to be used by Spark ML.

The original file (`data/iris.data`) is a CSV without header, with the four measurements and
the name of the species in each line. The Spark ML algorithms need instead a numeric column
"label" and a column "features" with all the measurements in a single vector. Two stages of a
`Pipeline` do it:

- `StringIndexer` replaces the name of the species by a number (0.0, 1.0, 2.0).
- `VectorAssembler` joins the measurements in a vector.

The result has the same structure as the file read by `ml.read_libsvm` (`data/iris.libsvm`), which
was generated in this way.
"""

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

DEFAULT_INPUT_FILE = "data/iris.data"
FEATURE_COLUMNS = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

IRIS_SCHEMA = StructType(
    [StructField(name, DoubleType()) for name in FEATURE_COLUMNS]
    + [StructField("species", StringType())]
)


def read_iris(spark_session: SparkSession, file_name: str) -> DataFrame:
    """Read the CSV file of Iris, which has no header.

    The schema is given, so that Spark does not need to read the file twice to infer it.

    Args:
        spark_session: Active Spark session.
        file_name: Path of the CSV file.

    Returns:
        A DataFrame with the columns of `IRIS_SCHEMA`.
    """
    # fmt: off
    return (
        spark_session.read
        .schema(IRIS_SCHEMA)
        .csv(file_name)
    )
    # fmt: on


def preprocess(iris: DataFrame) -> DataFrame:
    """Convert the species into a numeric label and the measurements into a vector.

    The species are numbered in alphabetical order: setosa is 0.0, versicolor 1.0 and virginica
    2.0. Without this order, `StringIndexer` numbers the values by frequency, and as all the
    species are equally frequent the result would not be predictable.

    Args:
        iris: DataFrame with the columns of `IRIS_SCHEMA`.

    Returns:
        A DataFrame with the columns "label" (double) and "features" (vector).
    """
    indexer = StringIndexer(inputCol="species", outputCol="label", stringOrderType="alphabetAsc")
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")

    # fmt: off
    return (
        Pipeline(stages=[indexer, assembler])
        .fit(iris)
        .transform(iris)
        .select("label", "features")
    )
    # fmt: on


def main(file_name: str = DEFAULT_INPUT_FILE) -> None:
    """Run the example.

    Args:
        file_name: Path of the original Iris file.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("PreprocessIris")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        iris = read_iris(spark_session, file_name)
        print("Original data")
        iris.show(5)

        processed_data = preprocess(iris)
        print("Preprocessed data")
        processed_data.printSchema()
        processed_data.show(5, truncate=False)
        processed_data.groupBy("label").count().orderBy("label").show()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
