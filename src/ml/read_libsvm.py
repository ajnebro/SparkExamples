"""Read a file in LIBSVM format as a DataFrame.

LIBSVM is a text format for labeled data: each line has a label followed by the non-zero
features as `index:value` pairs. Spark reads it with the `libsvm` data source, which returns a
DataFrame with a column "label" (double) and a column "features" (vector). The vectors are
sparse: only the non-zero values are stored, as `(size, [indices], [values])`.

The data is the Iris data set (150 flowers, 3 classes, 4 features in centimeters), described in
`src/ml/README.md`.
"""

from pyspark.sql import DataFrame, SparkSession

DEFAULT_INPUT_FILE = "data/iris.libsvm"


def read_libsvm(spark_session: SparkSession, file_name: str) -> DataFrame:
    """Read a LIBSVM file.

    Args:
        spark_session: Active Spark session.
        file_name: Path of the file in LIBSVM format.

    Returns:
        A DataFrame with the columns "label" (double) and "features" (vector).

    Raises:
        AnalysisException: If the file does not exist.
    """
    # fmt: off
    return (
        spark_session.read
        .format("libsvm")
        .load(file_name)
    )
    # fmt: on


def main(file_name: str = DEFAULT_INPUT_FILE) -> None:
    """Run the example.

    Args:
        file_name: Path of the file in LIBSVM format.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("ReadLibSVM")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        data_frame = read_libsvm(spark_session, file_name)
        data_frame.printSchema()
        data_frame.show(truncate=False)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
