"""Prepare the Wine data of the UCI repository to be used by Spark ML.

The original file (`data/wine.data`) is a CSV without header. The first column is the class of
the wine (1, 2 or 3, the grape variety) and the next 13 are chemical measurements. The Spark ML
algorithms need a numeric column "label" that starts at 0 and a column "features" with all the
measurements in a single vector. Two stages of a `Pipeline` do it:

- `StringIndexer` turns the class into the label 0.0, 1.0 or 2.0.
- `VectorAssembler` joins the 13 measurements in a vector.

The measurements have very different scales (the proline is around 1000 and the hue, around 1),
which is important for the algorithms that use distances or penalize large coefficients.
"""

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

DEFAULT_INPUT_FILE = "data/wine.data"
FEATURE_COLUMNS = [
    "alcohol",
    "malic_acid",
    "ash",
    "alcalinity_of_ash",
    "magnesium",
    "total_phenols",
    "flavanoids",
    "nonflavanoid_phenols",
    "proanthocyanins",
    "color_intensity",
    "hue",
    "od280_od315",
    "proline",
]

WINE_SCHEMA = StructType(
    [StructField("class", IntegerType())]
    + [StructField(name, DoubleType()) for name in FEATURE_COLUMNS]
)


def read_wine(spark_session: SparkSession, file_name: str) -> DataFrame:
    """Read the CSV file of Wine, which has no header.

    Args:
        spark_session: Active Spark session.
        file_name: Path of the CSV file.

    Returns:
        A DataFrame with the columns of `WINE_SCHEMA`.
    """
    # fmt: off
    return (
        spark_session.read
        .schema(WINE_SCHEMA)
        .csv(file_name)
    )
    # fmt: on


def preprocess(wine: DataFrame) -> DataFrame:
    """Convert the class into a label that starts at 0 and the measurements into a vector.

    `StringIndexer` works with strings, so the class is cast first. Numbering the values in
    alphabetical order makes the class 1 the label 0.0, the class 2 the label 1.0 and the
    class 3 the label 2.0.

    Args:
        wine: DataFrame with the columns of `WINE_SCHEMA`.

    Returns:
        A DataFrame with the columns "label" (double) and "features" (vector).
    """
    indexer = StringIndexer(inputCol="class_name", outputCol="label", stringOrderType="alphabetAsc")
    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features")
    with_class_name = wine.withColumn("class_name", wine["class"].cast("string"))

    # fmt: off
    return (
        Pipeline(stages=[indexer, assembler])
        .fit(with_class_name)
        .transform(with_class_name)
        .select("label", "features")
    )
    # fmt: on


def main(file_name: str = DEFAULT_INPUT_FILE) -> None:
    """Run the example.

    Args:
        file_name: Path of the original Wine file.
    """
    # fmt: off
    spark_session = (
        SparkSession.builder
        .appName("PreprocessWine")
        .master("local[*]")
        .getOrCreate()
    )
    # fmt: on

    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        wine = read_wine(spark_session, file_name)
        print("Original data")
        wine.show(5)

        processed_data = preprocess(wine)
        print("Preprocessed data")
        processed_data.printSchema()
        processed_data.show(5, truncate=False)
        processed_data.groupBy("label").count().orderBy("label").show()

        print("Range of some measurements")
        wine.select("hue", "magnesium", "proline").summary("min", "max").show()
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
