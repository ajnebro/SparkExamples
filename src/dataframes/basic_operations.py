"""Show the basic operations of Spark DataFrames using a CSV file with personal data.

The input file has a header and one row per person, e.g.:

    Name,Age,Weight,HasACar,BirthDate
    Luis,23,84.5,TRUE,2019-02-28
    Lola,42,70.2,false,2000-10-01
"""

import argparse
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession, functions

DEFAULT_INPUT_FILE = "data/personalData.csv"
DEFAULT_OUTPUT_DIRECTORY = "output/basic_operations"
SENIOR_AGE = 45


def read_personal_data(spark_session: SparkSession, file_name: str) -> DataFrame:
    """Read a CSV file with a header, inferring the type of each column.

    Args:
        spark_session: Active Spark session.
        file_name: Path of the CSV file.

    Returns:
        A DataFrame with the content of the file.
    """
    return (
        spark_session.read.option("header", "true")
        .option("inferSchema", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(file_name)
    )


def add_senior_column(data_frame: DataFrame, age_threshold: int = SENIOR_AGE) -> DataFrame:
    """Add a boolean column "Senior" that is true when the age is greater than a threshold.

    Args:
        data_frame: DataFrame with an "Age" column.
        age_threshold: Age above which a person is considered senior.

    Returns:
        The DataFrame with the new column.
    """
    return data_frame.withColumn("Senior", functions.col("Age") > age_threshold)


def sort_by_age_and_weight(data_frame: DataFrame) -> DataFrame:
    """Sort the rows by age (descending) and, for equal ages, by weight (ascending).

    Args:
        data_frame: DataFrame with "Age" and "Weight" columns.

    Returns:
        The sorted DataFrame.
    """
    return data_frame.orderBy(functions.col("Age").desc(), functions.col("Weight").asc())


def count_by_car_ownership(data_frame: DataFrame) -> DataFrame:
    """Count the number of people that have and do not have a car.

    Args:
        data_frame: DataFrame with a "HasACar" column.

    Returns:
        A DataFrame with the columns "HasACar" and "count".
    """
    return data_frame.groupBy("HasACar").count()


def sum_of_weights(data_frame: DataFrame) -> float:
    """Sum the weights using the DataFrame API.

    Args:
        data_frame: DataFrame with a "Weight" column.

    Returns:
        The sum of the weights (0.0 if the DataFrame is empty).
    """
    total = data_frame.agg(functions.sum("Weight")).first()[0]
    return total if total is not None else 0.0


def sum_of_weights_with_rdd(data_frame: DataFrame) -> float:
    """Sum the weights using the RDD API.

    Args:
        data_frame: DataFrame with a "Weight" column.

    Returns:
        The sum of the weights (0.0 if the DataFrame is empty).
    """
    return data_frame.rdd.map(lambda row: row["Weight"]).fold(0.0, lambda a, b: a + b)


def mean_age(data_frame: DataFrame) -> float:
    """Compute the mean age using the DataFrame API.

    Args:
        data_frame: DataFrame with an "Age" column.

    Returns:
        The mean age.

    Raises:
        ValueError: If the DataFrame is empty.
    """
    if data_frame.isEmpty():
        raise ValueError("Cannot compute the mean age of an empty DataFrame")
    return data_frame.agg(functions.avg("Age")).first()[0]


def mean_age_with_rdd(data_frame: DataFrame) -> float:
    """Compute the mean age using the RDD API.

    Args:
        data_frame: DataFrame with an "Age" column.

    Returns:
        The mean age.

    Raises:
        ValueError: If the DataFrame is empty.
    """
    if data_frame.isEmpty():
        raise ValueError("Cannot compute the mean age of an empty DataFrame")
    ages = data_frame.rdd.map(lambda row: row["Age"])
    return ages.reduce(lambda a, b: a + b) / ages.count()


def show_inspection(data_frame: DataFrame) -> None:
    """Print the schema, content, data types, statistics and plan of a DataFrame.

    Args:
        data_frame: DataFrame to inspect.
    """
    data_frame.printSchema()
    data_frame.show()
    print(f"Data types: {data_frame.dtypes}")
    data_frame.describe().show()
    data_frame.explain()


def show_column_operations(data_frame: DataFrame) -> None:
    """Print the result of selecting, adding, renaming and dropping columns.

    Args:
        data_frame: DataFrame with the personal data.
    """
    data_frame.select("Name", "Age").show()
    data_frame.select("Name", functions.col("Age") + 1).show()
    data_frame.select("Name", functions.length("Name") > 4).show()
    data_frame.select("Name", functions.col("Name").startswith("L")).show()
    add_senior_column(data_frame).show()
    data_frame.withColumnRenamed("HasACar", "Owner").show()
    data_frame.drop("BirthDate").show()


def show_sorting(data_frame: DataFrame) -> None:
    """Print the DataFrame sorted in several equivalent and alternative ways.

    Args:
        data_frame: DataFrame with "Age" and "Weight" columns.
    """
    data_frame.sort(functions.col("Age").desc()).show()
    data_frame.sort("Age", ascending=False).show()
    sort_by_age_and_weight(data_frame).show()


def show_aggregations(data_frame: DataFrame) -> None:
    """Print aggregations computed with the RDD API and with the DataFrame API.

    Args:
        data_frame: DataFrame with the personal data.
    """
    for row in data_frame.rdd.collect():
        print(row)
    print(f"Sum of weights (RDD): {sum_of_weights_with_rdd(data_frame)}")
    print(f"Sum of weights (DataFrame): {sum_of_weights(data_frame)}")
    data_frame.agg({"Weight": "sum", "Age": "min"}).show()
    print(f"Mean age (RDD): {mean_age_with_rdd(data_frame)}")
    print(f"Mean age (DataFrame): {mean_age(data_frame)}")
    data_frame.select(functions.avg("Age").alias("Average")).show()
    count_by_car_ownership(data_frame).show()


def write_data_frame(data_frame: DataFrame, output_directory: str) -> None:
    """Write a DataFrame in JSON and CSV formats, replacing any previous output.

    Args:
        data_frame: DataFrame to write.
        output_directory: Directory where the "json" and "csv" subdirectories are created.
    """
    data_frame.write.mode("overwrite").json(f"{output_directory}/json")
    data_frame.write.mode("overwrite").option("header", "true").csv(f"{output_directory}/csv")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line arguments.

    Args:
        argv: Arguments to parse. `sys.argv[1:]` is used if None.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_file", nargs="?", default=DEFAULT_INPUT_FILE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_DIRECTORY)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Run the example.

    Args:
        argv: Command line arguments. `sys.argv[1:]` is used if None.
    """
    args = parse_args(argv)
    spark_session = (
        SparkSession.builder.appName("DataFrameBasicOperations").master("local[*]").getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        # The DataFrame is used by many actions, so it is kept in memory
        data_frame = read_personal_data(spark_session, args.input_file).persist()
        show_inspection(data_frame)
        show_column_operations(data_frame)
        show_sorting(data_frame)
        show_aggregations(data_frame)
        write_data_frame(data_frame, args.output)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
