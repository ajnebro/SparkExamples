"""Show the basic operations of Spark DataFrames using a CSV file with personal data.

The input file has a header and one row per person, e.g.:

    Name,Age,Weight,HasACar,BirthDate
    Luis,23,84.5,TRUE,2019-02-28
    Lola,42,70.2,false,2000-10-01
"""

from pyspark.sql import DataFrame, SparkSession, functions

DEFAULT_INPUT_FILE = "data/personalData.csv"
DEFAULT_OUTPUT_DIRECTORY = "output/basic_operations"
SENIOR_AGE = 45
ADULT_AGE = 30


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


def add_age_group_column(data_frame: DataFrame) -> DataFrame:
    """Add a column "AgeGroup" with the values "young", "adult" or "senior".

    A person is young below ADULT_AGE, senior above SENIOR_AGE, and adult otherwise. As there
    is no `otherwise` clause, a null age produces a null group.

    Args:
        data_frame: DataFrame with an "Age" column.

    Returns:
        The DataFrame with the new column.
    """
    age = functions.col("Age")
    age_group = (
        functions.when(age < ADULT_AGE, "young")
        .when(age <= SENIOR_AGE, "adult")
        .when(age > SENIOR_AGE, "senior")
    )
    return data_frame.withColumn("AgeGroup", age_group)


def filter_by_age_range(data_frame: DataFrame, min_age: int, max_age: int) -> DataFrame:
    """Keep the rows whose age is between two values, both included.

    Args:
        data_frame: DataFrame with an "Age" column.
        min_age: Minimum age.
        max_age: Maximum age.

    Returns:
        The DataFrame with the rows inside the range (none if min_age > max_age).
    """
    return data_frame.filter(functions.col("Age").between(min_age, max_age))


def mean_weight_by_age_group(data_frame: DataFrame) -> DataFrame:
    """Compute the number of people and their mean weight for each age group.

    Args:
        data_frame: DataFrame with "Age" and "Weight" columns.

    Returns:
        A DataFrame with the columns "AgeGroup", "People" and "MeanWeight", sorted by group.
    """
    return (
        add_age_group_column(data_frame)
        .groupBy("AgeGroup")
        .agg(
            functions.count("*").alias("People"),
            functions.round(functions.avg("Weight"), 2).alias("MeanWeight"),
        )
        .orderBy("AgeGroup")
    )


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


def mean_age(data_frame: DataFrame) -> float | None:
    """Compute the mean age using the DataFrame API.

    Args:
        data_frame: DataFrame with an "Age" column.

    Returns:
        The mean age, or None if the DataFrame is empty (`avg` of no values is null).
    """
    return data_frame.agg(functions.avg("Age")).first()[0]


def mean_age_with_rdd(data_frame: DataFrame) -> float:
    """Compute the mean age using the RDD API.

    Args:
        data_frame: DataFrame with an "Age" column.

    Returns:
        The mean age.

    Raises:
        ValueError: If the DataFrame is empty (Spark cannot `reduce` an empty RDD).
    """
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
    add_age_group_column(data_frame).show()
    data_frame.withColumnRenamed("HasACar", "Owner").show()
    data_frame.drop("BirthDate").show()


def show_filtering(data_frame: DataFrame) -> None:
    """Print the rows selected with several kinds of filter conditions.

    Args:
        data_frame: DataFrame with the personal data.
    """
    data_frame.filter(functions.col("Age") > SENIOR_AGE).show()
    # `where` is an alias of `filter`, and it also accepts a SQL expression
    data_frame.where(f"Age > {SENIOR_AGE} AND HasACar").show()
    # Combined conditions: each one goes in parentheses, using & (and), | (or) and ~ (not)
    data_frame.filter((functions.col("Weight") > 80) | ~functions.col("HasACar")).show()
    data_frame.filter(functions.col("Name").isin("Luis", "Lola")).show()
    filter_by_age_range(data_frame, 40, 66).show()


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
    mean_weight_by_age_group(data_frame).show()


def write_data_frame(data_frame: DataFrame, output_directory: str) -> None:
    """Write a DataFrame in JSON and CSV formats, replacing any previous output.

    Args:
        data_frame: DataFrame to write.
        output_directory: Directory where the "json" and "csv" subdirectories are created.
    """
    data_frame.write.mode("overwrite").json(f"{output_directory}/json")
    data_frame.write.mode("overwrite").option("header", "true").csv(f"{output_directory}/csv")


def main(
    file_name: str = DEFAULT_INPUT_FILE, output_directory: str = DEFAULT_OUTPUT_DIRECTORY
) -> None:
    """Run the example.

    Args:
        file_name: Path of the CSV file with the personal data.
        output_directory: Directory where the data is written in JSON and CSV formats.
    """
    spark_session = (
        SparkSession.builder.appName("DataFrameBasicOperations").master("local[*]").getOrCreate()
    )
    try:
        spark_session.sparkContext.setLogLevel("ERROR")
        # The DataFrame is used by many actions, so it is kept in memory
        data_frame = read_personal_data(spark_session, file_name).persist()
        show_inspection(data_frame)
        show_column_operations(data_frame)
        show_filtering(data_frame)
        show_sorting(data_frame)
        show_aggregations(data_frame)
        write_data_frame(data_frame, output_directory)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()
