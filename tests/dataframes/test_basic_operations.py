"""Tests for dataframes.basic_operations."""

from collections.abc import Callable
from datetime import date
from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from dataframes.basic_operations import (
    add_age_group_column,
    add_senior_column,
    count_by_car_ownership,
    filter_by_age_range,
    mean_age,
    mean_age_with_rdd,
    mean_weight_by_age_group,
    parse_args,
    read_personal_data,
    sort_by_age_and_weight,
    sum_of_weights,
    sum_of_weights_with_rdd,
    write_data_frame,
)

SCHEMA = "Name STRING, Age INT, Weight DOUBLE, HasACar BOOLEAN"
Aggregation = Callable[[DataFrame], float]
PEOPLE = [
    ("Luis", 23, 84.5, True),
    ("Lola", 42, 70.2, False),
    ("Paco", 66, 90.1, False),
    ("Ana", 66, 60.0, True),
]


@pytest.fixture
def people(spark_session: SparkSession) -> DataFrame:
    """Provide a small DataFrame with personal data."""
    return spark_session.createDataFrame(PEOPLE, SCHEMA)


@pytest.fixture
def no_people(spark_session: SparkSession) -> DataFrame:
    """Provide an empty DataFrame with the personal data schema."""
    return spark_session.createDataFrame([], SCHEMA)


class TestReadPersonalData:
    def test_should_infer_the_type_of_each_column(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Integers, doubles, booleans in any case and dates are inferred from the CSV."""
        csv_file = tmp_path / "people.csv"
        csv_file.write_text("Name,Age,Weight,HasACar,BirthDate\nLuis,23,84.5,TRUE,2019-02-28\n")

        result = read_personal_data(spark_session, str(csv_file))

        assert result.dtypes == [
            ("Name", "string"),
            ("Age", "int"),
            ("Weight", "double"),
            ("HasACar", "boolean"),
            ("BirthDate", "date"),
        ]
        assert result.first()["BirthDate"] == date(2019, 2, 28)

    def test_should_raise_error_when_file_does_not_exist(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Given a missing file, Spark raises an AnalysisException."""
        with pytest.raises(AnalysisException):
            read_personal_data(spark_session, str(tmp_path / "missing.csv"))


class TestAddSeniorColumn:
    @pytest.mark.parametrize(
        ("age_threshold", "expected"),
        [(45, [False, False, True, True]), (23, [False, True, True, True]), (70, [False] * 4)],
    )
    def test_should_mark_people_older_than_threshold(
        self, people: DataFrame, age_threshold: int, expected: list[bool]
    ) -> None:
        """A person is senior only when the age is strictly greater than the threshold."""
        result = add_senior_column(people, age_threshold)

        assert [row["Senior"] for row in result.collect()] == expected


class TestAddAgeGroupColumn:
    @pytest.mark.parametrize(
        ("age", "expected"),
        [(29, "young"), (30, "adult"), (45, "adult"), (46, "senior"), (None, None)],
    )
    def test_should_assign_the_group_of_each_age(
        self, spark_session: SparkSession, age: int | None, expected: str | None
    ) -> None:
        """The limits of each group are included as documented, and a null age has no group."""
        data_frame = spark_session.createDataFrame([("X", age, 70.0, True)], SCHEMA)

        result = add_age_group_column(data_frame).first()["AgeGroup"]

        assert result == expected


class TestFilterByAgeRange:
    @pytest.mark.parametrize(
        ("min_age", "max_age", "expected"),
        [(40, 66, ["Lola", "Paco", "Ana"]), (23, 23, ["Luis"]), (67, 90, [])],
    )
    def test_should_keep_rows_inside_the_range(
        self, people: DataFrame, min_age: int, max_age: int, expected: list[str]
    ) -> None:
        """Both limits of the range are included."""
        result = filter_by_age_range(people, min_age, max_age)

        assert [row["Name"] for row in result.collect()] == expected

    def test_should_raise_error_when_range_is_inverted(self, people: DataFrame) -> None:
        """Given a minimum age greater than the maximum, the range is rejected."""
        with pytest.raises(ValueError, match="greater than max_age"):
            filter_by_age_range(people, 50, 40)


class TestMeanWeightByAgeGroup:
    def test_should_count_people_and_average_weight_per_group(self, people: DataFrame) -> None:
        """Each group has its number of people and its mean weight rounded to two decimals."""
        result = mean_weight_by_age_group(people)

        assert [tuple(row) for row in result.collect()] == [
            ("adult", 1, 70.2),
            ("senior", 2, 75.05),
            ("young", 1, 84.5),
        ]


class TestSortByAgeAndWeight:
    def test_should_sort_by_age_descending_and_weight_ascending(self, people: DataFrame) -> None:
        """Older people come first; equal ages are ordered by increasing weight."""
        result = sort_by_age_and_weight(people)

        assert [row["Name"] for row in result.collect()] == ["Ana", "Paco", "Lola", "Luis"]


class TestCountByCarOwnership:
    def test_should_count_owners_and_non_owners(self, people: DataFrame) -> None:
        """Each value of HasACar is counted once per person."""
        result = count_by_car_ownership(people)

        assert {row["HasACar"]: row["count"] for row in result.collect()} == {True: 2, False: 2}


class TestSumOfWeights:
    @pytest.mark.parametrize("sum_function", [sum_of_weights, sum_of_weights_with_rdd])
    def test_should_add_all_the_weights(self, people: DataFrame, sum_function: Aggregation) -> None:
        """The DataFrame and RDD versions return the same sum."""
        result = sum_function(people)

        assert result == pytest.approx(304.8)

    @pytest.mark.parametrize("sum_function", [sum_of_weights, sum_of_weights_with_rdd])
    def test_should_return_zero_on_empty_data_frame(
        self, no_people: DataFrame, sum_function: Aggregation
    ) -> None:
        """Given no rows, the sum is 0.0."""
        result = sum_function(no_people)

        assert result == 0.0


class TestMeanAge:
    @pytest.mark.parametrize("mean_function", [mean_age, mean_age_with_rdd])
    def test_should_compute_the_mean_age(
        self, people: DataFrame, mean_function: Aggregation
    ) -> None:
        """The DataFrame and RDD versions return the same mean."""
        result = mean_function(people)

        assert result == pytest.approx(49.25)

    @pytest.mark.parametrize("mean_function", [mean_age, mean_age_with_rdd])
    def test_should_raise_error_on_empty_data_frame(
        self, no_people: DataFrame, mean_function: Aggregation
    ) -> None:
        """Given no rows, the mean is undefined."""
        with pytest.raises(ValueError, match="empty"):
            mean_function(no_people)


class TestWriteDataFrame:
    def test_should_write_json_and_csv_that_can_be_read_back(
        self, spark_session: SparkSession, people: DataFrame, tmp_path: Path
    ) -> None:
        """Both outputs contain all the rows of the DataFrame."""
        write_data_frame(people, str(tmp_path))

        json_rows = spark_session.read.json(str(tmp_path / "json")).count()
        csv_rows = spark_session.read.option("header", "true").csv(str(tmp_path / "csv")).count()
        assert (json_rows, csv_rows) == (4, 4)


class TestParseArgs:
    def test_should_use_defaults_when_no_arguments_are_given(self) -> None:
        """Given no arguments, the default input file and output directory are used."""
        args = parse_args([])

        assert (args.input_file, args.output) == (
            "data/personalData.csv",
            "output/basic_operations",
        )

    def test_should_accept_custom_input_file_and_output(self) -> None:
        """Given arguments, they override the defaults."""
        args = parse_args(["people.csv", "--output", "out"])

        assert (args.input_file, args.output) == ("people.csv", "out")

    def test_should_exit_on_unknown_option(self) -> None:
        """Given an unknown option, the parser exits with an error."""
        with pytest.raises(SystemExit):
            parse_args(["--unknown"])
