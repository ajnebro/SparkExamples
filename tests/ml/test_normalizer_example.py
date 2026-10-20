"""Tests for ml.normalizer_example."""

import pytest
from pyspark.errors import PySparkException
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession

from ml.normalizer_example import SAMPLE_VECTORS, normalize


def normalized(vectors: list[list[float]], p: float, spark_session: SparkSession) -> list[list]:
    """Normalize some vectors and return the result as lists of numbers."""
    data = [(index, Vectors.dense(vector)) for index, vector in enumerate(vectors)]
    data_frame = spark_session.createDataFrame(data, ["id", "features"])
    rows = normalize(data_frame, p).orderBy("id").collect()

    return [row["normalized_features"].toArray().tolist() for row in rows]


class TestNormalize:
    @pytest.mark.parametrize(
        ("vector", "p", "expected"),
        [
            ([1.0, 0.5, -1.0], 1.0, [0.4, 0.2, -0.4]),
            ([1.0, 0.5, -1.0], float("inf"), [1.0, 0.5, -1.0]),
            ([4.0, 10.0, 2.0], float("inf"), [0.4, 1.0, 0.2]),
            ([3.0, 4.0], 2.0, [0.6, 0.8]),
            ([0.0, 0.0], 1.0, [0.0, 0.0]),
        ],
    )
    def test_should_divide_each_vector_by_its_norm(
        self, spark_session: SparkSession, vector: list[float], p: float, expected: list[float]
    ) -> None:
        """Each vector is divided by its p-norm, and a zero vector is left as it is."""
        assert normalized([vector], p, spark_session)[0] == pytest.approx(expected)

    def test_should_normalize_each_row_on_its_own(self, spark_session: SparkSession) -> None:
        """The norm of a row does not depend on the other rows."""
        result = normalized([[1.0, 1.0], [10.0, 30.0]], 1.0, spark_session)

        assert result[0] == pytest.approx([0.5, 0.5])
        assert result[1] == pytest.approx([0.25, 0.75])

    def test_should_keep_the_original_columns(self, spark_session: SparkSession) -> None:
        """The result has the original columns and the new one."""
        data_frame = spark_session.createDataFrame(SAMPLE_VECTORS, ["id", "features"])

        assert normalize(data_frame, 1.0).columns == ["id", "features", "normalized_features"]

    def test_should_raise_error_if_p_is_smaller_than_one(self, spark_session: SparkSession) -> None:
        """A p-norm is only defined for p greater than or equal to 1."""
        data_frame = spark_session.createDataFrame(SAMPLE_VECTORS, ["id", "features"])

        with pytest.raises(PySparkException):
            normalize(data_frame, 0.5).collect()
