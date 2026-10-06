"""Tests for spark_apps.session."""

import pytest

from spark_apps.session import create_spark_session


class TestCreateSparkSession:
    def test_should_create_session_with_given_app_name(self) -> None:
        """Given an app name, when creating a session, then it uses that name."""
        spark = create_spark_session("test-app")

        try:
            assert spark.sparkContext.appName == "test-app"
        finally:
            spark.stop()

    def test_should_raise_error_on_empty_app_name(self) -> None:
        """Given an empty app name, when creating a session, then ValueError is raised."""
        with pytest.raises(ValueError):
            create_spark_session("")
