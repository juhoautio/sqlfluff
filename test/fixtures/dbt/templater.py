"""Fixtures for dbt templating tests."""

from sqlfluff.core.templaters import DbtTemplater
import pytest
import os


DBT_FLUFF_CONFIG = {
    "core": {
        "templater": "dbt",
        "dialect": "postgres",
    },
    "templater": {
        "dbt": {
            "profiles_dir": "test/fixtures/dbt",
            "project_dir":"test/fixtures/dbt_project"
        },
    },
}


@pytest.fixture()
def dbt_templater():
    """Returns an instance of the DbtTemplater."""
    return DbtTemplater()

