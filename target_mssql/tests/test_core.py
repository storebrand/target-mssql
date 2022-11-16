""" Attempt at making some standard Target Tests. """
# flake8: noqa
import io
from contextlib import redirect_stdout
from pathlib import Path

import pytest
from singer_sdk.testing import sync_end_to_end

from target_mssql.target import Targetmssql
from target_mssql.tests.samples.aapl.aapl import Fundamentals
from target_mssql.tests.samples.sample_tap_countries.countries_tap import (
    SampleTapCountries,
)


@pytest.fixture()
def mssql_config():
    return {
        "sqlalchemy_url": "mssql+pymssql://sa:p@55w0rd@localhost:1433/master",
        "schema": "dbo",
        "user": "sa",
        "password": "P@55w0rd",
        "host": "localhost",
        "port": "1433",
        "database": "master",
    }


@pytest.fixture
def mssql_target(mssql_config) -> Targetmssql:
    return Targetmssql(config=mssql_config)


def singer_file_to_target(file_name, target) -> None:
    """Singer file to Target, emulates a tap run
    Equivalent to running cat file_path | target-name --config config.json.
    Note that this function loads all lines into memory, so it is
    not good very large files.
    Args:
        file_name: name to file in .tests/data_files to be sent into target
        Target: Target to pass data from file_path into..
    """
    file_path = Path(__file__).parent / Path("./data_files") / Path(file_name)
    buf = io.StringIO()
    with redirect_stdout(buf):
        with open(file_path, "r") as f:
            for line in f:
                print(line.rstrip("\r\n"))  # File endings are here,
                # and print adds another line ending so we need to remove one.
    buf.seek(0)
    target.listen(buf)


# TODO should set schemas for each tap individually so we don't collide
# Test name would work well
@pytest.mark.skip(
    reason="TODO: Something with identity, doesn't make sense. external API, skipping"
)
def test_countries_to_mssql(mssql_config):
    tap = SampleTapCountries(config={}, state=None)
    target = Targetmssql(config=mssql_config)
    sync_end_to_end(tap, target)


def test_aapl_to_mssql(mssql_config):
    tap = Fundamentals(config={}, state=None)
    target = Targetmssql(config=mssql_config)
    sync_end_to_end(tap, target)


# TODO this test should throw an exception
def test_record_before_schema(mssql_target):
    with pytest.raises(Exception) as e_info:
        file_name = "record_before_schema.singer"
        singer_file_to_target(file_name, mssql_target)


# TODO this test should throw an exception
def test_invalid_schema(mssql_target):
    with pytest.raises(Exception) as e_info:
        file_name = "invalid_schema.singer"
        singer_file_to_target(file_name, mssql_target)


# TODO this test should throw an exception
def test_record_missing_key_property(mssql_target):
    with pytest.raises(Exception) as e_info:
        file_name = "record_missing_key_property.singer"
        singer_file_to_target(file_name, mssql_target)


# TODO this test should throw an exception
def test_record_missing_required_property(mssql_target):
    with pytest.raises(Exception) as e_info:
        file_name = "record_missing_required_property.singer"
        singer_file_to_target(file_name, mssql_target)


# TODO test that data is correctly set
# see target-sqllit/tests/test_target_sqllite.py
@pytest.mark.skip(reason="Waiting for SDK to handle this")
def test_column_camel_case(mssql_target):
    file_name = "camelcase.singer"
    singer_file_to_target(file_name, mssql_target)


# TODO test that data is correctly set
@pytest.mark.skip(reason="Waiting for SDK to handle this")
def test_special_chars_in_attributes(mssql_target):
    file_name = "special_chars_in_attributes.singer"
    singer_file_to_target(file_name, mssql_target)


# TODO test that data is correctly set
def test_optional_attributes(mssql_target):
    file_name = "optional_attributes.singer"
    singer_file_to_target(file_name, mssql_target)


# Test that schema without properties (no columns) fails
def test_schema_no_properties(mssql_target):
    with pytest.raises(Exception) as e_info:
        file_name = "schema_no_properties.singer"
        singer_file_to_target(file_name, mssql_target)


# TODO test that data is correct
def test_schema_updates(mssql_target):
    file_name = "schema_updates.singer"
    singer_file_to_target(file_name, mssql_target)


# TODO test that data is correct
def test_multiple_state_messages(mssql_target):
    file_name = "multiple_state_messages.singer"
    singer_file_to_target(file_name, mssql_target)


# TODO test that data is correct
@pytest.mark.skip(reason="TODO")
def test_relational_data(mssql_target):
    file_name = "user_location_data.singer"
    singer_file_to_target(file_name, mssql_target)

    file_name = "user_location_upsert_data.singer"
    singer_file_to_target(file_name, mssql_target)


# TODO test that data is correct
def test_no_primary_keys(mssql_target):
    file_name = "no_primary_keys.singer"
    singer_file_to_target(file_name, mssql_target)

    file_name = "no_primary_keys_append.singer"
    singer_file_to_target(file_name, mssql_target)


# TODO test that data is correct
def test_duplicate_records(mssql_target):
    with pytest.raises(Exception) as e_info:
        file_name = "duplicate_records.singer"
        singer_file_to_target(file_name, mssql_target)


def test_array_data(mssql_target):
    file_name = "array_data.singer"
    singer_file_to_target(file_name, mssql_target)


@pytest.mark.skip(reason="TODO")
def test_encoded_string_data(mssql_target):
    file_name = "encoded_strings.singer"
    singer_file_to_target(file_name, mssql_target)


def test_tap_appl(mssql_target):
    file_name = "tap_aapl.singer"
    singer_file_to_target(file_name, mssql_target)


@pytest.mark.skip(reason="TODO")
def test_tap_countries(mssql_target):
    file_name = "tap_countries.singer"
    singer_file_to_target(file_name, mssql_target)


def test_missing_value(mssql_target):
    file_name = "missing_value.singer"
    singer_file_to_target(file_name, mssql_target)


@pytest.mark.skip(reason="TODO")
def test_large_int(mssql_target):
    file_name = "large_int.singer"
    singer_file_to_target(file_name, mssql_target)
