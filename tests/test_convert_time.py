import os
import sys
from unittest.mock import MagicMock

import pandas as pd
import pytest

from one_pass.time.conversion import convert_time


@pytest.mark.parametrize(
    "frequency, timestamp, expected",
    [
        ("hourly", pd.Timestamp("2017-01-01T12"), 60),
        ("12hourly", pd.Timestamp("2017-01-01T12"), 720),
    ],
)
def test_convert_time(frequency, timestamp, expected):
    """A parametrized test (each entry in the second argument is a test case)."""
    assert convert_time(frequency, timestamp)[0] == expected


@pytest.mark.parametrize(
    "invalid_frequency, timestamp",
    [
        (None, pd.Timestamp("2017-01-01T12")),
        ("Python", pd.Timestamp("2017-01-01T12")),
        ("", pd.Timestamp("2017-01-01T12")),
    ],
)
def test_convert_time_invalid_frequency(invalid_frequency, timestamp):
    """A test that verifies invalid frequencies raise an error."""
    with pytest.raises(ValueError, match=f".*'{invalid_frequency}' is not supported.*"):
        convert_time(invalid_frequency, timestamp)


@pytest.mark.parametrize(
    "days_in_month, timestamp, expected",
    [
        (28, pd.Timestamp("2019-02-01T12"), 40320),
        (29, pd.Timestamp("2020-02-01T12"), 41760),
        (30, pd.Timestamp("2017-11-01T12"), 43200),
        (31, pd.Timestamp("2017-01-01T12"), 44640),
    ],
)
def test_convert_time_monthly(days_in_month, timestamp, expected):
    """Tests for the monthly conversion as it is a special case."""
    # Using a mock as otherwise we would have to create datetime
    # objects with Pandas.
    # timestamp_input = MagicMock()
    assert timestamp.days_in_month == days_in_month
    assert convert_time("monthly", timestamp=timestamp)[0] == expected
