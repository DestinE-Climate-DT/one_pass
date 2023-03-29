import pytest

from one_pass.convert_time import convert_time


@pytest.mark.parametrize("frequency,expected", [
    ("hourly", 60),
    ("12hourly", 720)
])
def test_convert_time(frequency, expected):
    """A parametrized test (each entry in the second argument is a test case)."""
    assert convert_time(frequency) == expected

@pytest.mark.parametrize("invalid_frequency", [
    (None),
    ("Python"),
    ("")
])
def test_convert_time_invalid_frequency(invalid_frequency):
    """A test that verifies invalid frequencies raise an error."""
    with pytest.raises(ValueError, match=f".*'{invalid_frequency}' is not supported.*"):
        convert_time(invalid_frequency)


def test_convert_time_monthly_missing_time_stamp_input():
    """A simple unit test for the convert_time function, to test validation."""
    with pytest.raises(ValueError, match=".*must provide a time_stamp_input for monthly.*"):
        convert_time("monthly", None)



@pytest.mark.parametrize("days_in_month,expected", [
    (29, 41760),
    (30, 43200),
    (31, 44640)
])
def test_convert_time_monthly(mocker, days_in_month, expected):
    """Tests for the monthly conversion as it is a special case."""
    # Using a mock as otherwise we would have to create datetime
    # objects with Pandas.
    time_stamp_input = mocker.MagicMock()
    time_stamp_input.days_in_month = days_in_month
    assert convert_time("monthly", time_stamp_input=time_stamp_input) == expected
