from bisect import bisect_right
import calendar
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from itertools import product
from numpy import random, vstack, zeros
import pytest
from typing import Optional, Sequence

import pandas as pd
from tqdm import tqdm

from one_pass.time.conversion import (
    convert_word_to_minutes,
    get_minutes_into_word,
    MINUTES_IN_PERIOD,
)
from one_pass.options import FrequencyKey


@dataclass(frozen=True)
class _TestParams:
    num_test_days_in_year: int
    num_tests_in_day: int
    offset_minutes: bool
    years: Sequence[int]
    seed: int = 10


TEST_PARAMS = _TestParams(
    num_test_days_in_year=25,
    num_tests_in_day=1,
    offset_minutes=True,
    seed=10,
    years=list(range(1900, 2150, 3)),
)


def _get_days_in_quarter(timestamp: pd.Timestamp):
    quarter = ((timestamp.month - 1) // 3) + 1
    first_month_in_quarter = ((quarter - 1) * 3) + 1
    quarter_monthly_timestamps = (
        pd.Timestamp(year=timestamp.year, month=first_month_in_quarter + i, day=1)
        for i in range(0, 3)
    )
    return sum(month.days_in_month for month in quarter_monthly_timestamps)


def _get_days_in_year(timestamp: pd.Timestamp) -> int:
    is_leap = calendar.isleap(timestamp.year)
    return 366 if is_leap else 365


def _get_days_in_year_period(timestamp: pd.Timestamp, years: int = 1) -> int:
    count = 0
    for i in range(0, years):
        timestamp_i = pd.Timestamp(year=timestamp.year + i, month=1, day=1)
        count += _get_days_in_year(timestamp_i)  # type: ignore
    return count


def _get_data_points() -> Sequence[pd.Timestamp]:
    random.seed(TEST_PARAMS.seed)
    data_points = []
    for year in TEST_PARAMS.years:
        first_of_january = pd.Timestamp(
            year=year, month=1, day=1, hour=0, minute=0, second=0
        )
        for offset_day in random.randint(
            0,
            _get_days_in_year(first_of_january),  # type: ignore
            TEST_PARAMS.num_test_days_in_year,
        ):
            if TEST_PARAMS.offset_minutes:
                offsets = random.randint(0, 24, (TEST_PARAMS.num_tests_in_day, 3))
            else:
                offsets = random.randint(0, 24, (TEST_PARAMS.num_tests_in_day, 1))
                offsets = vstack((offsets, zeros((TEST_PARAMS.num_tests_in_day, 2))))
            for offset_hours, offset_minutes, offset_seconds in offsets:
                data_point = first_of_january + pd.DateOffset(
                    days=offset_day,
                    hours=offset_hours,
                    minutes=offset_minutes,
                    seconds=offset_seconds,
                )
                data_points.append(data_point)
    return data_points


@pytest.fixture(scope="session")
def data_points() -> Sequence[pd.Timestamp]:
    return _get_data_points()


@pytest.mark.parametrize(
    "time_word, minutes_e",
    [
        (FrequencyKey.HALF_HOURLY, 30),
        (FrequencyKey.HOURLY, 60),
        (FrequencyKey.TWO_HOURLY, 2 * 60),
        (FrequencyKey.THREE_HOURLY, 3 * 60),
        (FrequencyKey.SIX_HOURLY, 6 * 60),
        (FrequencyKey.TWELVE_HOURLY, 12 * 60),
        (FrequencyKey.DAILY, 24 * 60),
        (FrequencyKey.DAILY_NOON, 24 * 60),
        (FrequencyKey.WEEKLY, 7 * 24 * 60),
    ],
)
def test_minute_conversion_constant(
    data_points: Sequence[pd.Timestamp], time_word, minutes_e
):
    for data_point in data_points:
        minutes_o = convert_word_to_minutes(time_word, data_point)
        assert minutes_o == minutes_e


def test_minute_conversion_monthly(data_points: Sequence[pd.Timestamp]):
    for data_point in data_points:
        minutes_o = convert_word_to_minutes(FrequencyKey.MONTHLY, data_point)
        assert minutes_o == data_point.days_in_month * 24 * 60


def test_minute_conversion_three_monthly(data_points: Sequence[pd.Timestamp]):
    for data_point in data_points:
        minutes_o = convert_word_to_minutes(FrequencyKey.THREE_MONTHLY, data_point)
        assert minutes_o == _get_days_in_quarter(data_point) * 24 * 60


def test_minute_conversion_yearly(data_points):
    for data_point in data_points:
        minutes_o = convert_word_to_minutes(FrequencyKey.YEARLY, data_point)
        assert minutes_o == _get_days_in_year(data_point) * 24 * 60


def test_minute_conversion_ten_yearly(data_points):
    for data_point in data_points:
        minutes_o = convert_word_to_minutes(FrequencyKey.TEN_YEARLY, data_point)
        assert minutes_o == _get_days_in_year_period(data_point, 10) * 24 * 60


@lru_cache(maxsize=None)
def _get_generic_year(year: int):
    is_leap = calendar.isleap(year)
    first_weekday = calendar.weekday(year, 1, 1)
    for try_year in TEST_PARAMS.years:
        if calendar.weekday(try_year, 1, 1) == first_weekday and not (
            is_leap ^ calendar.isleap(try_year)
        ):
            return try_year
    return year


@lru_cache(maxsize=None)
def _get_year_start_times(year: int):
    first_of_january = pd.Timestamp(year=year, month=1, day=1)
    assert isinstance(first_of_january, pd.Timestamp)
    num_days = 366 if first_of_january.is_leap_year else 365
    start_times = defaultdict(list)
    half_hour_freq = []
    for day, hour, minute in product(range(num_days), range(24), (0, 30)):
        timestamp = first_of_january + pd.DateOffset(
            days=day, hours=hour, minutes=minute
        )
        half_hour_freq.append(timestamp)
    start_times[FrequencyKey.HALF_HOURLY] = half_hour_freq[:]
    start_times[FrequencyKey.HOURLY] = half_hour_freq[::2]
    start_times[FrequencyKey.TWO_HOURLY] = half_hour_freq[::4]
    start_times[FrequencyKey.THREE_HOURLY] = half_hour_freq[::6]
    start_times[FrequencyKey.SIX_HOURLY] = half_hour_freq[::12]
    start_times[FrequencyKey.TWELVE_HOURLY] = half_hour_freq[::24]
    daily_freq = half_hour_freq[::48]
    start_times[FrequencyKey.DAILY] = daily_freq[:]
    start_times[FrequencyKey.DAILY_NOON] = half_hour_freq[24::48]
    start_times[FrequencyKey.WEEKLY] = [
        day for day in daily_freq if day.day_of_week == 0
    ]
    start_times[FrequencyKey.MONTHLY] = [day for day in daily_freq if day.day == 1]
    start_times[FrequencyKey.THREE_MONTHLY] = [
        day for day in daily_freq if (day.day == 1 and (day.month - 1) % 3 == 0)
    ]
    start_times[FrequencyKey.YEARLY] = [first_of_january]
    return start_times


@lru_cache(maxsize=4092)
def _get_start_times(year: int, time_word: FrequencyKey):
    generic_year = _get_generic_year(year)
    generic_start_times = _get_year_start_times(generic_year)[time_word]
    start_times = [start_time.replace(year=year) for start_time in generic_start_times]
    return start_times


def _get_first_year_arg(time_word: FrequencyKey, timestamp: pd.Timestamp):
    if time_word == FrequencyKey.TEN_YEARLY:
        return timestamp.year - random.randint(0, 10)
    return None


def _get_expected_minutes_into_statistic(
    data_point: pd.Timestamp, time_word: FrequencyKey, first_year: Optional[int] = None
):
    if time_word == FrequencyKey.TEN_YEARLY:
        start_times = [pd.Timestamp(year=first_year, month=1, day=1)]
    else:
        start_times = _get_start_times(data_point.year, time_word)
    idx = bisect_right(start_times, data_point)
    if idx == 0:
        raise ValueError(
            f"Wrong data point ({data_point}) for time word '{time_word}' and "
            f"start_times used (first was {start_times[0]})."
        )
    minutes_e = (data_point - start_times[idx - 1]).total_seconds() / 60
    return minutes_e, start_times[idx - 1]


@pytest.mark.parametrize("time_word", list(MINUTES_IN_PERIOD.keys()))
def test_minutes_into(data_points: Sequence[pd.Timestamp], time_word: FrequencyKey):
    random.seed(TEST_PARAMS.seed)
    for data_point in data_points:
        first_year = _get_first_year_arg(time_word, data_point)
        try:
            minutes_e, last_start_ts = _get_expected_minutes_into_statistic(
                data_point, time_word, first_year
            )
        except ValueError as exc:
            if time_word == FrequencyKey.WEEKLY and data_point.month == 1:
                # Does not cover the situation in which we are in the first
                # week of the year but the year does not start in a Monday,
                # with weekly stat freq
                continue
            if (
                time_word == FrequencyKey.DAILY_NOON
                and data_point.month == 1
                and data_point.day == 1
                and data_point.hour < 12
            ):
                # Does not cover the situation in which we are in the first
                # 12 hours of the first of January, with daily_noon stat freq
                continue
            raise exc
        minutes_o = get_minutes_into_word(data_point, time_word, first_year=first_year)
        assert int(minutes_e) == minutes_o, (
            f"For tested timestamp {data_point} with timeword '{time_word}', "
            f"expected minutes ({int(minutes_e)}) into said timeword do not "
            f"match with result ({minutes_o}). Last starting timestamp "
            f"considered for statistic was {last_start_ts}."
        )


def summary_dataframe(data_points, fun=_get_start_times):
    random.seed(TEST_PARAMS.seed)
    cols = (
        "TIMESTAMP",
        "TIMEWORD",
        "YEARS_IN",
        "TEST_1_PASSED",
        "TEST_2_PASSED",
        "WEEKDAY",
        "MINUTES_O",
        "HOURS_O",
        "DAYS_O",
        "LAST_START_TS",
        "MINUTES_IN_O",
        "HOURS_IN_O",
        "DAYS_IN_O",
        "MINUTES_IN_E",
        "HOURS_IN_E",
        "DAYS_IN_E",
    )
    tups = []
    with tqdm(total=len(data_points) * len(MINUTES_IN_PERIOD)) as pbar:
        for data_point in data_points:
            for time_word in MINUTES_IN_PERIOD.keys():
                first_year = _get_first_year_arg(time_word, data_point)
                minutes_o = convert_word_to_minutes(time_word, data_point)
                minutes_in_o = get_minutes_into_word(
                    data_point, time_word, first_year=first_year
                )
                try:
                    minutes_in_e, last_start_ts = _get_expected_minutes_into_statistic(
                        data_point, time_word, first_year
                    )
                except ValueError as exc:
                    if time_word == FrequencyKey.WEEKLY and data_point.month == 1:
                        minutes_in_e, last_start_ts = -1, "-1"
                    elif (
                        time_word == FrequencyKey.DAILY_NOON
                        and data_point.month == 1
                        and data_point.day == 1
                    ):
                        minutes_in_e, last_start_ts = -1, "-1"
                    else:
                        raise exc
                tups.append(
                    (
                        data_point,
                        time_word.value,
                        first_year or -1,
                        "N/A",
                        int(minutes_in_e) == minutes_in_o,
                        data_point.day_of_week,
                        minutes_o,
                        minutes_o / 60,
                        minutes_o / (60 * 24),
                        last_start_ts,
                        minutes_in_o,
                        minutes_in_o / 60,
                        minutes_in_o / (60 * 24),
                        minutes_in_e,
                        minutes_in_e / 60,
                        minutes_in_e / (60 * 24),
                    )
                )
                pbar.update()
    df = pd.DataFrame.from_records(tups, columns=cols)
    return df


def write_summary_dataframe():
    data_points = _get_data_points()
    df = summary_dataframe(data_points)
    df.to_csv("dataframe.csv", float_format="%8.3f")


if __name__ == "__main__":
    write_summary_dataframe()
