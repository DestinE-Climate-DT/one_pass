"""Functions to convert time for the one pass algorithms.

MINUTES_IN_PERIOD is a mapping between time words (and time stamps) and number of
minutes, to avoid having multiple if statements. Converts to
minutes expect for monthly and yearly where you need to check the number
of days in a month. This is done below
"""

from functools import lru_cache
from typing import Optional, Tuple

import pandas as pd

from one_pass.options import FrequencyKey


MINUTES_IN_PERIOD = {
    FrequencyKey.HALF_HOURLY: 30,
    FrequencyKey.HOURLY: 60,
    FrequencyKey.TWO_HOURLY: 2 * 60,
    FrequencyKey.THREE_HOURLY: 3 * 60,
    FrequencyKey.SIX_HOURLY: 6 * 60,
    FrequencyKey.TWELVE_HOURLY: 12 * 60,
    FrequencyKey.DAILY: 24 * 60,
    FrequencyKey.DAILY_NOON: 24 * 60,
    FrequencyKey.WEEKLY: 7 * 24 * 60,
    FrequencyKey.MONTHLY: None,  # Here and below computed differently
    FrequencyKey.THREE_MONTHLY: None,
    FrequencyKey.YEARLY: None,
    FrequencyKey.TEN_YEARLY: None,
}


def _get_num_days(time_word: FrequencyKey, timestamp: pd.Timestamp) -> int:
    if time_word == FrequencyKey.MONTHLY:
        return timestamp.days_in_month
    if time_word == FrequencyKey.THREE_MONTHLY:
        quarter = timestamp.quarter
        if quarter == 1:
            # January, February, March
            return (31 + 29 + 31) if timestamp.is_leap_year else (31 + 28 + 31)
        if quarter == 2:
            # April, May, June
            return 30 + 31 + 30
        # July-September or October-December
        return 31 + 31 + 30
    if time_word == FrequencyKey.YEARLY:
        return 366 if timestamp.is_leap_year else 365
    if time_word == FrequencyKey.TEN_YEARLY:
        return sum(
            _get_num_days(
                FrequencyKey.YEARLY, timestamp.replace(year=timestamp.year + i, day=1)
            )
            for i in range(10)
        )
    raise RuntimeError(f"Unexpected time_word: {time_word}.")


@lru_cache
def convert_word_to_minutes(time_word: FrequencyKey, timestamp: pd.Timestamp) -> int:
    """Converts word, either the stat_freq or output_freq inputs into
    number of minutes

    Arguments
    ---------
    time_word : str. this will be the time word that does not have a
            set number of minutes. e.g. monthly, 3monthly,
            yearly or 10yearly, due to different numbers of
            days in months and years.
    timestamp : pd.Timestamp. timestamp is the pandas timestamp
            of the current incoming data

    Returns
    ---------
    stat_freq_min : the total number of minutes corresponding
            to the given 'time_word' and current time stamp
    """
    # converts word to minutes
    minutes_in_period = MINUTES_IN_PERIOD[time_word]
    if minutes_in_period is None:
        return 24 * 60 * _get_num_days(time_word, timestamp)
    return minutes_in_period


@lru_cache
def get_minutes_into_word(  # pylint: disable=R0911,R0912
    timestamp: pd.Timestamp, time_word: str, first_year: Optional[int] = None
) -> int:
    """Calculates the time in minutes that the current time stamp
    is into the period corresponding to the selected time word.

    Arguments
    -----------
    timestamp : pd.Timestamp of the current incoming data slice
    time_word : str. String corresponding to the selected frequency request
            of the data. e.g. 'daily'.
    first year : int, optional. First year (only necessary if time_word is 10yearly)

    Returns
    ----------
    timestamp_min : int. The number of minutes that the incoming
            timestamp is into the selected time_word.
    """
    ts = timestamp
    # based on the incoming word (or stat freq will convert the time
    # stamp into the number of minutes INTO that freq)
    if time_word == FrequencyKey.HALF_HOURLY:
        return ts.minute % 30
    if time_word == FrequencyKey.HOURLY:
        return ts.minute
    if time_word == FrequencyKey.TWO_HOURLY:
        return (ts.hour % 2) * 60 + ts.minute
    if time_word == FrequencyKey.THREE_HOURLY:
        return (ts.hour % 3) * 60 + ts.minute
    if time_word == FrequencyKey.SIX_HOURLY:
        return (ts.hour % 6) * 60 + ts.minute
    if time_word == FrequencyKey.TWELVE_HOURLY:
        return (ts.hour % 12) * 60 + ts.minute
    if time_word == FrequencyKey.DAILY:
        return ts.hour * 60 + ts.minute
    if time_word == FrequencyKey.DAILY_NOON:
        return ((ts.hour - 12) % 24) * 60 + ts.minute
    if time_word == FrequencyKey.WEEKLY:
        return ts.day_of_week * 24 * 60 + ts.hour * 60 + ts.minute
    if time_word == FrequencyKey.MONTHLY:
        return (ts.day - 1) * 24 * 60 + ts.hour * 60 + ts.minute
    if time_word == FrequencyKey.THREE_MONTHLY:
        # Jan: 0, Feb: 1, Mar: 2, Apr: 0, May: 1, etc.
        full_months_in_quarter = (ts.month - 1) % 3
        # Jan: 0, Feb: 31, Mar: 31 + 28 (or 31 + 29), Apr: 0, May: 30, etc.
        days_in_quarter_full_months = sum(
            ts.replace(month=(ts.month - i), day=1).days_in_month
            for i in range(1, full_months_in_quarter + 1)
        )
        return (
            (days_in_quarter_full_months + ts.day - 1) * 24 * 60
            + ts.hour * 60
            + ts.minute
        )
    if time_word == FrequencyKey.YEARLY:
        return (ts.day_of_year - 1) * 24 * 60 + ts.hour * 60 + ts.minute
    if time_word == FrequencyKey.TEN_YEARLY:
        if first_year is None:
            raise ValueError("first_year needed to compute TEN_YEARLY statistic.")
        if (ts.year - first_year) > 10:
            raise ValueError("TEN_YEARLY statistic has spanned more than 10 years.")
        if first_year > ts.year:
            raise ValueError(
                "first_year is larger than current year for TEN_YEARLY statistic."
            )
        days_in_previous_years = sum(
            _get_num_days(FrequencyKey.YEARLY, ts.replace(year=year, day=1))
            for year in range(first_year, ts.year)
        )
        return (
            (days_in_previous_years + ts.day_of_year - 1) * 24 * 60
            + ts.hour * 60
            + ts.minute
        )
    raise ValueError(f"Bad value for time_word: '{time_word}'.")


@lru_cache
def convert_time(
    time_word: FrequencyKey, timestamp: pd.Timestamp, first_year: Optional[int] = None
) -> Tuple[int, int]:
    """Function to convert current time stamp and requested frequency
    of the statistic into two outputs

    Arguments
    ---------
    time_word : FrequencyKey
        a word specifiying a frequency for the statistic
    timestamp : pd.Timestamp
        Timestamp of the current data
    first_year : int, optional
        First year (only necessary if time_word is 10yearly)

    Returns
    ---------
    int:
        the total number of minutes of the time_word (usually) stat_freq
        but in the case of continuous data, output_freq
    int:
        the total number of minutes of the incoming timestamp
        since the start of the given time word frequency
    """

    if time_word not in MINUTES_IN_PERIOD:
        valid_values = ", ".join(MINUTES_IN_PERIOD.keys())
        raise ValueError(
            f"The input saving frequency '{time_word}' is not supported, "
            f"valid values are: {valid_values}"
        )
    return (
        convert_word_to_minutes(time_word, timestamp),
        get_minutes_into_word(timestamp, time_word, first_year),
    )


def convert_time_append(
    stat_freq: FrequencyKey,
    output_freq: FrequencyKey,
    timestamp: pd.Timestamp,
    first_year: Optional[int] = None,
) -> int:
    """Calculates the value timestamp_tot_append. This is only relevant if
    output_freq > stat_freq and you want multiple statistcs appended in one
    xr.Dataset. It gives you the number of stat_freqs you are into the output_freq
    for the incoming timestamp. For example, if stat_freq is daily and output_freq
    is weekly, the weekly files will contain data Mon-Sun. However if the first
    timestamp coming in is a Thursday then the final file will only contain Thu - Sun.
    In this case, timestamp_tot_append = 3, where the unit is days.

    Arguments
    -----------
    stat_freq : str. The requested stat_freq string
    output_freq : str. The requested output_freq string
    timestamp : pd.Timestamp. Incoming tim opaestamp of the current data
    first year : int, optional. First year (only necessary if time_word is 10yearly)

    Returns
    -----------
    timestamp_input_tot_append : int. The number of stat_freq in minutes that you are
            into the ouput_freq.
    """

    if stat_freq == FrequencyKey.CONTINUOUS:
        return 0

    # how many minutes we are into the stat
    minutes_into_stat_freq = get_minutes_into_word(timestamp, stat_freq, first_year)
    # how many minutes we are into the output freq
    minutes_into_output_freq = get_minutes_into_word(
        timestamp, output_freq, first_year
    )
    # how many minutes we are into the output_freq - how many minutes we are into
    # the stat freq (will make it a round value)
    difference_minutes = int(minutes_into_output_freq - minutes_into_stat_freq)
    assert difference_minutes >= 0
    return difference_minutes
