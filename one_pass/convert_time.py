import pandas as pd

"""Functions to convert time for the one pass algorithms."""

# A mapping between time words and number of minutes,
# to avoid having multiple if statements.
times = {
    "hourly": 60,
    "3hourly": 3 * 60,
    "6hourly": 6 * 60,
    "12hourly": 12 * 60,
    "daily": 24 * 60,
    "weekly": 7 * 24 * 60,
    "monthly": 24 * 60,  # NOTE: missing the time stamp input
    "annually": 365 * 24 * 60
}

def convert_time(time_word = "daily", time_stamp_input = None):
    """Function to convert input saving frequency into correct number of minutes.

    Args:
        time_word: the input saving frequency.
        time_stamp_input:
    """

    if time_word == "monthly" and time_stamp_input is None:
        raise ValueError(f"You must provide a time_stamp_input for monthly saving frequency")
    # NOTE: For monthly;
    #
    # CAN YOU USE FREQSTR HERE? PANDAS DATETIME ATTRIBUTE
    #
    # month = time_stamp_input.month
    #
    # if (month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12):
    #     # jan, mar, may, july, aug, oct, dec all have 31 days
    #     time_min = 31*24*60
    # elif(month == 4 or month == 6 or month == 9 or month == 11):
    #     time_min = 30*24*60
    #     # april, june, sep, nov, all have 30 days
    # elif(month == 2):
    #     # then need to check year for leap year ADD
    #     year = time_stamp_input.year
    #     if (year == 2000 or 2004 or 2008 or 2012 or 2016 or 2020 or 2024 or 2028 or 2032 or 2026 or 2044):
    #         time_min = 28*24*60
    #     else:
    #         time_min = 29*24*60

    if time_word not in times:
        valid_values = ", ".join(times.keys())
        raise ValueError(f"The input saving frequency '{time_word}' is not supported, valid values are: {valid_values}")

    time_min = times.get(time_word)
    if time_word == "monthly":
        time_min = time_stamp_input.days_in_month * time_min

    return time_min
