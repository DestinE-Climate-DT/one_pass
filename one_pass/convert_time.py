import numpy as np
import pandas as pd

"""
Functions to convert time for the one pass algorithms.

times is a mapping between time words (and time stamps) and number of 
minutes, to avoid having multiple if statements. Converts to 
minutes expect for monthly, where you need to check the number 
of days in a month. This is done below 
"""

times = {
    "hourly": 60,
    "3hourly": 3 * 60,
    "6hourly": 6 * 60,
    "12hourly": 12 * 60,
    "daily": 24 * 60,
    "daily_noon" : 24*60,
    "weekly": 7 * 24 * 60,
    "monthly": 24 * 60,  # NOTE: see below for minutes with monthly
    "3monthly": 24 * 60,  # NOTE: see below for minutes with 3monthly
    "annually": 365 * 24 * 60,
}


def convert_time(time_word="daily", time_stamp_input=None):

    """
    Function to convert current time stamp and known frequency
    of the statistic into three outputs

    Arguments
    ---------
    time_word = a word specifiying a frequency for the statistic
    time_stamp_input = a pandas timestamp of the current data

    Returns
    ---------
    stat_freq_min = the total number of minutes corresponding
        to the given 'time_word' (for montly, this will depend
        on the incoming timestamp)
    time_stamp_min = the total number of minutes of the
        incoming timestamp since the start of the given time word
        frequency
    time_stamp_tot_append = the number of units (given by time word)
        of the current time stamp - used for appending data in
        time_append
    """

    #### first looking at requested stat freq ####
    if time_word not in times:
        valid_values = ", ".join(times.keys())
        raise ValueError(
            f"The input saving frequency '{time_word}' is not supported, \
            valid values are: {valid_values}"
        )

    if time_stamp_input is None:
        raise ValueError("You must provide a time_stamp_input for saving frequency")

    # converts word to minutes
    stat_freq_min = times.get(time_word)

    if time_word == "monthly":
        stat_freq_min = time_stamp_input.days_in_month * stat_freq_min
    elif time_word == "3monthly":
        quarter = time_stamp_input.quarter
        if quarter == 1 :
            if time_stamp_input.is_leap_year :
                stat_freq_min = stat_freq_min*(31+31+29)
            else:
                stat_freq_min = stat_freq_min*(31+31+28)
        elif quarter == 2 :
            stat_freq_min = stat_freq_min*(30+31+30)
        elif quarter in (3,4):
            stat_freq_min = stat_freq_min*(31+31+30)

    # now looking at given time stamp
    # converting everything into minutes
    time_stamp_input_min = time_stamp_input.minute
    time_stamp_input_hour = time_stamp_input.hour*60
    time_stamp_input_day_of_month = (time_stamp_input.day-1)*24*60
    time_stamp_input_day_of_week = (time_stamp_input.day_of_week)*24*60
    time_stamp_input_day_of_year = (time_stamp_input.day_of_year-1)*24*60
    time_stamp_input_month = (
        (time_stamp_input.month - 1) * time_stamp_input.days_in_month * 24 * 60
    )
    time_stamp_input_year = time_stamp_input.year * 365 * 24 * 60

    # based on the incoming word (or stat freq will convert the time
    # stamp into the number of minutes INTO that freq)
    if time_word == "hourly":
        time_stamp_min = time_stamp_input_min
        time_stamp_tot_append = time_stamp_input_hour

    elif time_word == "3hourly":
        if np.mod(time_stamp_input.hour, 3) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_min = time_stamp_input_min
        time_stamp_tot_append = time_stamp_input_hour

    elif time_word == "6hourly":
        if np.mod(time_stamp_input.hour, 6) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_min = time_stamp_input_min
        time_stamp_tot_append = time_stamp_input_hour

    elif time_word == "12hourly":
        if np.mod(time_stamp_input.hour, 12) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_min = time_stamp_input_min
        time_stamp_tot_append = time_stamp_input_hour

    elif time_word == "daily":
        time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        time_stamp_tot_append = time_stamp_input_day_of_month

    elif time_word == "daily_noon":

        if time_stamp_input.hour >= 13 :
            time_stamp_input_hour = (time_stamp_input.hour - 13)*60
        else:
            time_stamp_input_hour = (time_stamp_input.hour + 13)*60
        
        time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        time_stamp_tot_append = time_stamp_input_day_of_month

    elif time_word == "weekly":
        time_stamp_min = (
            time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day_of_week
        )
        # this gives you the week of the year
        time_stamp_tot_append = time_stamp_input.week * 24 * 60

    elif time_word == "monthly":

        time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                         time_stamp_input_day_of_month
        time_stamp_tot_append = time_stamp_input_month

    if time_word == "3monthly":
        # True for Feb, March, May, June, August, Sep, 
        if np.mod(time_stamp_input.month - 1, 3) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                             time_stamp_input_day_of_year #  + time_stamp_input_month
        else:
            # for Jan, April, etc.
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                             time_stamp_input_day_of_month

        time_stamp_tot_append = time_stamp_input_month

    if time_word == "annually":

        time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                         time_stamp_input_day_of_year
        time_stamp_tot_append = time_stamp_input_year

    return stat_freq_min, time_stamp_min, time_stamp_tot_append
