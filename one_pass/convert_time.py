"""Functions to convert time for the one pass algorithms.

times is a mapping between time words (and time stamps) and number of
minutes, to avoid having multiple if statements. Converts to
minutes expect for monthly and yearly where you need to check the number
of days in a month. This is done below
"""
import numpy as np
import pandas as pd

times = {
    "half_hourly" : 30,
    "hourly": 60,
    "2hourly":2 * 60,
    "3hourly": 3 * 60,
    "6hourly": 6 * 60,
    "12hourly": 12 * 60,
    "daily": 24 * 60,
    "daily_noon" : 24*60,
    "weekly": 7 * 24 * 60,
    "monthly": 24 * 60,  # NOTE: see below for minutes with monthly
    "3monthly": 24 * 60,  # NOTE: see below for minutes with 3monthly
    "yearly": 365*24 * 60, # NOTE: see below for dealing with leap years
    "10yearly" : 24 * 60, # NOTE: see below for dealing with leap years
}

def convert_word_to_minutes(
        time_word : str, time_stamp_input : pd.Timestamp
    ):
    """Converts word, either the stat_freq or output_freq inputs into
    number of minutes

    Arguments
    ---------
    time_word : str. this will be the time word that does not have a
            set number of minutes. e.g. monthly, 3monthly,
            yearly or 10yearly, due to different numbers of
            days in months and years.
    time_stamp_input : pd.Timestamp. time_stamp_input is the pandas timestamp 
            of the current incoming data

    Returns
    ---------
    stat_freq_min : the total number of minutes corresponding
            to the given 'time_word' and current time stamp
    """
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
    elif time_word == "yearly" and time_stamp_input.is_leap_year :
        stat_freq_min += (24 * 60)
    elif time_word == "10yearly" :
        # if the 1st or second year in the 10 is a leap year then
        # there will be 3 leap years in the 10 year period
        # otherwise there will only be 2
        if time_stamp_input.is_leap_year :
            stat_freq_min *= (3*366 + 7*365)
        elif (time_stamp_input + pd.DateOffset(years =1)).is_leap_year :
            stat_freq_min *= (3*366 + 7*365)
        else:
            stat_freq_min *= (2*366 + 8*365)

    return stat_freq_min

def calc_time_stamp_min(time_stamp_input : pd.Timestamp,
                        time_word : str, class_obj : object = None
                    ):
    """Calculates the time in minutes that the current time stamp
    is into the selected time word.

    Arguments
    -----------
    time_stamp_input : pd.Timestamp of the current incoming data slice
    time_word : str. String corresponding to the selected frequency request
            of the data. e.g. 'daily'.
    class_obj : class. Opa class.

    Returns
    ----------
    time_stamp_min : int. The number of minutes that the incoming
            time_stamp_input is into the selected time_word.
    """
    # now looking at given time stamp
    # converting everything into minutes
    time_stamp_input_min = time_stamp_input.minute
    time_stamp_input_hour = time_stamp_input.hour*60
    time_stamp_input_day_of_month = (time_stamp_input.day-1)*24*60
    time_stamp_input_day_of_week = (time_stamp_input.day_of_week)*24*60
    time_stamp_input_day_of_year = (time_stamp_input.day_of_year-1)*24*60

    # based on the incoming word (or stat freq will convert the time
    # stamp into the number of minutes INTO that freq)
    if time_word in ("hourly", "half_hourly"):
        time_stamp_min = time_stamp_input_min

    elif time_word == "2hourly":
        if np.mod(time_stamp_input.hour, 2) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_min = time_stamp_input_min

    elif time_word == "3hourly":
        if np.mod(time_stamp_input.hour, 3) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_min = time_stamp_input_min

    elif time_word == "6hourly":
        if np.mod(time_stamp_input.hour, 6) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_min = time_stamp_input_min

    elif time_word == "12hourly":
        if np.mod(time_stamp_input.hour, 12) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_min = time_stamp_input_min

    elif time_word == "daily":
        time_stamp_min = time_stamp_input_min + time_stamp_input_hour

    elif time_word == "daily_noon":
        if time_stamp_input.hour >= 13 :
            time_stamp_input_hour = (time_stamp_input.hour - 13)*60
        else:
            time_stamp_input_hour = (time_stamp_input.hour + 13)*60

        time_stamp_min = time_stamp_input_min + time_stamp_input_hour

    elif time_word == "weekly":
        time_stamp_min = (
            time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day_of_week
        )

    elif time_word == "monthly":

        time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                         time_stamp_input_day_of_month

    if time_word == "3monthly":
        # True for Feb, March, May, June, August, Sep,
        if np.mod(time_stamp_input.month - 1, 3) != 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                             time_stamp_input_day_of_year
        else:
            # for Jan, April, etc.
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                             time_stamp_input_day_of_month

    if time_word == "yearly":
        time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                         time_stamp_input_day_of_year

    if time_word == "10yearly":
        if class_obj is not None:
            try:
                getattr(class_obj, "year_for_10annual")
            except AttributeError:
                setattr(class_obj, "year_for_10annual", time_stamp_input.year)

        years_in = np.mod(time_stamp_input.year - getattr(class_obj, "year_for_10annual"), 10)

        if years_in == 0:
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                             time_stamp_input_day_of_year #  + time_stamp_input_month
        else:
            additional_years = 0
            for k in range(years_in):
                if (time_stamp_input + pd.DateOffset(years =-k)).is_leap_year:
                    additional_years += (366 * 24 * 60)
                else:
                    additional_years += (365 * 24 * 60)
            time_stamp_min = time_stamp_input_min + time_stamp_input_hour + \
                             time_stamp_input_day_of_year + additional_years

    return time_stamp_min

def convert_time(time_word : str, time_stamp_input : pd.Timestamp,
                 class_obj = None
                ):
    """Function to convert current time stamp and requested frequency
    of the statistic into two outputs

    Arguments
    ---------
    time_word : str. a word specifiying a frequency for the statistic
    time_stamp_input : Panda pandas timestamp of the current data
    class_object : object. The Opa class object

    Returns
    ---------
    stat_freq_min : the total number of minutes of the time_word (usually)
            stat_freq but in the case of continuous data, output_freq
    time_stamp_min : the total number of minutes of the
            incoming timestamp since the start of the given time word
            frequency
    """
    #### first looking at requested stat freq ####
    if time_word not in times:
        valid_values = ", ".join(times.keys())
        raise ValueError(
            f"The input saving frequency '{time_word}' is not supported, "
            f"valid values are: {valid_values}"
        )

    stat_freq_min = convert_word_to_minutes(time_word, time_stamp_input)

    time_stamp_min = calc_time_stamp_min(time_stamp_input, time_word, class_obj)

    return stat_freq_min, time_stamp_min

def convert_time_append(stat_freq : str, output_freq : str,
                            time_stamp_input : pd.Timestamp, class_obj : object
                        ):
    """Calculates the value time_stamp_tot_append. This is only relevant if
    output_freq > stat_freq and you want multiple statistcs appended in one
    xr.Dataset. It gives you the number of stat_freqs you are into the output_freq
    for the incoming timestamp. For example, if stat_freq is daily and output_freq
    is weekly, the weekly files will contain data Mon-Sun. However if the first
    timestamp coming in is a Thursday then the final file will only contain Thu - Sun.
    In this case, time_stamp_tot_append = 3, where the unit is days.

    Arguments
    -----------
    stat_freq : str. The requested stat_freq string
    output_freq : str. The requested output_freq string
    time_stamp_input : pd.Timestamp. Incoming timestamp of the current data
    class_obj : Opa class. to check for certain variables.

    Returns
    -----------
    time_stamp_input_tot_append : int. The number of stat_freq in minutes that you are
            into the ouput_freq.
    """
    # how many minutes you are into the stat
    time_stamp_min_into_stat = calc_time_stamp_min(
                                    time_stamp_input, stat_freq, class_obj
                                )
    # how many minutes you are into the output freq
    time_stamp_min_into_output = calc_time_stamp_min(
                                    time_stamp_input, output_freq, class_obj
                                )
    # how many minutes you are into the output_freq - how many minutes you are into
    # the stat freq (will make it a round value)
    time_stamp_tot_append = int(time_stamp_min_into_output - time_stamp_min_into_stat)

    return time_stamp_tot_append
