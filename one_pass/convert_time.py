import pandas as pd
import numpy as np

"""Functions to convert time for the one pass algorithms."""
"""A mapping between time words and number of minutes,
# to avoid having multiple if statements. Converts to minutes expect for monthly, 
where you need to check the number of days in a month. This is done below. """

times = {
    "hourly": 60,
    "3hourly": 3 * 60,
    "6hourly": 6 * 60,
    "12hourly": 12 * 60,
    "daily": 24 * 60,
    "weekly": 7 * 24 * 60,
    "monthly": 24 * 60,  # NOTE: missing the time stamp input
    "3monthly": 24 * 60,
    "annually": 365 * 24 * 60,
    "continuous": 10e10, # NOTE: missing the time step input 
}

def convert_time(time_word = "daily", time_stamp_input = None, time_step_input = None):

    """Function to convert input saving frequency into correct number of minutes.

    Args:
        time_word: the input saving frequency.
        time_stamp_input: input timestamp of the incoming data 
    """

    #### first looking at requested stat freq #### 
    if time_word not in times:
        valid_values = ", ".join(times.keys())
        raise ValueError(f"The input saving frequency '{time_word}' is not supported, valid values are: {valid_values}")

    if time_stamp_input is None:
        raise ValueError(f"You must provide a time_stamp_input for saving frequency")
    # NOTE: For monthly;

    stat_freq_min = times.get(time_word)

    if time_word == "monthly":
        stat_freq_min = time_stamp_input.days_in_month * stat_freq_min
    elif time_word == "3monthly":
        stat_freq_min = stat_freq_min * (time_stamp_input.days_in_month + 
                               pd.Timestamp(year = time_stamp_input.year, month = time_stamp_input.month + 1, day = 1).days_in_month + 
                               pd.Timestamp(year = time_stamp_input.year, month = time_stamp_input.month + 2, day = 1).days_in_month)
    elif time_word == "continuous": 
        stat_freq_min = stat_freq_min * time_step_input # this is to make sure n_data is fully divisable by the timestep

    #### now looking at given time stamp #### 
    # converting everything into minutes 
    time_stamp_input_min = time_stamp_input.minute
    time_stamp_input_hour = time_stamp_input.hour*60 
    time_stamp_input_day = (time_stamp_input.day-1)*24*60
    time_stamp_input_day_of_week = (time_stamp_input.day_of_week)*24*60
    time_stamp_input_month = (time_stamp_input.month- 1)* time_stamp_input.days_in_month*24*60
    time_stamp_input_year = time_stamp_input.year*365*24*60

    if(time_word == "hourly"):
        
        time_stamp_input_tot = time_stamp_input_min
        time_stamp_tot_append = time_stamp_input_hour

    elif(time_word == "3hourly"):
        
        if(np.mod(time_stamp_input.hour, 3) != 0):
            time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_input_tot = time_stamp_input_min

    elif(time_word == "6hourly"):
        
        if(np.mod(time_stamp_input.hour, 6) != 0):
            time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour
        else:
            time_stamp_input_tot = time_stamp_input_min

    elif(time_word == "daily"):

        time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour
        time_stamp_tot_append = time_stamp_input_day

    elif(time_word == "weekly"):

        time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day_of_week

    elif(time_word == "monthly"):

        time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day 
        time_stamp_tot_append = time_stamp_input_month

    if(time_word == "3monthly"):

        if(np.mod(time_stamp_input.month - 1, 3) != 0):
            time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day + time_stamp_input_month
        else:
            time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day

    if(time_word == "annually"):

        time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day + time_stamp_input_month
        #TODO: finish time_stamp_tot_append 
        
    if(time_word == "continuous"):

        time_stamp_input_tot = time_stamp_input_min + time_stamp_input_hour + time_stamp_input_day + time_stamp_input_month + time_stamp_input_year

    return stat_freq_min, time_stamp_input_tot, time_stamp_tot_append
