# function to convert input saving frequency into correct number of minutes
def convert_time(time_word ="daily", time_stamp_input = None):

    if(time_word == "hourly"):
        time_min = 60

    elif(time_word == "3hourly"):
        time_min = 3*60

    elif(time_word == "6hourly"):
        time_min = 6*60

    elif(time_word == "12hourly"):
        time_min = 12*60

    elif(time_word == "daily"):
        time_min = 24*60

    elif(time_word == "weekly"):
        time_min = 7*24*60

    elif(time_word == "monthly"):
        # CAN YOU USE FREQSTR HERE? PANDAS DATETIME ATTRIBUTE 
        
        #month = timeStampInput.month

        time_min = time_stamp_input.days_in_month * 24 * 60

        # if (month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12):
        #     # jan, mar, may, july, aug, oct, dec all have 31 days 
        #     time_min = 31*24*60
        # elif(month == 4 or month == 6 or month == 9 or month == 11):
        #     time_min = 30*24*60
        #     # april, june, sep, nov, all have 30 days 
        # elif(month == 2):
        #     # then need to check year for leap year ADD 
        #     year = timeStampInput.year
        #     if (year == 2000 or 2004 or 2008 or 2012 or 2016 or 2020 or 2024 or 2028 or 2032 or 2026 or 2044):
        #         time_min = 28*24*60
        #     else:
        #         time_min = 29*24*60

    elif(time_word == "annually"):

        time_min = 365*24*60


    return time_min
