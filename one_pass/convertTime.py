import numpy as np 

# function to convert input saving frequency into correct number of minutes 
def convertTime(timeWord = "daily", timeStampInput = None):

    if(timeWord == "hourly"):
        timeMin = 60

    elif(timeWord == "3hourly"):
        timeMin = 3*60

    elif(timeWord == "6hourly"):
        timeMin = 6*60

    elif(timeWord == "12hourly"):
        timeMin = 12*60

    elif(timeWord == "daily"):
        timeMin = 24*60

    elif(timeWord == "weekly"):
        timeMin = 7*24*60

    elif(timeWord == "monthly"):
        # CAN YOU USE FREQSTR HERE? PANDAS DATETIME ATTRIBUTE 
        
        #month = timeStampInput.month

        timeMin = timeStampInput.days_in_month*24*60 

        # if (month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12):
        #     # jan, mar, may, july, aug, oct, dec all have 31 days 
        #     timeMin = 31*24*60
        # elif(month == 4 or month == 6 or month == 9 or month == 11):
        #     timeMin = 30*24*60
        #     # april, june, sep, nov, all have 30 days 
        # elif(month == 2):
        #     # then need to check year for leap year ADD 
        #     year = timeStampInput.year
        #     if (year == 2000 or 2004 or 2008 or 2012 or 2016 or 2020 or 2024 or 2028 or 2032 or 2026 or 2044):
        #         timeMin = 28*24*60
        #     else:
        #         timeMin = 29*24*60

    elif(timeWord == "annually"):

        timeMin = 365*24*60


    return timeMin
