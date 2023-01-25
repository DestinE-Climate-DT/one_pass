

# import relevant libraries 
import numpy as np
import xarray as xr 
import pandas as pd
import glob
from datetime import datetime, timedelta

#import sys
#import time
#import dask
#import os
#import cfgrib

# actual one-pass function for calculating the mean statistic 
# input is the new data set (as numpy array)
# mean is the previously calculated mean (same shape as input, all zeros if first one, again numpy array)
# count is the actual number of values that have been input into the one-pass 

def meanFun(input, mean, count):

    # now to update the acutal mean:
    # can do this element use (python) or using numpy, should check speed but I think python is faster
    mean += (input - mean)/(count+1) # python method 
    #mean += np.subtract(input, mean)/(count+1) #numpy method
    count += 1 # updating the count 
    
    # checker to see the loop is working
    #print(count)
    
    return mean, count 

    # this function includes all the meta data checks and calculations for count and nData 

def meanOPA(dsFull, meanFreq, var, save):

    # defining these as arbitary values so that we can start the while loop, these will be redefinded on the first loop
    nData = 1000
    count = 0

    while(count < nData):

        # HERE YOU HAVE THE MARS REQUESTS FOR DATA 
        # simulating streamed data by just taking a time slice 
        ds = dsFull.isel(time=slice(count,(count+1))) # slice(start, stop, step), extract 'moving window' which is hourly data

        # currently ds is the whole Dataset with all variables so want to filter for use one variable, 
        # below code only works on a dataArray not on a full dataSet 
        # converting dataSet into dataArray LIST OF ALL POTENTIAL GRIB VARIABLES THAT IT COULD BE 
        if(var == "tas"):
            ds = ds.tas
        elif(var == "uas"):
            ds = ds.uas
        elif(var == "vas"):
            ds = ds.vas
 
        # this will work even with 1 input for time, but data must still have a time dimension 
        timeStampList = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps 
        # this exracts all the timestamps of each data point in hours converting to a pandas timestamp from np.datetime64
        timeStampPandas = [pd.to_datetime(x) for x in timeStampList]
        timeStamp = timeStampPandas[0]

        # # checking that the spacing of the data is indeed hours
        # will need to do this differently when you only have 1 GRIB message - e.g. one time stamp
        # this is currently using the numpy version of the list, maybe re-write to work on Pandas date time list
        # TIMESTEP SHOUND BE KNOWN APRIORI FROM THE MARS REQUEST
        timeStep = np.float64(60) # converting timeStep into minutes, float 64
        #timeStampList[1] - timeStampList[0] # this is assuming there are a few GRIB mesages per data block 
        
        # need to decide what your default time unit is going to be 
        # THIS IS CRITICAL FOR NOW CHOOSING MINUTES
        timeStep = (timeStep.astype('timedelta64[m]') / np.timedelta64(1, 'm')) # converting timeStep into minutes, float 64

        # loop to calculate nData, based on timeStamp and timeStep 
        # want to check what the mean frequency is, converting data to integer value for looping 
        # - this could be problematic if not wholy divisable
        if(meanFreq == "hourly"): # i.e hourly means
            # first thing to check is if this is the first in the series, time stamp must be less than 60 
            timeStampMin = timeStamp.minute
            
            if(timeStampMin == timeStep): # this indicates that it's the first data of the day otherwise timeStamp will be larger
                # calculated by hour freq of mean / timestep (hours) of data
                nData = int(60/timeStep) # number of elements of data that need to be added to the cumulative mean 
                count = 0 # first in the loop 
                mean = np.zeros((np.size(ds.lat), np.size(ds.lon))) # only initalise cumulative mean if you know this is the first input
                #timeDaily = pd.DatetimeIndex([]) # initalising time loop 

            elif(timeStep > 60):
                # we have a problem 
                print('timeStep too large for hourly means')

        elif(meanFreq == "daily"):
            # first check if the timeStep is less than an hour, in which case need to count the minutes 
            if(timeStep < 60):
                timeStampMin = timeStamp.minute
                if(timeStampMin == timeStep): # this indicates that it's the first, works when comparing float64 to int
                    nData = int(24*60/timeStep) # number of elements of data that need to be added to the cumulative mean 
                    count = 0
                    mean = np.zeros((np.size(ds.lat), np.size(ds.lon))) # only initalise cumulative mean if you know this is the first input
                    #timeDaily = pd.DatetimeIndex([]) # initalising time loop 

            elif(timeStep < 60*24): # time step is less than a day 
                timeStampHour = timeStamp.hour*60 # converting to minutes 
                
                # THIS LINE NEEDS TO CHECK IF IT'S THE FIRST DATA ELEMENT COMING IN
                # NEED TO FIND A MORE ROBUST WAY OF DOING THIS
                if(timeStampHour == 0): # this indicates that it's the first, works when comparing float64 to int,
                    nData = int(24*60/timeStep) # number of elements of data that need to be added to the cumulative mean 
                    count = 0
                    mean = np.zeros((np.size(ds.lat), np.size(ds.lon))) # only initalise cumulative mean if you know this is the first input
                    #timeDaily = pd.DatetimeIndex([]) # initalising time loop 

        elif(meanFreq == "weekly"):
            nData = int(24*7*60/timeStep) # is there ever not 7 days in a week? 
            # NOT FINISHED 

        elif(meanFreq == "monthly"): 
            # NOT FINISHED 
            # need to check the month of input before making calculation 
            # this extracts the month of the date, need to check this works with different versions of numpy 
            month = timeStampPandas[0].month
            if (month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12):
                # jan, mar, may, july, aug, oct, dec all have 31 days 
                nData = int((31*24*60)/timeStep)
            elif(month == 4 or month == 6 or month == 9 or month == 11):
                nData = int((30*24*60)/timeStep)
                # april, june, sep, nov, all have 30 days 
            elif(month == 2):
                # then need to check year for leap year ADD 
                nData = int((28*24*60)/timeStep)

        # if none of the if statements above are entered then none of the arrays will be re-initalised and the code will carry on 
        # assuming that the cumulative mean is saved in memory 


        # incoming into this you ALWAYS have:
        #  ds - new dataArray (no longer dataSet as you have extracted the variable of interest)
        #  mean - cumulative, might be an empty array if this is the first time this has run
        #  nData - number of data points required until the mean is full 
        #  count - number of data points that have been read 

        ## HERE CONVERTING THE NEW DATA INTO A NUMPY ARRAY 
        # this may need to be more robust if multiple heights or other dimensions 
        ds = np.squeeze(ds).data # if there are multiple heights in the same file, this will remove redundant 1 dimensions and .data extracts numpy array


        ## RUNNING THROUGH ACTUAL MEAN ALGORITHM 
        mean, count = meanFun(ds, mean, count)
            
            # what would need to be saved for a restart file? 
            # RESTART FILES?

    ## NOW SAVING - DON'T NEED A CONDITION AS WHILE LOOP HAS FINISHED 
    
    #meanDaily = np.insert(meanDaily, countDays,  mean, axis = 0) # this is if you want to put multiple days in one file 
    #meanDaily = np.vstack((meanDaily, mean))
    #timeDaily[0] = timeStamp.date()
    #timeDaily = pd.DatetimeIndex.insert(timeDaily, countDays, timeStampDaily[i])

    #countDays = countDays + 1

    mean = np.expand_dims(mean, axis=0) # adding back extra time dimension 

    dsFull.attrs["OPA"] = "daily mean calculated using one-pass algorithm"
    attrs = dsFull.attrs

    # converting the mean into a new dataArray 
    dm = xr.Dataset(
    data_vars = dict(
        tas_Mean = (["time","lat","lon"], mean),    # need to add variable attributes                         
    ),
    coords = dict(
        time = (["time"], [pd.to_datetime(timeStamp.date())]),
        lon = (["lon"], dsFull.lon.data),
        lat = (["lat"], dsFull.lat.data),
    ),
    attrs = attrs
    )

    if(save == "true"):
        # save new DataSet as netCDF 
        newFile = "/esarchive/scratch/kgrayson/git/onepass_development/mean/nexGEMS/tas_daily_means.nc"
        #f = open(newFile, "w")
        dm.to_netcdf(path = newFile)
        dm.close() 
        print('finished saving')
    else: 
        return dm
        # output DataSet in memory, should look like incoming file but with some attributes changed 

# working with nextGEMS original files 
#filePath = "/esarchive/exp/mpi-esm1-2-xxr/nextgems/original_files/"

filePath = "/esarchive/scratch/alacima/python/destination_earth/icon/*.nc" # this has already been re-gridded onto regular lat / lon grid
#fileList = sorted(os.listdir(filePath)) # sorted to get them into the correct order 
fileList = glob.glob(filePath) # glob function used to just extract the netCDF files 
fileList.sort() # sorted to get them into the correct order 

nFiles = np.size(fileList) # finding number of files (also number of months)
#fileName = filePath + fileList[0]

ds = xr.open_dataset(fileList[0]) # open dataset 

#, chunks={"values": "auto"}
#ds = xr.open_dataset(filePath,engine='cfgrib',backend_kwargs={'filter_by_keys':{'typeOfLevel': 'surface'}})

#ds = xr.open_dataset(fileName, engine = "netcdf4", chunks={"lat": 100, "lon": 100}) # open dataset 
# adding in chunks so that the data set is filled with dask arrays, might want to use auto chunking 
#, chunks={"lat": "auto", "lon": "auto"}

# what variables need to be passed to the function? 
meanFreq = "daily"
var = "tas"
save = "false"

# actual function for OPA 
dm = meanOPA(ds, meanFreq, var, save)