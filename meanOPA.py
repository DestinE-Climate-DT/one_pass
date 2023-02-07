# module imports happen inside here
import numpy as np
import xarray as xr 
import pandas as pd
import glob
from datetime import datetime, timedelta
import yaml
import sys


class meanOPA: # individual clusters 

    # initalising the function from the ymal config file 
    def __init__(self, filePath = "config.yml"): 
            # read the config yaml
        #filePath = '/esarchive/scratch/kgrayson/git/one_pass/config.yml'
       

        try:
            with open(filePath, 'r', encoding='utf-8') as file:
                config = yaml.load(file, Loader=yaml.FullLoader)
        except IOError:
            sys.exit(['ERROR: config file not found: you need to have this configuration file!'])

            #sys.exit([f'ERROR: {filePath} not found: you need to have this configuration file!'])
        
        #return cfg

        #with open(filePath) as f:
        #    config = yaml.safe_load(f)

        # extracting variables from the config dictionary 
        meanFreq = dict((k, config[k]) for k in ['meanFreq']
            if k in config)
        meanFreq = list(meanFreq.values())
        self.meanFreq = meanFreq[0]

        var = dict((k, config[k]) for k in ['var']
            if k in config)
        var = list(var.values())
        self.var = var[0]

        save = dict((k, config[k]) for k in ['save']
            if k in config)
        save = list(save.values())
        self.save = save[0]

        saveFreq = dict((k, config[k]) for k in ['saveFreq']
            if k in config)
        saveFreq = list(saveFreq.values())
        self.saveFreq = saveFreq[0]

        timeStep = dict((k, config[k]) for k in ['timeStep']
            if k in config)
        timeStep = list(timeStep.values())
        self.timeStep = np.float64(timeStep[0])

        filePath = dict((k, config[k]) for k in ['filePath']
            if k in config)
        filePath = list(filePath.values())
        self.filePath = filePath[0]

        filePathSave = dict((k, config[k]) for k in ['filePathSave']
            if k in config)
        filePathSave = list(filePathSave.values())
        self.filePathSave = filePathSave[0]

        

    # maybe most important function to check at what stage of the code you are 
    def _checkTimeStamp(self, ds):

        timeStampList = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps 
        timeStampPandas = [pd.to_datetime(x) for x in timeStampList]
        timeStamp = timeStampPandas[0]
        self.timeStamp = timeStamp
        # this is currently using the numpy version of the list, maybe re-write to work on Pandas date time list
        # TIMESTEP SHOUND BE KNOWN APRIORI FROM THE MARS REQUEST
        #timeStep = np.float64(60) # converting timeStep into minutes, float 64
        #timeStampList[1] - timeStampList[0] # this is assuming there are a few GRIB mesages per data block 
        
        # need to decide what your default time unit is going to be THIS IS CRITICAL FOR NOW CHOOSING MINUTES
        timeStep = (self.timeStep.astype('timedelta64[m]') / np.timedelta64(1, 'm')) # converting timeStep into minutes, float 64

        # loop to calculate nData, based on timeStamp and timeStep 
        # want to check what the mean frequency is, converting data to integer value for looping 
        # - this could be problematic if not wholy divisable
        if(self.meanFreq == "hourly"): # i.e hourly means
            # first thing to check is if this is the first in the series, time stamp must be less than 60 
            timeStampMin = timeStamp.minute

            # timeStep < meanFreq
            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampMin <= timeStep): # this indicates that it's the first data of the day otherwise timeStamp will be larger
                # calculated by hour freq of mean / timestep (hours) of data
                self.nData = int(60/timeStep) # number of elements of data that need to be added to the cumulative mean 
                self.count = 0 # first in the loop 
                self.meanCum = np.zeros((np.size(ds.lat), np.size(ds.lon))) # only initalise cumulative mean if you know this is the first input

            # timeStep == meanFreq 
            elif(timeStep == 60): # the case where timeStep matches meanFreq 
                # calculated by hour freq of mean / timestep (hours) of data
                self.nData = int(60/timeStep) # number of elements of data that need to be added to the cumulative mean 
                self.count = 0 # first in the loop 
                self.meanCum = np.zeros((np.size(ds.lat), np.size(ds.lon))) # only initalise cumulative mean if you know this is the first input

            # timeStep > meanFreq 
            elif(timeStep > 60):
                # we have a problem 
                print('timeStep too large for hourly means')

        if(self.meanFreq == "daily"):
            #
            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampTot = timeStampMin + timeStampHour 

            # first check if the timeStep is less than an hour, in which case need to count the minutes 
            if(timeStep < 60):
                # this will only work if the one hour is wholly divisable by the timestep 
                if(timeStampTot <= timeStep): # this indicates that it's the first, works when comparing float64 to int
                    self.nData = int(24*60/timeStep) # number of elements of data that need to be added to the cumulative mean 
                    self.count = 0
                    self.meanCum = np.zeros((np.size(ds.lat), np.size(ds.lon))) # only initalise cumulative mean if you know this is the first input

            elif(timeStep < 60*24): # time step is less than a day 
                # THIS LINE NEEDS TO CHECK IF IT'S THE FIRST DATA ELEMENT COMING IN
                # NEED TO FIND A MORE ROBUST WAY OF DOING THIS
                if(timeStampHour == 0): # this indicates that it's the first, works when comparing float64 to int,
                    self.nData = int(24*60/timeStep) # number of elements of data that need to be added to the cumulative mean 
                    self.count = 0
                    self.meanCum = np.zeros((np.size(ds.lat), np.size(ds.lon))) # only initalise cumulative mean if you know this is the first input


        elif(self.meanFreq == "weekly"):
            self.nData = int(24*7*60/timeStep) # is there ever not 7 days in a week? 
            # NOT FINISHED 

        elif(self.meanFreq == "monthly"): 
            # NOT FINISHED 
            # need to check the month of input before making calculation 
            # this extracts the month of the date, need to check this works with different versions of numpy 
            month = timeStampPandas[0].month
            if (month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12):
                # jan, mar, may, july, aug, oct, dec all have 31 days 
                self.nData = int((31*24*60)/timeStep)
            elif(month == 4 or month == 6 or month == 9 or month == 11):
                self.nData = int((30*24*60)/timeStep)
                # april, june, sep, nov, all have 30 days 
            elif(month == 2):
                # then need to check year for leap year ADD 
                self.nData = int((28*24*60)/timeStep)
        

    # reducing the variable space 
    def _checkVariable(self, ds): # THIS NEEDS TO CONVERT FROM A DATASET TO A DATAARRAY 
        if(self.var == "tas"):
            ds = ds.tas
        elif(self.var == "uas"):
            ds = ds.uas
        elif(self.var == "vas"):
            ds = ds.vas
        return ds


    def _checkNumTimeStamps(self, ds):
        # checking to see how many time stamps are actually in the file, it's possible that the FDB interface will have mutliple messages 
        timeStampList = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps 
        timeNum = np.size(timeStampList)
        #self.timeNum = timeNum
        
        return timeNum

    def _convertNumpy(self, ds):
        dsNp = np.squeeze(ds).data # if there are multiple heights in the same file, this will remove redundant 1 dimensions and .data extracts numpy array
        return dsNp
    
    # actual mean function
    def _update(self, dsNp, weight=1):# where x is the new value and weight is the new weight 
        self.count += weight
        self.meanCum += weight*(dsNp - self.meanCum) / (self.count) # udating mean with one-pass algorithm

    def _dataOutput(self, ds):

        finalMean = np.expand_dims(self.meanCum, axis=0) # adding back extra time dimension 

        if (self.meanFreq == "hourly"):
            finalTimeStamp = self.timeStamp
        elif (self.meanFreq == "daily"): 
            finalTimeStamp = self.timeStamp.date()

        ds.attrs["OPA"] = "daily mean calculated using one-pass algorithm"
        attrs = ds.attrs

        # converting the mean into a new dataArray 
        dm = xr.Dataset(
        data_vars = dict(
            tas_Mean = (["time","lat","lon"], finalMean),    # need to add variable attributes                         
        ),
        coords = dict(
            time = (["time"], [pd.to_datetime(finalTimeStamp)]),
            lon = (["lon"], ds.lon.data),
            lat = (["lat"], ds.lat.data),
        ),
        attrs = attrs
        )

        if(self.save == "true"):
            fileName = self.filePathSave + self.var + "_" + self.meanFreq + str(finalTimeStamp) + "_mean.nc" 
            # save new DataSet as netCDF 
            #f = open(newFile, "w")
            dm.to_netcdf(path = fileName)
            dm.close() 
            print('finished saving')
        
        
        return dm
            # output DataSet in memory, should look like incoming file but with some attributes changed 


    def mean(self, ds): # acutal function that you call 
        
        # STEPS 
        # 1. check time stamp of data at set nCount etc.
        # 2. check how many time stamps are in the data 
        # 3. actual algorithm 
        # 4. write algorithm 

        # check the time stamp and if the data needs to be reset 
        self._checkTimeStamp(ds)

        if (hasattr(self, 'var')): # if there are multiple variables in the file 
            # THIS NEEDS TO CONVERT FROM A DATASET TO A DATA ARRAY 
            ds = self._checkVariable(ds)


        timeNum = self._checkNumTimeStamps(ds) # this checks if there are multiple time stamps in a file and will do np.mean

        if (timeNum == 1):
            # convert array to numpy 
            dsNp = self._convertNumpy(ds) # this removes redundant singular dimensions         
            # actually adding to the mean 
            self._update(dsNp)

        elif (timeNum > 1): 

            axNum = ds.get_axis_num('time')
            # first compute normal mean
            tempMean = np.mean(ds, axis = axNum, dtype = np.float64)  # updating the mean with np.mean over the timesteps avaliable 
            self.tempMean = tempMean
            #self.count += timeNum # updating the count 
            dsNp = self._convertNumpy(tempMean)
            self.dsNp = dsNp
            self._update(dsNp, timeNum)
        

        if (self.count == self.nData):
        # how to output the data 
            dm = self._dataOutput(ds)

            return dm # returning dm from the function only if this condtion is met 

