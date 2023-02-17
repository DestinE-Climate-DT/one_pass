# module imports happen inside here
import numpy as np
import xarray as xr 
import pandas as pd
import glob
from datetime import datetime, timedelta
import yaml
import sys
import dask.array as da
import os
import importlib
imported_module = importlib.import_module("convertTime")
importlib.reload(imported_module)
from convertTime import convertTime
#os.chdir('/home/b/b382291/regridder/AQUA')
from aqua.util import load_yaml

class meanOPA: # individual clusters 

    # initalising the function from the ymal config file 
    def __init__(self, meanFreq = "daily", 
                var = None , save = "false", saveFreq = None): 

        self.meanFreq = meanFreq
        #self.var = var
        self.save = save
        self.saveFreq = saveFreq

        if (var != None):
            self.var = var 

        filePath = "/home/b/b382291/git/one_pass/config.yml"
        config = load_yaml(filePath)

        self.timeStep = config["timeStep"] # this is int value in minutes 

        self.filePathSave = config["filePathSave"]


    def _initalise(self, ds): 
        # only initalise cumulative mean if you know this is the first input
        self.count = 0
        # calculated by MIN freq of mean / timestep min of data
        self.nData = int(self.meanFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean
        if ds.chunks is None: 
            self.meanCum = np.zeros((np.size(ds.lat), np.size(ds.lon))) 
        else:
            self.meanCum = da.zeros((np.size(ds.lat), np.size(ds.lon))) 


    def _checkTimeStamp(self, ds):
    # function to calculate: 
    # - nData (number of pieces of information (GRIB messages) required to make up the required statistic)
    # - count (how far through the statisic you are, i.e. count = 5 if you've read 5 of the required GRIB messages)
    # - initalises empty array for the statistic 
    # all based on timeStamp and timeStep 

        timeStampList = sorted(ds.time.data) # assuming that incoming data has a time dimension
        timeStampPandas = [pd.to_datetime(x) for x in timeStampList]
        #self.timeStamp = timeStampList[0]
        timeStamp = timeStampPandas[0] # converting to a pandas datetime to calculate if it's the first 
        self.timeStamp = timeStamp
        
        #self.timeStep = (self.timeStep.astype('timedelta64[m]') / np.timedelta64(1, 'm')) # converting timeStep into minutes, float 64

        # converting mean freq into a number
        self.meanFreqMin = convertTime(timeWord = self.meanFreq, timeStamp = timeStampList)

        if(self.meanFreq == "hourly"): 

            # first thing to check is if this is the first in the series, time stamp must be less than 60 
            timeStampMin = self.timeStamp.minute

            # timeStep < meanFreq
            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampMin <= self.timeStep): # this indicates that it's the first data of the day otherwise timeStamp will be larger
                # initalise cumulative mean array 
                self._initalise(ds)

            # timeStep == meanFreq 
            elif(self.timeStep == self.meanFreqMin): # the case where timeStep matches meanFreq 
                # initalise cumulative mean array 
                self._initalise(ds)

            # timeStep > meanFreq 
            elif(self.timeStep > self.meanFreqMin):
                # we have a problem 
                print('timeStep too large for hourly means')


        if(self.meanFreq == "daily"):
            #
            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampTot = timeStampMin + timeStampHour 

            # first check if the timeStep is less than an hour, in which case need to count the minutes 
            if(self.timeStep < 60):
                # this will only work if the one hour is wholly divisable by the timestep 
                if(timeStampTot <= self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                    # initalise cumulative mean array 
                    self._initalise(ds)


            elif(self.timeStep < self.meanFreqMin): # time step is less than a day 
                # NEED TO FIND A MORE ROBUST WAY OF DOING THIS
                if(timeStampHour == 0): # this indicates that it's the first, works when comparing float64 to int,
                    # initalise cumulative mean array 
                    self._initalise(ds)


        elif(self.meanFreq == "weekly"):
            self.nData = int(self.meanFreqMin/self.timeStep) # is there ever not 7 days in a week? 
            # NOT FINISHED 
                #self._initalise(ds) 


        elif(self.meanFreq == "monthly"): 
            # NOT FINISHED 
            # need to check the month of input before making calculation 
            # this extracts the month of the date, need to check this works with different versions of numpy 
            month = timeStampPandas[0].month

            #if(test_first == "true"): # FIX 
            #    self._initalise(ds)


    # reducing the variable space, if the input is in a dataset it will convert to a dataArray 
    def _checkVariable(self, ds): 
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


    def _npMean(self, ds): # computes np mean 
        axNum = ds.get_axis_num('time')
        # first compute normal mean
        tempMean = np.mean(ds, axis = axNum, dtype = np.float64)  # updating the mean with np.mean over the timesteps avaliable 
        #self.tempMean = tempMean
        return tempMean

    def _convertNumpy(self, ds):
        dsNp = np.squeeze(ds) # if there are multiple heights in the same file, this will remove redundant 1 dimensions and .data extracts numpy array
        return dsNp
    
    # actual mean function
    def _update(self, dsNp, weight=1):# where x is the new value and weight is the new weight 
        self.count += weight
        self.meanCum += weight*(dsNp - self.meanCum) / (self.count) # udating mean with one-pass algorithm


    def _createDataSet(self, finalMean, finalTimeStamp, ds, attrs):

        # converting the mean into a new dataArray 
        dm = xr.Dataset(
        data_vars = dict(
            meanVar = (["time","lat","lon"], finalMean),    # need to add variable attributes                         
        ),
        coords = dict(
            time = (["time"], [pd.to_datetime(finalTimeStamp)]),
            lon = (["lon"], ds.lon.data),
            lat = (["lat"], ds.lat.data),
        ),
        attrs = attrs
        )
        return dm 


    def _dataOutput(self, ds):

        # this is really slow 
        finalMean = np.expand_dims(self.meanCum, axis=0) # adding back extra time dimension 

        if (self.meanFreq == "hourly"):
            finalTimeStamp = self.timeStamp
            timeStampString = finalTimeStamp[0].strftime("%Y_%m_%d_%H") 

        elif (self.meanFreq == "daily"): 
            finalTimeStamp = self.timeStamp.date()
            timeStampString = self.timeStamp.strftime("%Y_%m_%d")

        ds.attrs["OPA"] = "daily mean calculated using one-pass algorithm"
        attrs = ds.attrs


        dm = self._createDataSet(finalMean, finalTimeStamp, ds, attrs)

        return dm, timeStampString

    def _dataOutputAppend(self, ds, timeDimLength):

        if (self.countSave > 0):
            self.dmSave = np.append(self.dmSave, ds) # appending new time 
        else: 
            self.finalTimeStamp =[] # this needs to be self.

        if (self.meanFreq == "hourly"):
            self.finalTimeStamp = np.append(self.finalTimeStamp, self.timeStamp)
            timeStampString = finalTimeStamp[0].strftime("%Y_%m_%d_%H") + "_to_" + finalTimeStamp[-1].strftime("%Y_%m_%d_%H")

        elif (self.meanFreq == "daily"): 
            self.finalTimeStamp = np.append(self.finalTimeStamp, self.timeStamp.date()) # keeping as Pandas to keep date
            timeStampString = self.timeStamp[0].strftime("%Y_%m_%d") + "_to_" + self.timeStamp[-1].strftime("%Y_%m_%d")

        self.countSave += 1 # updating count 

        if(self.countSave == timeDimLength):
            # do you have the full amount ready to save 
            ds.attrs["OPA"] = "daily mean calculated using one-pass algorithm"
            attrs = ds.attrs
            dm = self._createDataSet(self.dmSave, self.finalTimeStamp, ds, attrs)
            self._saveOutput(dm, timeStampString)





    def _saveOutput(self, dm, timeStampString):


        if (hasattr(self, 'var')): # if there are multiple variables in the file 
            # needs to convert from dataSet to a dataArray 
            fileName = self.filePathSave + timeStampString + "_" + self.var + "_" + self.meanFreq + "_mean.nc" 
        else: 
            fileName = self.filePathSave + timeStampString + "_" + dm.cfVarName + "_" + self.meanFreq  + "_mean.nc" 
        # + self.cfVarName 

        #fileName = self.filePathSave + "_" + self.meanFreq + str(finalTimeStamp) + "_mean.nc" 
        dm.to_netcdf(path = fileName, mode ='w') # will re-write the file if it is already there
        dm.close() 
        print('finished saving')



    def mean(self, ds): # acutal function that you call 
        
        # STEPS 
        # 1. check time stamp of data at set nCount etc.
        # 2. check how many time stamps are in the data 
        # 3. actual algorithm 
        # 4. write algorithm 

        # check the time stamp and if the data needs to be reset 
        self._checkTimeStamp(ds)


        if (hasattr(self, 'var')): # if there are multiple variables in the file 
            # needs to convert from dataSet to a dataArray 
            ds = self._checkVariable(ds)


        # try:
        #     doStuff(a.property)
        # except AttributeError:
        #     otherStuff()


        timeNum = self._checkNumTimeStamps(ds) # this checks if there are multiple time stamps in a file and will do np.mean
        howMuch = (self.nData - self.count)

        if (timeNum == 1):
            # convert array to numpy 
            dsNp = self._convertNumpy(ds) # this removes redundant singular dimensions         
            # actually adding to the mean 
            self._update(dsNp)
        
        # will this span over a new statistic? no 
        elif (howMuch >= timeNum):

            # first compute normal mean
            tempMean = self._npMean(ds)
            # remove redundant time dimension 
            dsNp = self._convertNumpy(tempMean)
            # update rolling statistic with weight 
            self._update(dsNp, timeNum)

        # will this span over a new statistic? YES arrrgghhhh
        elif(howMuch < timeNum): 
            # first compute normal mean over the rest of the statistic left to compute 
            tempMean = self._npMean(ds.isel(time=slice(0,howMuch)))
            # remove redundant time dimension 
            dsNp = self._convertNumpy(tempMean)
            # update rolling statistic with weight of the last few days 
            self._update(dsNp, howMuch)

            # need to finish the statistic 


        # when the statistic is full
        if (self.count == self.nData):
        # how to output the data 
            dm, timeStampString = self._dataOutput(ds)

            if(self.save == "true"): # only save if requested 

                # converting save freq into a number
                saveFreqMin = convertTime(timeWord = self.meanFreq)

                if(saveFreqMin < self.meanFreqMin): 
                    print('Saving frequency can not be less than frequency of statistic!')
                    
                elif(saveFreqMin == self.meanFreqMin):                     
                    self._saveOutput(dm, timeStampString)


                elif(saveFreqMin > self.meanFreqMin): 
                    
                    timeDimLength = saveFreqMin / self.meanFreqMin # how many do you need to append 

                    if hasattr(self, 'countSave'):

                        # append data array with new time outputs 
                        self._dataOutputAppend(ds, timeDimLength)
                        
                    else: 
                        self.countSave = 0
                        self.dmSave = ds
                        #self.finalTimeStamp = self.timeStamp
                        self._dataOutputAppend(ds, timeDimLength)

            if(howMuch < timeNum):
                # need to run the mean function again 
                ds = ds.isel(time=slice(howMuch,timeNum))
                meanOPA.mean(self, ds) # calling recursive function 

            return dm # returning dm from the function only if this condtion is met 











