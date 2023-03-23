import numpy as np
from numpy import where
import xarray as xr 
import pandas as pd
import glob
from datetime import datetime, timedelta
import yaml
import sys
import dask
import dask.array as da
import os
import importlib
from convertTime import convertTime
from util import load_yaml

class opa: # individual clusters 

    # initalising the function from the ymal config file 
    def __init__(self, statistic = "mean", statFreq = "daily", outputFreq = "daily",
                save = "false", variable = None, threshold = None, configPath =  None): # should this be **kwargs?  

        #saveFreq = None,
        self.statistic = statistic 
        self.statFreq = statFreq
        self.outputFreq = outputFreq
        self.save = save
        self.configPath = configPath

        #self.saveFreq = saveFreq
        self.threshold = threshold # this will only be set for looking at threshold exceedence 

        if(self.statistic == "threshExceed" and threshold == None):
            raise Exception('need to provide threshold of exceedance value')

        if (variable != None):
            self.variable = variable 

        config = load_yaml(self.configPath)

        self.timeStep = config["timeStep"] # this is int value in minutes 
        self.filePathSave = config["filePathSave"]


    def _initalise(self, ds, timeStampTot): 
        # only initalise cumulative mean if you know this is the first input
        self.count = 0
        # calculated by MIN freq of stat / timestep min of data
        if ((self.statFreqMin/self.timeStep).is_integer()):

            if(timeStampTot == 0):
                self.nData = int(self.statFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean
            else:
                if((self.timeStep/timeStampTot).is_integer()):# THIS SHOULD BE GREATER THAN 1
                    self.nData = int(self.statFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean
                else:
                    raise Exception('Timings of input data span over new statistic')
                    # POTENTIALLY SHOULD ALSO RAISE EXPECTION HERE
                    #print('WARNING: timings of input data span over new statistic')
                    #self.nData = int(self.statFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean

        else: 
            # we have a problem 
            raise Exception('Frequency of the requested statistic (e.g. daily) must be wholly divisable by the timestep (dt) of the input data')
       
                
        if(self.statistic != "hist" or self.statistic != "percentile"):  
        # potentially don't want zeros for threshold exceedance as well? 
            if ds.chunks is None: 
                value = np.zeros((1, np.size(ds.lat), np.size(ds.lon))) 
            else:
                
                value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array
                #, coords = ds.coords, dims= ds.dims
            self.__setattr__(str(self.statistic+"Cum"), value)

            if(self.statistic == "var"):
                # then also need to calculate the mean 
                self.__setattr__("meanCum", value)

            # for the standard deviation need both the mean and variance throughout 
            elif(self.statistic == "std"): # can reduce storage here by not saving both the cumulative variance and std
                self.__setattr__("meanCum", value)
                self.__setattr__("varCum", value)

            elif(self.statistic == "min" or self.statistic == "max"):
                self.__setattr__("timings", value)

            # else loop for histograms of percentile calculations that may require a different intital grid
        else: # NEED TO CHANGE THIS! 
            if ds.chunks is None: 
                value = np.zeros((1, np.size(ds.lat), np.size(ds.lon))) 
            else:
                value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array

    def _checknData(self):
        try:
            getattr(self, "nData")
        except AttributeError:
            raise Exception('cannot start required statistic without the initial data')
        
    def _checkTimeStamp(self, ds):
    # function to calculate: 
    # - nData (number of pieces of information (GRIB messages) required to make up the required statistic)
    # - count (how far through the statistic you are, i.e. count = 5 if you've read 5 of the required GRIB messages)
    # - initalises empty array for the statistic 
    # all based on timeStamp and timeStep 

        timeStampList = sorted(ds.time.data) # assuming that incoming data has a time dimension
        timeStampPandas = [pd.to_datetime(x) for x in timeStampList]
        timeStamp = timeStampPandas[0] # converting to a pandas datetime to calculate if it's the first 
        self.timeStamp = timeStamp
        
        # converting statistic freq into a number 'other code -  convertTime' 
        self.statFreqMin = convertTime(timeWord = self.statFreq, timeStampInput = timeStamp)

        # statFreq < timeStep
        if(self.statFreqMin < self.timeStep):
            # we have a problem 
            raise Exception('timeStep too large for requested statistic')
        

        if(self.statFreq == "hourly"): 
            # first thing to check is if this is the first in the series, time stamp must be less than 60 
            timeStampMin = self.timeStamp.minute

            # statFreq >= timeStep  
            if(timeStampMin <= self.timeStep): # this indicates that it's the first data of the hour otherwise timeStamp will be larger
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampMin)
            else:
                self._checknData()

        if(self.statFreq == "3hourly"): 
            # first thing to check is if this is the first in the series, time stamp must be less than 3*60 
            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour # converting to minutes - 
            if(np.mod(timeStampHour, 3) != 0): 
                timeStampTot = timeStampMin + timeStampHour*60
            else:
                timeStampTot = timeStampMin

            # statFreq >= timeStep  
            # just less than otherwise timestep 00:00 and 01:00 would both initalise
            if(timeStampTot < self.timeStep): 
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)
            else:
                self._checknData()

        if(self.statFreq == "6hourly"): 
            # first thing to check is if this is the first in the series, time stamp must be less than 6*60 
            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour # converting to minutes - 
            if(np.mod(timeStampHour, 6) != 0): 
                timeStampTot = timeStampMin + timeStampHour*60
            else:
                timeStampTot = timeStampMin

            # statFreq >= timeStep  
            # just less than otherwise timestep 00:00 and 01:00 would both initalise
            if(timeStampTot < self.timeStep): 
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)
            else:
                self._checknData()

        if(self.statFreq == "daily"):
            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampTot = timeStampMin + timeStampHour 

            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampTot < self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)
            else:
                self._checknData()

        
        elif(self.statFreq == "weekly"): 

            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampDay = (timeStamp.day_of_week)*24*60
            timeStampTot = timeStampMin + timeStampHour + timeStampDay 

            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampTot < self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)
            
            else:
                self._checknData()


        if(self.statFreq == "monthly"): 

            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampDay = (timeStamp.day-1)*24*60
            timeStampMonth = timeStamp.month * timeStamp.days_in_month
            timeStampTot = timeStampMin + timeStampHour + timeStampDay + timeStampMonth

            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampTot < self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)
            
            else:
                self._checknData()

        if(self.statFreq == "annually"): 

            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampDay = (timeStamp.day-1)*24*60
            timeStampTot = timeStampMin + timeStampHour + timeStampDay

            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampTot < self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)
            
            else:
                self._checknData()


    def _checkVariable(self, ds): 

        try:
            getattr(ds, "data_vars") # this means it a dataSet
            self.dataSetAttr = ds.attrs #keeping the attributes of the full dataSet to append to the final dataSet      
            try:
                ds = getattr(ds, self.variable)
            except AttributeError:
                raise Exception('If passing dataSet need to provide variable, opa can only use one variable at the moment')
        except AttributeError:
            pass # data already at dataArray 

        return ds


    def _checkNumTimeStamps(self, ds):
        # checking to see how many time stamps are actually in the file, it's possible that the GSV interface will have mutliple messages 
        timeStampList = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps 
        timeNum = np.size(timeStampList)
        
        return timeNum


    def _twoPassMean(self, ds): # computes normal mean using two pass 
        axNum = dsNp.get_axis_num('time')
        temp = np.mean(ds, axis = axNum, dtype = np.float64, keepdims=True)  # updating the mean with np.mean over the timesteps avaliable 
        #temp = ds.resample(time ='1D').mean() # keeps the format (1, lat, lon)

        return temp

    def _twoPassVar(self, ds): # computes sample (ddof = 1) varience using two pass 
        #temp = ds.resample(time ='1D').var(ddof = 1) # keeps the format (1, lat, lon)
        axNum = dsNp.get_axis_num('time')
        temp = np.var(ds, axis = axNum, dtype = np.float64, keepdims=True, ddof = 1)  # updating the mean with np.mean over the timesteps avaliable 
        
        
        return temp

    
    # actual mean function
    def _updateMean(self, dsNp, weight):# where x is the new value and weight is the new weight 
        self.count += weight
        
        if (weight == 1): 
            meanCum = self.meanCum + weight*(dsNp - self.meanCum) / (self.count) # udating mean with one-pass algorithm
        else:
            tempMean = self._twoPassMean(dsNp) # compute two pass mean first 
            meanCum = self.meanCum + weight*(tempMean - self.meanCum) / (self.count) # udating mean with one-pass algorithm

        self.meanCum = meanCum.data


    # varience one-pass 
    def _updateVar(self, dsNp, weight):
        
        # storing 'old' mean temporarily 
        oldMean = self.meanCum 

        if(weight == 1):
            self._updateMean(dsNp, weight)
            varCum = self.varCum + weight*(dsNp - oldMean)*(dsNp - self.meanCum) 
        else:
            tempMean = self._twoPassMean(dsNp) # two-pass mean 
            self._updateMean(tempMean, weight) # update self.meanCum 
            tempVar = self._twoPassVar(dsNp) # two pass varience 
            # see paper Mastelini. S
            varCum = self.varCum + tempVar + np.square(oldMean - tempMean)*((self.count - weight)*weight/self.count)
            
        if (self.count == self.nData):
            varCum = varCum/(self.count - 1) # using sample variance NOT population varience 
            
        self.varCum = varCum.data



    def _updateStd(self, dsNp, weight): 
        # can reduce storage here if you choose not to have specific varCum and stdCum names 
        self._updateVar(dsNp, weight)
        if (self.count == self.nData):
            self.stdCum = np.sqrt(self.varCum)


    def _updateMin(self, dsNp, weight):
        # creating array of timestamps that corresponds to the min value 
        if(weight == 1):
            timestamp = np.datetime_as_string((dsNp.time.values[0]))
            dsTime = xr.zeros_like(dsNp)
            dsTime = dsTime.where(dsTime != 0, timestamp)

        else:
            axNum = dsNp.get_axis_num('time')
            timings = dsNp.time
            minIndex = dsNp.argmin(axis = axNum, keep_attrs = False)
            #self.minIndex = minIndex 
            dsNp = np.amin(dsNp, axis = axNum, keepdims = True)
            dsTime = xr.zeros_like(dsNp) # now this will have dimensions 1,lat,lon

            for i in range(0, weight):
                #self.i = i
                #self.timestamp = dsNp.time.values[i]
                timestamp = np.datetime_as_string((timings.values[i]))
                dsTime = dsTime.where(minIndex != i, timestamp)  

        if(self.count > 0):
            self.minCum['time'] = dsNp.time
            self.timings['time'] = dsNp.time
            dsTime = dsTime.where(dsNp < self.minCum, self.timings)
            # this gives the new self.minCum number when the  condition is FALSE (location at which to preserve the objects values)
            dsNp = dsNp.where(dsNp < self.minCum, self.minCum)

        dsTime = dsTime.astype('datetime64[ns]') # convert to datetime64 for saving 
        
        #self.dsNp = dsNp
        self.count += weight
        self.minCum = dsNp #running this way around as Array type does not have the function .where, this only works for dataArray
        self.timings = dsTime

        return 


    def _updateMax(self, dsNp,weight):
        
        if(weight == 1):
            timestamp = np.datetime_as_string((dsNp.time.values[0]))
            dsTime = xr.zeros_like(dsNp)
            dsTime = dsTime.where(dsTime != 0, timestamp)
        else:
            axNum = dsNp.get_axis_num('time')
            timings = dsNp.time
            maxIndex = dsNp.argmax(axis = axNum, keep_attrs = False)
            self.maxIndex = maxIndex 
            dsNp = np.amax(dsNp, axis = axNum, keepdims = True)
            dsTime = xr.zeros_like(dsNp) # now this will have dimensions 1,lat,lon

            for i in range(0, weight):
                timestamp = np.datetime_as_string((timings.values[i]))
                dsTime = dsTime.where(maxIndex != i, timestamp)  
        
        if(self.count > 0):
            self.maxCum['time'] = dsNp.time
            self.timings['time'] = dsNp.time
            dsTime = dsTime.where(dsNp > self.maxCum, self.timings)
            # this gives the new self.maxCum number when the  condition is FALSE (location at which to preserve the objects values)
            dsNp = dsNp.where(dsNp > self.maxCum, self.maxCum)

        dsTime = dsTime.astype('datetime64[ns]') # convert to datetime64 for saving 
        
        self.count += weight
        self.maxCum = dsNp #running this way around as Array type does not have the function .where, this only works for dataArray
        self.timings = dsTime
        
        return 


    def _updateThreshold(self, dsNp, weight):
        
        if(weight > 1):
            
            dsNp = dsNp.where(dsNp < self.threshold, 1)
            dsNp = dsNp.where(dsNp >= self.threshold, 0)
            #dsNp = dsNp.sum(dim = "time")
            dsNp = np.sum(dsNp, axis = 0, keepdims = True) #try slower np version that preserves dimensions 
            dsNp = self.threshExceedCum + dsNp

        else:
            if(self.count > 0):
                self.threshExceedCum['time'] = dsNp.time

            dsNp = dsNp.where(dsNp < self.threshold, self.threshExceedCum + 1)
            dsNp = dsNp.where(dsNp >= self.threshold, self.threshExceedCum)

        self.count += weight
        self.threshExceedCum = dsNp #running this way around as Array type does not have the function .where, this only works for dataArray
        
        return 


    def _update(self, ds, weight=1):
        
        if (self.statistic == "mean"):
            self._updateMean(ds, weight) # sometimes called with weights and sometimes not 

        elif(self.statistic == "var"): 
            self._updateVar(ds, weight)

        elif(self.statistic == "std"): 
            self._updateStd(ds, weight)

        elif(self.statistic == "min"): 
            self._updateMin(ds, weight)

        elif(self.statistic == "max"): 
            self._updateMax(ds, weight)

        elif(self.statistic == "threshExceed"): 
            self._updateThreshold(ds, weight)

        #elif(self.statistic == "percentile"):
        # run tdigest


    def _createDataSet(self, finalStat, finalTimeStamp, ds):

        # converting the mean into a new dataArray 
        dm = xr.Dataset(
        data_vars = dict(
                [(ds.name, (["time","lat","lon"], finalStat, ds.attrs))],    # need to add variable attributes 
            ),
        coords = dict(
            #time = (["time"], [pd.to_datetime(finalTimeStamp)]),
            time = (["time"], [finalTimeStamp], ds.time.attrs),
            lon = (["lon"], ds.lon.data, ds.lon.attrs),
            lat = (["lat"], ds.lat.data, ds.lat.attrs),
        ),
        attrs = self.dataSetAttr
        )

        if(hasattr(self, 'timings')):
            timingAttrs = {'OPA':'time stamp of ' + str(self.statFreq + " " + self.statistic)}
            dm = dm.assign(timings = (["time","lat","lon"], self.timings.data, timingAttrs))

        return dm 


    def _saveOutput(self, dm, ds, timeStampString):

        if (hasattr(self, 'var')): # if there are multiple variables in the file 
            fileName = self.filePathSave + timeStampString + "_" + self.var + "_" + self.statFreq + "_" + self.statistic + ".nc" 
        else: 
            fileName = self.filePathSave + timeStampString + "_" + ds.name + "_" + self.statFreq  + "_" + self.statistic + ".nc"

        dm.to_netcdf(path = fileName, mode ='w') # will re-write the file if it is already there
        dm.close() 
        print('finished saving')



    def _dataOutput(self, ds):

        if (self.statistic == "mean"):
            finalStat = self.meanCum 

        elif(self.statistic == "var"): 
            finalStat = self.varCum

        elif(self.statistic == "std"): 
            finalStat = self.stdCum

        elif(self.statistic == "min"): 
            finalStat = self.minCum.data

        elif(self.statistic == "max"): 
            finalStat = self.maxCum.data

        elif(self.statistic == "threshExceed"): 
            finalStat = self.threshExceedCum.data
            ds.name = 'exceedance_freq'

        if (self.statFreq == "hourly" or self.statFreq == "3hourly" or self.statFreq == "6hourly"):
            finalTimeStamp = self.timeStamp.to_datetime64().astype('datetime64[h]')
            timeStampString = self.timeStamp.strftime("%Y_%m_%d_T%H") 
            
        elif (self.statFreq == "daily"): 
            finalTimeStamp = self.timeStamp.to_datetime64().astype('datetime64[D]')
            timeStampString = self.timeStamp.strftime("%Y_%m_%d")

        elif (self.statFreq == "weekly"): 
            finalTimeStamp = self.timeStamp.to_datetime64().astype('datetime64[W]')
            timeStampString = self.timeStamp.strftime("%Y_%m_%d")

        elif (self.statFreq == "monthly"): 
            finalTimeStamp = self.timeStamp.to_datetime64().astype('datetime64[M]')
            timeStampString = self.timeStamp.strftime("%Y_%m")

        elif (self.statFreq == "annual"): 
            finalTimeStamp = self.timeStamp.to_datetime64().astype('datetime64[Y]')
            timeStampString = self.timeStamp.strftime("%Y")


        try:
            getattr(self, "dataSetAttr") # if it was originally a data set
        except AttributeError: # only looking at a dataArray
            self.dataSetAttr = ds.attrs #both dataSet and dataArray will have matching attribs

        self.dataSetAttr["OPA"] = str(self.statFreq + " " + self.statistic + " " + "calculated using one-pass algorithm")
        ds.attrs["OPA"] = str(self.statFreq + " " + self.statistic + " " + "calculated using one-pass algorithm")

        dm = self._createDataSet(finalStat, finalTimeStamp, ds)

        return dm, timeStampString, finalTimeStamp



    def _dataOutputAppend(self, dm, ds, timeAppend):

        self.dmOutput = xr.concat([self.dmOutput, dm], "time")
        self.countAppend += 1 # updating count 

        if(self.countAppend == timeAppend and self.save == True):

            if (self.statFreq == "hourly" or self.statFreq == "3hourly" or self.statFreq == "6hourly"):
                self.timeStampString = self.timeStampString + "_to_" + self.timeStamp.strftime("%Y_%m_%d_T%H")

            elif (self.statFreq == "daily"): 
                self.timeStampString = self.timeStampString + "_to_" + self.timeStamp.strftime("%Y_%m_%d")

            elif (self.statFreq == "weekly"): 
                self.timeStampString = self.timeStampString + "_to_" + self.timeStamp.strftime("%Y_%m_%d")
            
            elif (self.statFreq == "monthly"): 
                self.timeStampString = self.timeStampString + "_to_" + self.timeStamp.strftime("%Y_%m")

            elif (self.statFreq == "annually"): 
                self.timeStampString = self.timeStampString + "_to_" + self.timeStamp.strftime("%Y")
           
            self._saveOutput(self.dmOutput, ds, self.timeStampString)

        return self.dmOutput


    def compute(self, ds):

        self._checkTimeStamp(ds) # check the time stamp and if the data needs to be reset 

        ds = self._checkVariable(ds) # convert from a dataSet to a dataArray if required
        
        weight = self._checkNumTimeStamps(ds) # this checks if there are multiple time stamps in a file and will do two pass statistic
        howMuchLeft = (self.nData - self.count) # how much is let of your statistic to fill 

        if (weight == 1 or howMuchLeft >= weight): # will not span over new statistic 

            self._update(ds, weight)  # update rolling statistic with weight 

        elif(howMuchLeft < weight): # will this span over a new statistic?

            dsLeft = ds.isel(time=slice(0,howMuchLeft)) # extracting time until the end of the statistic 
            
            # update rolling statistic with weight of the last few days 
            self._update(dsLeft, howMuchLeft)
            # still need to finish the statistic (see below)

        if (self.count == self.nData):  # when the statistic is full

        # how to output the data as a dataSet 
            dm, timeStampString, finalTimeStamp = self._dataOutput(ds)
            
            # if there's more to compute? 
            if (howMuchLeft < weight):
                # need to run the function again 
                ds = ds.isel(time=slice(howMuchLeft, weight))
                opa.compute(self, ds) # calling recursive function 
            
            # converting output freq into a number
            outputFreqMin = convertTime(timeWord = self.outputFreq, timeStampInput = self.timeStamp)
            # eg. how many days requested 7 days of saving with daily data
            timeAppend = outputFreqMin / self.statFreqMin # how many do you need to append 
            self.timeAppend = timeAppend
            #if(self.save == True): # only save if requested 

            if(timeAppend < 1): #outputFreqMin < self.statFreqMin
                print('Output frequency can not be less than frequency of statistic!')
                
            elif(timeAppend == 1): #outputFreqMin == self.statFreqMin
                if(self.save == True):
                    self._saveOutput(dm, ds, timeStampString)
                return dm 
            
            elif(timeAppend > 1): #outputFreqMin > self.statFreqMin
                
                if (hasattr(self, 'countAppend') == False or self.countAppend == 0):
                    self.countAppend = 1
                    self.dmOutput = dm # storing the dataSet ready for appending 
                    self.finalTimeStamp = finalTimeStamp 
                    self.timeStampString = timeStampString 
                else:
                    # append data array with new time outputs 
                    dm = self._dataOutputAppend(dm, ds, timeAppend)
                   

            if(self.countAppend == timeAppend):
                self.countAppend = 0
                delattr(self, "finalTimeStamp")
                delattr(self, "timeStampString")
                return dm
