# module imports happen inside here
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
imported_module = importlib.import_module("convertTime")
importlib.reload(imported_module)
from convertTime import convertTime
os.chdir('/home/b/b382291/regridder/AQUA')
from aqua.util import load_yaml




class opa: # individual clusters 

    # initalising the function from the ymal config file 
    def __init__(self, statistic = "mean", statFreq = "daily", outputFreq = "daily",
                save = "false", saveFreq = None, variable = None, threshold = None): # should this be **kwargs?  

        self.statistic = statistic 
        self.statFreq = statFreq
        self.outputFreq = outputFreq
        self.save = save
        self.saveFreq = saveFreq
        self.threshold = threshold # this will only be set for looking at threshold exceedence 

        if (variable != None):
            self.variable = variable 

        filePath = "/home/b/b382291/git/one_pass/config.yml"
        config = load_yaml(filePath)

        self.timeStep = config["timeStep"] # this is int value in minutes 
        self.filePathSave = config["filePathSave"]


    def _initalise(self, ds, timeStampMin): 
        # only initalise cumulative mean if you know this is the first input
        self.count = 0
        # calculated by MIN freq of mean / timestep min of data
        if ((self.statFreqMin/self.timeStep).is_integer()):

            if(timeStampMin == 0):
                self.nData = int(self.statFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean
            else:
                if((self.timeStep/timeStampMin).is_integer()):
                    self.nData = int(self.statFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean
                else:
                    # POTENTIALLY SHOULD ALSO RAISE EXPECTION HERE
                    print('WARNING: timings of input data span over new statistic')
                    self.nData = int(self.statFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean

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

            # else loop for histograms of percentile calculations that may require a different intital grid
        else: # NEED TO CHANGE THIS! 
            if ds.chunks is None: 
                value = np.zeros((1, np.size(ds.lat), np.size(ds.lon))) 
            else:
                value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array

        
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


        if(self.statFreq == "daily"):
            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampTot = timeStampMin + timeStampHour 

            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampTot < self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)

        elif(self.statFreq == "monthly"): 

            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampDay = (timeStamp.day-1)*24*60
            timeStampTot = timeStampMin + timeStampHour + timeStampDay

            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampTot < self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                # initalise cumulative statistic array 
                self._initalise(ds, timeStampTot)
            
            else:
                try:
                    getattr(self, "nData")
                except AttributeError:
                    raise Exception('cannot start required statistic without the initial data')

    # reducing the variable space, if the input is in a dataset it will convert to a dataArray 
    def _checkVariable(self, ds): 
        #elif(self.var == "uas"):
        #    ds = ds.uas
        #elif(self.var == "vas"):
            #ds = ds.vas
        #ds = ds.variable # THIS DOESN'T WORK! 
        #stat1.__getattribute__(b)

        return ds


    def _checkNumTimeStamps(self, ds):
        # checking to see how many time stamps are actually in the file, it's possible that the FDB interface will have mutliple messages 
        timeStampList = sorted(ds.time.data) # this method could be very different for a GRIB file, need to understand time stamps 
        timeNum = np.size(timeStampList)
        #self.timeNum = timeNum
        
        return timeNum


    def _twoPassMean(self, ds): # computes normal mean using two pass 
        temp = ds.resample(time ='1D').mean() # keeps the format (1, lat, lon)
        
        #axNum = ds.get_axis_num('time')
        #tempMean = np.mean(ds, axis = axNum, dtype = np.float64)  # updating the mean with np.mean over the timesteps avaliable 
        #self.tempMean = tempMean
        return temp

    def _twoPassVar(self, ds): # computes sample (ddof = 1) varience using two pass 
        temp = ds.resample(time ='1D').var(ddof = 1) # keeps the format (1, lat, lon)
        return temp


    #def _convertNumpy(self, ds):
    #    dsNp = np.squeeze(ds) # if there are multiple heights in the same file, this will remove redundant 1 dimensions and .data extracts numpy array
    #    #self.dsNp = dsNp
    #    return dsNp
    
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

        # NEED TO INCLUDE TIMESTAMPS HERE 
        if(weight > 1):

            axNum = dsNp.get_axis_num('time')
            dsNp = np.amin(dsNp, axis = axNum, keepdims = True)
            

        if(self.count > 0):
            self.minCum['time'] = dsNp.time
            # this gives the new self.minCum number when the  condition is FALSE (location at which to preserve the objects values)
            dsNp = dsNp.where(dsNp < self.minCum, self.minCum)
        
        self.count += 1
        self.minCum = dsNp #running this way around as Array type does not have the function .where, this only works for dataArray
        
        return 


    def _updateMax(self, dsNp,weight):
        
        if(weight > 1):
            axNum = dsNp.get_axis_num('time')
            dsNp = np.amax(dsNp, axis = axNum, keepdims = True)

        if(self.count > 0):
            self.maxCum['time'] = dsNp.time
            # this gives the new self.maxCum number when the  condition is FALSE (location at which to preserve the objects values)
            dsNp = dsNp.where(dsNp > self.maxCum, self.maxCum)
        
        self.count += 1
        self.maxCum = dsNp #running this way around as Array type does not have the function .where, this only works for dataArray
        
        return 


    def _updateThreshold(self, dsNp, weight):
        
        #if(weight > 1):
        #    axNum = dsNp.get_axis_num('time')
        #    dsNp = np.amax(dsNp, axis = axNum, keepdims = True)

        if(self.count > 0):
            self.threshExceedCum['time'] = dsNp.time

        dsNp = dsNp.where(dsNp < self.threshold, self.threshExceedCum + 1)
        dsNp = dsNp.where(dsNp >= self.threshold, self.threshExceedCum)

        self.count += 1
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


    def _createDataSet(self, finalStat, finalTimeStamp, ds, attrs):

        # converting the mean into a new dataArray 
        dm = xr.Dataset(
        data_vars = dict(
            stat = (["time","lat","lon"], finalStat),    # need to add variable attributes                         
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
        #finalMean = np.expand_dims(self.meanCum, axis=0) # adding back extra time dimension 

        if (self.statistic == "mean"):
            finalStat = self.meanCum # sometimes called with weights and sometimes not 

        elif(self.statistic == "var"): 
            finalStat = self.varCum

        elif(self.statistic == "std"): 
            finalStat = self.stdCum

        elif(self.statistic == "min"): 
            finalStat = self.minCum.data

        elif(self.statistic == "max"): 
            finalStat = self.maxCum # do you NEED .DATA? 


        elif(self.statistic == "threshExceed"): 
            finalStat = self.threshExceedCum.data

        if (self.statFreq == "hourly"):
            finalTimeStamp = self.timeStamp
            timeStampString = self.timeStamp.strftime("%Y_%m_%d_%H") 

        elif (self.statFreq == "daily"): 
            finalTimeStamp = self.timeStamp.date()
            timeStampString = self.timeStamp.strftime("%Y_%m_%d")

        elif (self.statFreq == "monthly"): 
            finalTimeStamp = self.timeStamp.date()
            timeStampString = self.timeStamp.strftime("%Y_%m")

        ds.attrs["OPA"] = str(self.statFreq + "_" + self.statistic + "_" + "calculated using one-pass algorithm")
        attrs = ds.attrs

        dm = self._createDataSet(finalStat, finalTimeStamp, ds, attrs)

        return dm, timeStampString

    def _dataOutputAppend(self, ds, timeDimLength):

        if (self.countSave > 0):
            self.dmSave = np.append(self.dmSave, ds) # appending new time 
        else: 
            self.finalTimeStamp =[] # this needs to be self.

        if (self.statFreq == "hourly"):
            self.finalTimeStamp = np.append(self.finalTimeStamp, self.timeStamp)
            timeStampString = finalTimeStamp[0].strftime("%Y_%m_%d_%H") + "_to_" + finalTimeStamp[-1].strftime("%Y_%m_%d_%H")

        elif (self.statFreq == "daily"): 
            self.finalTimeStamp = np.append(self.finalTimeStamp, self.timeStamp.date()) # keeping as Pandas to keep date
            timeStampString = self.timeStamp[0].strftime("%Y_%m_%d") + "_to_" + self.timeStamp[-1].strftime("%Y_%m_%d")

        self.countSave += 1 # updating count 

        if(self.countSave == timeDimLength):
            # do you have the full amount ready to save 
            ds.attrs["OPA"] = "FREQ AND STATISTIC calculated using one-pass algorithm"
            attrs = ds.attrs
            dm = self._createDataSet(self.dmSave, self.finalTimeStamp, ds, attrs)
            self._saveOutput(dm, timeStampString)





    def _saveOutput(self, dm, timeStampString):


        if (hasattr(self, 'var')): # if there are multiple variables in the file 
            # needs to convert from dataSet to a dataArray 
            fileName = self.filePathSave + timeStampString + "_" + self.var + "_" + self.statFreq + "_STAT.nc" 
        else: 
            fileName = self.filePathSave + timeStampString + "_" + dm.cfVarName + "_" + self.statFreq  + "_STAT.nc" 
        # + self.cfVarName 

        #fileName = self.filePathSave + "_" + self.statFreq + str(finalTimeStamp) + "_STATISTIC.nc" 
        dm.to_netcdf(path = fileName, mode ='w') # will re-write the file if it is already there
        dm.close() 
        print('finished saving')




###### the framework function is going to be the same for all alogirthms with a variable input depending on required statistic 

    def compute(self, ds):

        # STEPS 
        # 1. check time stamp of data and initalise the statistic (creating nData, empty grid and count)
        # 2. check format of the data is DataArray (reduce variables?)
        # 4. write algorithm 

        # check the time stamp and if the data needs to be reset 
        self._checkTimeStamp(ds)

        if (hasattr(self, 'variable')): # if there are multiple variables in the file 
            # needs to convert from dataSet to a dataArray 
            ds = self._checkVariable(ds)
            

        #try:
        #    getattr(self, "var")
        #    ds = self._checkVariable(ds)
        #except AttributeError:
            

        weight = self._checkNumTimeStamps(ds) # this checks if there are multiple time stamps in a file and will do np.mean
        howMuchLeft = (self.nData - self.count) # how much is let of your statistic to fill 

        # will not span over new statistic 
        if (weight == 1 or howMuchLeft >= weight):
            # update rolling statistic with weight 
            self._update(ds, weight) 

        # will this span over a new statistic?
        elif(howMuchLeft < weight): 
            # extracting time until the end of the statistic 
            dsLeft = ds.isel(time=slice(0,howMuchLeft))
            # update rolling statistic with weight of the last few days 
            self._update(dsLeft, howMuchLeft)
            # still need to finish the statistic (see below)


        # when the statistic is full
        if (self.count == self.nData):
        # how to output the data 
            dm, timeStampString = self._dataOutput(ds)
            
            if(self.save == "true"): # only save if requested 

                # converting save freq into a number
                saveFreqMin = convertTime(timeWord = self.statFreq)

                if(saveFreqMin < self.statFreqMin): 
                    print('Saving frequency can not be less than frequency of statistic!')
                    
                elif(saveFreqMin == self.statFreqMin):                     
                    self._saveOutput(dm, timeStampString)


                elif(saveFreqMin > self.statFreqMin): 
                    
                    timeDimLength = saveFreqMin / self.statFreqMin # how many do you need to append 

                    if hasattr(self, 'countSave'):

                        # append data array with new time outputs 
                        self._dataOutputAppend(ds, timeDimLength)
                        
                    else: 
                        self.countSave = 0
                        self.dmSave = ds
                        #self.finalTimeStamp = self.timeStamp
                        self._dataOutputAppend(ds, timeDimLength)

            if (howMuchLeft < weight):
                # need to run the function again 
                ds = ds.isel(time=slice(howMuchLeft, weight))
                opa.compute(self, ds) # calling recursive function 

            return dm 


            # GENERAL QUESTIONS: 
            # 1. do you want multiple variables per statistic? easier if not  
            # 2. where do you set the threshold? config file? as input into the function or the initalisation function?
        
            #if (ds.chunks is None):
             #   self.minCum = self._updateMin(self.minCum, ds)
            #else:
                #tempMin = self.minCum
                #tempDs = ds
                #delayed_result = dask.delayed(self._updateMin)(tempMin, tempDs)
                # to create a dask array to use in the future
                #daskMin = da.from_delayed(delayed_result, dtype=tempMin.dtype, shape=tempMin.shape)
                #self.minCum = daskMin.compute()
                #self.minCum = updateMin

        #self.timeStep = (self.timeStep.astype('timedelta64[m]') / np.timedelta64(1, 'm')) # converting timeStep into minutes, float 64

