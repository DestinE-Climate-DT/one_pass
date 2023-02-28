# module imports happen inside here
import numpy as np
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
                save = "false", saveFreq = None, var = None, threshold = None): # should this be **kwargs?  

        self.statistic = statistic 
        self.statFreq = statFreq
        self.outputFreq = outputFreq
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
        self.nData = int(self.statFreqMin/self.timeStep) # number of elements of data that need to be added to the cumulative mean
                
                
        if(self.statistic != "hist" or self.statistic != "percentile"):  
        # potentially don't want zeros for threshold exceedance as well? 
            if ds.chunks is None: 
                value = np.zeros((1, np.size(ds.lat), np.size(ds.lon))) 
            else:
                value = da.zeros((1, np.size(ds.lat), np.size(ds.lon))) # keeping everything as a 3D array

            self.__setattr__(str(self.statistic+"Cum"), value)

            if(self.statistic == "var"):
                # then also need to calculate the mean 
                self.__setattr__("meanCum", value)

            # for the standard deviation need both the mean and variance throughout 
            elif(self.statistic == "std"):
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
        #self.timeStamp = timeStampList[0]
        timeStamp = timeStampPandas[0] # converting to a pandas datetime to calculate if it's the first 
        self.timeStamp = timeStamp
        
        #self.timeStep = (self.timeStep.astype('timedelta64[m]') / np.timedelta64(1, 'm')) # converting timeStep into minutes, float 64

        # converting statistic freq into a number other code 
        self.statFreqMin = convertTime(timeWord = self.statFreq, timeStamp = timeStampList)

        if(self.statFreq == "hourly"): 

            # first thing to check is if this is the first in the series, time stamp must be less than 60 
            timeStampMin = self.timeStamp.minute

            # statFreq < timeStep  
            # this will only work if the one hour is wholly divisable by the timestep 
            if(timeStampMin <= self.timeStep): # this indicates that it's the first data of the day otherwise timeStamp will be larger
                # initalise cumulative statistic array 
                self._initalise(ds)

            # statFreq == timeStep # I THINK THIS IS WRONG? 
            elif(self.statFreqMin == self.timeStep): # the case where timeStep matches statFreq 
                # initalise cumulative statistic array 
                self._initalise(ds)

            # statFreq < timeStep
            elif( self.statFreqMin < self.timeStep):
                # we have a problem 
                print('timeStep too large for hourly statistic')

        if(self.statFreq == "daily"):
            #
            timeStampMin = timeStamp.minute 
            timeStampHour = timeStamp.hour*60 # converting to minutes - 
            timeStampTot = timeStampMin + timeStampHour 

            # first check if the timeStep is less than an hour, in which case need to count the minutes 
            if(self.timeStep < 60):
                # this will only work if the one hour is wholly divisable by the timestep 
                if(timeStampTot <= self.timeStep): # this indicates that it's the first, works when comparing float64 to int
                    # initalise cumulative statistic array 
                    self._initalise(ds)


            elif(self.timeStep < self.statFreqMin): # time step is less than a day 
                # NEED TO FIND A MORE ROBUST WAY OF DOING THIS
                if(timeStampHour == 0): # this indicates that it's the first, works when comparing float64 to int,
                    # initalise cumulative statistic array 
                    self._initalise(ds)


        elif(self.statFreq == "weekly"):
            self.nData = int(self.statFreqMin/self.timeStep) # is there ever not 7 days in a week? 
            # NOT FINISHED 
                #self._initalise(ds) 


        elif(self.statFreq == "monthly"): 
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
        #axNum = ds.get_axis_num('time')
        # first compute normal mean
        temp = ds.resample(time ='1D').mean() # keeps the format (1, lat, lon)
        #tempMean = np.mean(ds, axis = axNum, dtype = np.float64)  # updating the mean with np.mean over the timesteps avaliable 
        #self.tempMean = tempMean
        return temp

    def _npVar(self, ds): # computes np mean

        temp = ds.resample(time ='1D').var(ddof = 1) # keeps the format (1, lat, lon)

        return temp


    def _twoPass(self, ds):

        if (self.statistic == "mean"):
            temp = self._npMean(ds)  

        elif(self.statistic == "var"): 
            temp = self._npMean(ds)  

        elif(self.statistic == "std"): 
            temp = self._npStd(ds)  

        elif(self.statistic == "min"): 
            temp = self._npMin(ds)  

        elif(self.statistic == "max"): 
            temp = self._npMax(ds)  

        return temp 


    #def _convertNumpy(self, ds):
    #    dsNp = np.squeeze(ds) # if there are multiple heights in the same file, this will remove redundant 1 dimensions and .data extracts numpy array
    #    #self.dsNp = dsNp
    #    return dsNp
    
    # actual mean function
    def _updateMean(self, dsNp, weight):# where x is the new value and weight is the new weight 
        self.count += weight
        meanCum = self.meanCum + weight*(dsNp - self.meanCum) / (self.count) # udating mean with one-pass algorithm
        self.meanCum = meanCum.data


    # varience one-pass 
    def _updateVar(self, dsNp, weight):
        
        # storing 'old' mean temporarily 
        tempMean = self.meanCum 

        if(weight == 1):
            self._updateMean(dsNp, weight)
            varCum = self.varCum + weight*(dsNp - tempMean)*(dsNp - self.meanCum) 
        else:
            npMean = self._npMean(dsNp) # two-pass mean 
            self._updateMean(npMean, weight)
            npVar = self._npVar(dsNp)
            # see paper Mastelini. S
            varCum = self.varCum + npVar + np.square(tempMean - npMean)*((self.count - weight)*weight/self.count)
            
        if (self.count == self.nData):
            varCum = varCum/(self.count - 1) # using sample variance NOT population varience 
            
        self.varCum = varCum.data


    def _updateStd(self, dsNp, weight):

        self._updateVar(dsNp, weight)
        stdCum = np.sqrt(self.varCum)
        self.stdCum = stdCum.data


    def _updateMin(self, dsNp):
        self.minCum = where(dsNp < self.minCum, dsNp, self.minCum)

    def _updateMax(self, dsNp):
        self.maxCum = where(dsNp > self.maxCum, dsNp, self.maxCum)


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
            finalStat = self.minCum

        elif(self.statistic == "max"): 
            finalStat = self.maxCum

        if (self.statFreq == "hourly"):
            finalTimeStamp = self.timeStamp
            timeStampString = self.timeStamp.strftime("%Y_%m_%d_%H") 

        elif (self.statFreq == "daily"): 
            finalTimeStamp = self.timeStamp.date()
            timeStampString = self.timeStamp.strftime("%Y_%m_%d")

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

        if (hasattr(self, 'var')): # if there are multiple variables in the file 
            # needs to convert from dataSet to a dataArray 
            ds = self._checkVariable(ds)

        # try:
        #     doStuff(a.property)
        # except AttributeError:
        #     otherStuff()

        timeNum = self._checkNumTimeStamps(ds) # this checks if there are multiple time stamps in a file and will do np.mean
        howMuch = (self.nData - self.count) # how much is let of your statistic to fill 

        if (timeNum == 1):
            self._update(ds)

        # will this span over a new statistic? no 
        elif (howMuch >= timeNum):

            # first compute normal mean
            tempValue = self._twoPass(ds)
            
            # update rolling statistic with weight 
            self._update(ds, timeNum) # FIXXXXXXXX 29.02.23

        # will this span over a new statistic? YES arrrgghhhh
        elif(howMuch < timeNum): 
            # first compute normal mean over the rest of the statistic left to compute 
            tempMean = self._npMean(ds.isel(time=slice(0,howMuch)))
            # update rolling statistic with weight of the last few days 
            self._update(tempMean, howMuch)

            # need to finish the statistic 


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

            if (howMuch < timeNum):
                # need to run the function again 
                ds = ds.isel(time=slice(howMuch,timeNum))
                opa.mean(self, ds) # calling recursive function 

            return dm 


            # GENERAL QUESTIONS: 
            # 1. do you want multiple variables per statistic? easier if not  
            # 2. where do you set the threshold? config file? as input into the function or the initalisation function?
        