# script for unit testing in python 
# test_run_mean_OPA.py

import unittest
# import relevant libraries 
import numpy as np
import xarray as xr 
import pandas as pd
import glob
from datetime import datetime, timedelta
import importlib
imported_module = importlib.import_module("meanOPA")
importlib.reload(imported_module)
from meanOPA import *
from meanOPA import meanOPA # from module (file.py) import class name 

#importlib.reload(meanOPA)

#filePath = '/esarchive/scratch/kgrayson/git/one_pass/config.yml'

# then run the meanOPA with the config file 
meanOpa = meanOPA()
ds = xr.open_dataset(meanOpa.filePath) # open dataset 

# this simulates the streaming, at the moment the config file just gives the file path of the data 
for i in range(24): 
#
    ds1 = ds.isel(time=slice(i,i+1)) # slice(start, stop, step), extract 'moving window' which is hourly data
    dm = meanOpa.mean(ds1)

# then import all the modules that you require for the function 

class TestMeanOPA(unittest.TestCase): 

    def test_file(self): # test that dm matches the netCDF output

        # test output of mean function against .nc file, will only run this test if it saves the output eg. config save = true 
        if (meanOpa.save == "true"):
            self.assertAlmostEqual(dm, meanOpa.filePathSave)
    
    # this test compares against the actual mean computes 
    def test_mean(self):

        npMean = np.mean(ds.tas, axis = 0, dtype = np.float64)
        npMean = np.squeeze(npMean)

        decimalPlace = 7
        message = "OPA mean and numpy mean not equal to 7 dp"

        self.assertAlmostEqual(dm, npMean, decimalPlace, message)
