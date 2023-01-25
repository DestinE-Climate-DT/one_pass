# script for unit testing in python 
# test_mean_nextGEMS.py

import unittest
# import relevant libraries 
import numpy as np
import xarray as xr 
import pandas as pd
import glob
from mean_nexGEMS import meanOPA
from datetime import datetime, timedelta



# import xarray as xr 
# import pandas as pd 
# from mean_nexGEMS import meanOPA
# import numpy as np
# import os
# import time
# import glob
# import sys
# from datetime import datetime, timedelta


# then import all the modules that you require for the function 

class TestMeanOPA(unittest.TestCase): 
    
    def test_file(self):

        filePath = "/esarchive/scratch/alacima/python/destination_earth/icon/*.nc" # this has already been re-gridded onto regular lat / lon grid
        filePathOutput = "/esarchive/scratch/kgrayson/git/onepass_development/mean/nexGEMS/tas_daily_means.nc"
        fileList = glob.glob(filePath) # glob function used to just extract the netCDF files 
        fileList.sort() # sorted to get them into the correct order 

        ds = xr.open_dataset(fileList[0]) # open dataset 

        meanFreq = "daily"
        var = "tas"
        save = "false"
 
        # test output of mean function against .nc file 
        self.assertAlmostEqual(meanOPA(ds, meanFreq, var, save), filePathOutput)
        
    def test_mean(self):

        filePath = "/esarchive/scratch/alacima/python/destination_earth/icon/*.nc" # this has already been re-gridded onto regular lat / lon grid
        fileList = glob.glob(filePath) # glob function used to just extract the netCDF files 
        fileList.sort() # sorted to get them into the correct order 

        ds = xr.open_dataset(fileList[0]) # open dataset 

        meanFreq = "daily"
        var = "tas"
        save = "false"
        # test means 
        npMean = np.mean(ds.tas, axis = 0, dtype = np.float64)
        npMean = np.squeeze(npMean)

        decimalPlace = 7
        message = "OPA mean and numpy mean not equal to 7 dp"

        self.assertAlmostEqual(meanOPA(ds, meanFreq, var, save), npMean, decimalPlace, message)
