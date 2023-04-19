# script for unit testing in python 
# test_run_mean_OPA.py

import unittest
import xarray as xr
import numpy as np 
import glob 
import os 
import sys 

#os.chdir("/home/bsc32/bsc32263/git/one_pass")
from one_pass.opa import *
from one_pass.opa import Opa

os.chdir('/home/b/b382291/git/AQUA/') 
sys.path.insert(0, '/home/b/b382291/git/AQUA/')

from aqua import Reader
from aqua.reader import catalogue

#### get data from Levante#### 
reader = Reader(model="IFS", exp="tco2559-ng5", source="ICMGG_atm2d", regrid="r020")
data = reader.retrieve(fix=False)
data = reader.regrid(data)
data = data.es 

#### reading some data from disk on nord3 #### 
# file_path_data = "/home/bsc32/bsc32263/git/one_pass/uas_10_months.nc"
# fileList = glob.glob(file_path_data) 
# fileList.sort() 
# data = xr.open_dataset(fileList[0])  # , chunks = 'auto') # open dataset
# data = data.astype(np.float64)
# data = data.uas

def two_pass_mean(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    #np_mean = ds1.resample(time ='1D').mean()
    axNum = ds.get_axis_num('time')
    np_mean = np.mean(ds, axis = axNum, dtype = np.float64, keepdims = True)
    return np_mean
        
def two_pass_std(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_std = np.std(ds, axis = axNum, dtype = np.float64, ddof = 1, keepdims = True) 
    return np_std

def two_pass_var(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_std = np.var(ds, axis = axNum, dtype = np.float64, ddof = 1, keepdims = True)  
    return np_std

def two_pass_min(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_min = np.min(ds, axis = axNum, keepdims = True)  
    return np_min

def two_pass_max(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_min = np.max(ds, axis = axNum, keepdims = True)
    return np_min

class test_opa(unittest.TestCase): 

    def test_mean(self): 

        n_start = 0 
        n_data = 24

        pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": False,
        "checkpoint": True,
        "checkpoint_file": "/home/b/b382291/git/data/checkpoint_mean_daily.nc",
        "out_file": "/home/b/b382291/git/data/"}

        for i in range(n_start, n_data, 1): 
            ds = data.isel(time=slice(i,i+1)) # extract moving window
            daily_mean = Opa(pass_dic)
            dm = daily_mean.compute(ds)

        ## run two pass mean 
        np_mean = two_pass_mean(data, n_data)

        dec_place = 1e-10
        message = "OPA mean and numpy mean not equal to " + str(dec_place) + " dp"
        
        is_equal = np.allclose(dm.es, np_mean, rtol = dec_place, atol = 0)
        
        self.assertTrue(is_equal, message)

    
    def test_std(self): 

        n_start = 288 
        n_data = n_start + 29*24 
        
        pass_dic = {"stat": "std",
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "es",
        "save": False,
        "checkpoint": True,
        "checkpoint_file": "/home/b/b382291/git/data/checkpoint_std_monthly.nc",
        "out_file": "/home/b/b382291/git/data/"}

        for i in range(n_start, n_data, 1): 

            ds = data.isel(time=slice(i,i+1)) 
            monthly_std = Opa(pass_dic)
            dm = monthly_std.compute(ds)

        ## run two pass mean 
        np_std = two_pass_std(data, n_start, n_data)

        dec_place = 1e-10
        message = "OPA std and numpy std not equal to " + str(dec_place) + " dp"
        
        is_equal = np.allclose(dm.es, np_std, rtol = dec_place, atol = 0)
        self.assertTrue(is_equal, message)


    
    def test_min(self): 

        n_start = 288 
        n_data = n_start + 29*24 

        pass_dic = {"stat": "min",
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "es",
        "save": False,
        "checkpoint": True,
        "checkpoint_file": "/home/b/b382291/git/data/checkpoint_min_monthly.nc",
        "out_file": "/home/b/b382291/git/data/"}

        for i in range(0, n_data, 1): 

            ds = data.isel(time=slice(i,i+1)) 
            weekly_min = Opa(pass_dic)
            dm = weekly_min.compute(ds)

        ## run two pass min
        np_min = two_pass_min(data, n_start, n_data)

        dec_place = 1e-10
        message = "OPA min and numpy min not equal to " + str(dec_place) + " dp"
        
        is_equal = np.allclose(dm.es, np_min, rtol = dec_place, atol = 0)
        self.assertTrue(is_equal, message)


    def test_max(self): 

        n_start = 288 
        n_data = n_start + 29*24 

        pass_dic = {"stat": "max",
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "es",
        "save": False,
        "checkpoint": True,
        "checkpoint_file": "/home/b/b382291/git/data/checkpoint_max_monthly.nc",
        "out_file": "/home/b/b382291/git/data/"}

        for i in range(0, n_data, 1): 

            ds = data.isel(time=slice(i,i+1)) 
            monthly_max = Opa(pass_dic)
            dm = monthly_max.compute(ds)

        ## run two pass max
        np_max = two_pass_max(data, n_start, n_data)

        dec_place = 1e-10
        message = "OPA max and numpy max not equal to " + str(dec_place) + " dp"
        
        is_equal = np.allclose(dm.es, np_max, rtol = dec_place, atol = 0)
        self.assertTrue(is_equal, message)


if __name__ == '__main__':
    unittest.main()

