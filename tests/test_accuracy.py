# script for unit testing in python 
# test_run_mean_OPA.py

import unittest
import xarray as xr
import numpy as np 
import glob 
import os 
import sys 

path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(path)
os.chdir(path)

from one_pass.opa import *
from one_pass.opa import Opa

############### load data ##################################

# os.chdir('/home/b/b382291/git/AQUA/') 
# sys.path.insert(0, '/home/b/b382291/git/AQUA/')
# from aqua import Reader
# from aqua.reader import catalogue

# #### get data from Levante#### 
# reader = Reader(model="IFS", exp="tco2559-ng5", source="ICMGG_atm2d", regrid="r020")
# data = reader.retrieve(fix=False)
# data = reader.regrid(data)
# data = data.es 

#### reading some data from disk on nord3 #### 
file_path_data = os.path.realpath(os.path.join(os.path.dirname(__file__), 'uas_10_months.nc'))

fileList = glob.glob(file_path_data) 
fileList.sort() 
data = xr.open_dataset(fileList[0])  # , chunks = 'auto') # open dataset
data = data.astype(np.float64)

############################# define functions ######################################

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

#################################### define unit tests ###################################

class test_opa(unittest.TestCase): 

    def test_mean(self): 

        n_start = 2*30*24 
        n_data = n_start + 24 # + 3*30*24 + 2*24

        pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "uas",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}

        for i in range(n_start, n_data, 1): 
            ds = data.isel(time=slice(i,i+1)) # extract moving window
            daily_mean = Opa(pass_dic)
            dm = daily_mean.compute(ds)

        ## run two pass mean 
        data_arr = getattr(data, pass_dic["variable"])
        np_mean = two_pass_mean(data_arr, n_start, n_data)

        dec_place = 1e-10
        message = "OPA mean and numpy mean not equal to " + str(dec_place) + " dp"
    
        # the OPA will produce a dataSet to keep metadata to reducing down to dataArray
        dm = getattr(dm, pass_dic["variable"])

        is_equal = np.allclose(dm, np_mean, rtol = dec_place, atol = 0)
        
        self.assertTrue(is_equal, message)

    
    def test_std(self): 

        n_start = 2*30*24 
        n_data = n_start + 31*24 # 3*30*24 + 2*24 
        
        pass_dic = {"stat": "std",
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "uas",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests",
        "out_filepath": "tests/"}

        for i in range(n_start, n_data, 1): 

            ds = data.isel(time=slice(i,i+1)) 
            monthly_std = Opa(pass_dic)
            dm = monthly_std.compute(ds)

        ## run two pass mean 
        data_arr = getattr(data, pass_dic["variable"])
        np_std = two_pass_std(data_arr, n_start, n_data)

        dec_place = 1e-10
        message = "OPA std and numpy std not equal to " + str(dec_place) + " dp"
        
        dm = getattr(dm, pass_dic["variable"])

        is_equal = np.allclose(dm, np_std, rtol = dec_place, atol = 0)
        self.assertTrue(is_equal, message)


    
    def test_min(self): 


        n_start = 2*30*24 
        n_data = n_start + 24 # 3*30*24 + 2*24 

        pass_dic = {"stat": "min",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "uas",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}

        for i in range(n_start, n_data, 1): 

            ds = data.isel(time=slice(i,i+1)) 
            weekly_min = Opa(pass_dic)
            dm = weekly_min.compute(ds)

        ## run two pass min
        data_arr = getattr(data, pass_dic["variable"])
        np_min = two_pass_min(data_arr, n_start, n_data)

        dec_place = 1e-10
        message = "OPA min and numpy min not equal to " + str(dec_place) + " dp"
        
        dm = getattr(dm, pass_dic["variable"])

        is_equal = np.allclose(dm, np_min, rtol = dec_place, atol = 0)
        self.assertTrue(is_equal, message)


    def test_max(self): 

        n_start = 2*30*24 
        n_data = n_start + 31*24 #  3*30*24 + 2*24 

        pass_dic = {"stat": "max",
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "uas",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests",
        "out_filepath": "tests/"}

        for i in range(n_start, n_data, 1): 

            ds = data.isel(time=slice(i,i+1)) 
            monthly_max = Opa(pass_dic)
            dm = monthly_max.compute(ds)

        ## run two pass max
        data_arr = getattr(data, pass_dic["variable"])
        np_max = two_pass_max(data_arr, n_start, n_data)

        dec_place = 1e-10
        message = "OPA max and numpy max not equal to " + str(dec_place) + " dp"
        
        dm = getattr(dm, pass_dic["variable"])

        is_equal = np.allclose(dm, np_max, rtol = dec_place, atol = 0)
        self.assertTrue(is_equal, message)


if __name__ == '__main__':
    unittest.main()

