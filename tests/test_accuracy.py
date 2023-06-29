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

from one_pass.opa import Opa
from one_pass.convert_time import convert_time
from one_pass.check_stat import check_stat
from one_pass import util

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

dec_place = 1e-3

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
    np_var = np.var(ds, axis = axNum, dtype = np.float64, ddof = 1, keepdims = True)  
    
    return np_var

def two_pass_min(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_min = np.min(ds, axis = axNum, keepdims = True)  
    
    return np_min

def two_pass_max(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_max = np.max(ds, axis = axNum, keepdims = True)
    
    return np_max

#################################### define opa test ###################################


def opa_stat_no_checkpoint(n_start, n_data, step, pass_dic): 

    opa_stat = Opa(pass_dic)

    for i in range(n_start, n_data, step): 

        ds = data.isel(time=slice(i,i+step))
        dm = opa_stat.compute(ds)

    #dm = getattr(dm, pass_dic["variable"])
    dm = getattr(dm, pass_dic["variable"] + "_"+ pass_dic["stat"])

    return dm 

def opa_stat_with_checkpoint(n_start, n_data, step, pass_dic): 

    for i in range(n_start, n_data, step): 

        opa_stat = Opa(pass_dic)
        ds = data.isel(time=slice(i,i+step))
        dm = opa_stat.compute(ds)

    #dm = getattr(dm, pass_dic["variable"])
    dm = getattr(dm, pass_dic["variable"] + "_"+ pass_dic["stat"])

    return dm 


####################### py tests ##############################

def test_mean():
    
    n_start = 2*30*24 
    n_data = n_start + 12 # + 3*30*24 + 2*24
    step = 4 
    
    pass_dic = {"stat": "mean",
    "stat_freq": "12hourly",
    "output_freq": "12hourly",
    "time_step": 60,
    "variable": "uas",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests",
    "out_filepath": "tests"}

    data_arr = getattr(data, pass_dic["variable"])
    
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + str(pass_dic["stat"]) + " not equal to " + str(dec_place) + " dp"

    two_pass = two_pass_mean(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message
    
    
    
def test_std():

    n_start = 2*30*24 
    n_data = n_start + 24 
    step = 1

    pass_dic = {"stat": "std",
    "stat_freq": "daily",
    "output_freq": "daily",
    "time_step": 60,
    "variable": "uas",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests",
    "out_filepath": "tests"}
    
    data_arr = getattr(data, pass_dic["variable"])
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + str(pass_dic["stat"]) + " not equal to " + str(dec_place) + " dp"

    two_pass = two_pass_std(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message
    
    
    
def test_var():

    n_start = 24*3
    n_data = n_start + 7*24 
    step = 24

    pass_dic = {"stat": "var",
    "stat_freq": "weekly",
    "output_freq": "weekly",
    "time_step": 60,
    "variable": "uas",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests",
    "out_filepath": "tests"}
    
    data_arr = getattr(data, pass_dic["variable"])
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + str(pass_dic["stat"]) + " not equal to " + str(dec_place) + " dp"

    two_pass = two_pass_var(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message
    
def test_max():

    n_start = 0 
    n_data = n_start + 6 # 3*30*24 + 2*24 
    step = 2
    
    pass_dic = {"stat": "max",
    "stat_freq": "6hourly",
    "output_freq": "6hourly",
    "time_step": 60,
    "variable": "uas",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests",
    "out_filepath": "tests"}

    data_arr = getattr(data, pass_dic["variable"])
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + str(pass_dic["stat"]) + " not equal to " + str(dec_place) + " dp"

    two_pass = two_pass_max(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message
    
def test_min():

    n_start = 0 
    n_data = n_start + 31*24 # 3*30*24 + 2*24 
    step = 24 
    
    pass_dic = {"stat": "min",
    "stat_freq": "monthly",
    "output_freq": "monthly",
    "time_step": 60,
    "variable": "uas",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests",
    "out_filepath": "tests"}

    data_arr = getattr(data, pass_dic["variable"])
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + str(pass_dic["stat"]) + " not equal to " + str(dec_place) + " dp"

    two_pass = two_pass_min(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message