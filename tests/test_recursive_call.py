# script for unit testing in python
# test_run_mean_OPA.py

import glob
import os
import sys
import unittest
import dask

import numpy as np
import xarray as xr

path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)
os.chdir(path)

from one_pass.opa import Opa

############### load data ##################################
#### reading some data from disk on nord3 #### 
file_path_data = os.path.realpath(os.path.join(os.path.dirname(__file__), 'pr_12_months.nc'))

fileList = glob.glob(file_path_data) 
fileList.sort() 
if len(fileList) == 0:
    exit("ERROR: input file is missing. Please download using provided script (tests/get_data.sh)")
print(fileList[0])
# Error out early if test data is missing
if not os.path.isfile(fileList[0]):
    exit("ERROR: input file is missing. Please download using provided script (tests/get_data.sh)")
data = xr.open_dataset(fileList[0], engine='netcdf4') # , chunks = 'auto') # open dataset
data = data.compute()
data = data.astype(np.float64)

dec_place = 1e-6
dec_place_per = 1e-2
############################# define functions ######################################


def two_pass_mean(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data))
    # np_mean = ds1.resample(time ='1D').mean()
    axNum = ds.get_axis_num("time")
    np_mean = np.mean(ds, axis=axNum, dtype=np.float64, keepdims=True)
    return np_mean


def two_pass_std(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data))
    axNum = ds.get_axis_num("time")
    np_std = np.std(ds, axis=axNum, dtype=np.float64, ddof=1, keepdims=True)

    return np_std


def two_pass_var(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data))
    axNum = ds.get_axis_num("time")
    np_var = np.var(ds, axis=axNum, dtype=np.float64, ddof=1, keepdims=True)

    return np_var


def two_pass_min(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data))
    axNum = ds.get_axis_num("time")
    np_min = np.min(ds, axis=axNum, keepdims=True)

    return np_min

def two_pass_max(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data))
    axNum = ds.get_axis_num("time")
    np_max = np.max(ds, axis=axNum, keepdims=True)

    return np_max

def two_pass_percentile(data, n_start, n_data, perc_list):

    ds = data.isel(time=slice(n_start, n_data))
    axNum = ds.get_axis_num("time")
    np_percentile = np.percentile(ds, perc_list, axis=axNum)

    return np_percentile

def two_pass_sum(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_sum = np.sum(ds, axis = axNum, keepdims = True)
    
    return np_sum 

def duration_pick(time_unit, time_step):
    durations  = [5,10,15,20,30,45,60,90,120,180,240,360,540,720,
                  1080,1440,2880,4320,5760,7200,8640, 10080] 
    
    if time_unit != 'minutes':
        durations = [d/60 for d in durations]

    durations = [d for d in durations if d >= time_step]
    for d in durations:
        if d%time_step != 0:
            durations.remove(d)  
    durations = list(map(int, durations))
    return durations

#################################### define opa test ###################################

def opa_stat_no_checkpoint(n_start, n_data, step, pass_dic):

    opa_stat = Opa(pass_dic)
    for i in range(n_start, n_data, step):

        ds = data.isel(time=slice(i, i + step))
        dm = opa_stat.compute(ds)

    # dm = getattr(dm, pass_dic["variable"])
    dm = getattr(dm, pass_dic["variable"])

    return dm

def opa_stat_with_checkpoint(n_start, n_data, step, pass_dic):

    for i in range(n_start, n_data, step):

        opa_stat = Opa(pass_dic)
        ds = data.isel(time=slice(i, i + step))
        dm = opa_stat.compute(ds)

    # dm = getattr(dm, pass_dic["variable"])
    dm = getattr(dm, pass_dic["variable"])

    if pass_dic["stat"] == "percentile":
        dm = dm[0, :, :]

    return dm

####################### py tests ##############################

def test_mean():

    n_start = 0
    n_data = n_start + 24  + 13 # + 3*30*24 + 2*24
    step = 26

    pass_dic = {"stat": "mean",
    "stat_freq": "daily_noon",
    "output_freq": "daily_noon",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    data_arr = getattr(data, pass_dic["variable"])

    message = (
        "OPA "
        + str(pass_dic["stat"])
        + " and numpy "
        + str(pass_dic["stat"])
        + " not equal to "
        + str(dec_place)
        + " dp"
    )

    two_pass = two_pass_mean(data_arr, 13, 24 + 13)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, atol = dec_place), message

def test_std():

    n_start = 0
    n_data = n_start + 24*31*2 + 28*24
    step = 19

    pass_dic = {"stat": "std",
    "stat_freq": "3monthly",
    "output_freq": "3monthly",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}
    
    data_arr = getattr(data, pass_dic["variable"])
    message = (
        "OPA "
        + str(pass_dic["stat"])
        + " and numpy "
        + str(pass_dic["stat"])
        + " not equal to "
        + str(dec_place)
        + " dp"
    )

    two_pass = two_pass_std(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message
    
def test_var():

    n_start = 24*4
    n_data = n_start + 7*24 
    step = 13

    pass_dic = {"stat": "var",
    "stat_freq": "weekly",
    "output_freq": "weekly",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    data_arr = getattr(data, pass_dic["variable"])
    message = (
        "OPA "
        + str(pass_dic["stat"])
        + " and numpy "
        + str(pass_dic["stat"])
        + " not equal to "
        + str(dec_place)
        + " dp"
    )

    two_pass = two_pass_var(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, atol=dec_place), message

def test_sum_monthly():

    n_start = 31*24
    n_data = n_start + 24*28 # 3*30*24 + 2*24 
    step = 240
    
    pass_dic = {"stat": "sum",
    "stat_freq": "monthly",
    "output_freq": "monthly",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    data_arr = getattr(data, pass_dic["variable"])
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + \
        str(pass_dic["stat"]) + " not equal to " + str(dec_place)

    two_pass = two_pass_sum(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, rtol = dec_place, atol = dec_place), message

def test_sum_daily_noon():

    n_start = 40*24 + 13
    n_data = n_start + 24 # 3*30*24 + 2*24 
    step = 1
    
    pass_dic = {"stat": "sum",
    "stat_freq": "daily_noon",
    "output_freq": "daily_noon",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    data_arr = getattr(data, pass_dic["variable"])
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + \
        str(pass_dic["stat"]) + " not equal to " + str(dec_place)

    two_pass = two_pass_sum(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, rtol = dec_place, atol = dec_place), message
