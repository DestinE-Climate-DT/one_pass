# script for unit testing in python
# test_run_mean_OPA.py

import glob
import os
import sys
import unittest

import numpy as np
import xarray as xr

path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)
os.chdir(path)

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
file_path_data = os.path.realpath(os.path.join(os.path.dirname(__file__), 'pr_12_months.nc'))

fileList = glob.glob(file_path_data) 
fileList.sort() 
print(fileList[0])
data = xr.open_dataset(fileList[0], engine='netcdf4') # , chunks = 'auto') # open dataset
data = data.compute()
data = data.astype(np.float64)

dec_place = 1e-6
dec_place_per = 2
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
    
    # 3 months of data 
    n_start = 0
    n_data =  n_start + 31*24*2 + 28*24
    step = 4
    
    pass_dic = {"stat": "mean",
    "stat_freq": "continuous",
    "output_freq": "monthly",
    "percentile_list" : None,
    "thresh_exceed" : None,
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "out_filepath": "tests/"}

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

    two_pass = two_pass_mean(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, atol=dec_place), message


def test_std():

    # 3 months of data 
    n_start = 31*24 + 28*24
    n_data =  n_start + 31*24*2 + 30*24
    step = 6

    pass_dic = {"stat": "std",
    "stat_freq": "continuous",
    "output_freq": "monthly",
    "percentile_list" : None,
    "thresh_exceed" : None,
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests/",
    "out_filepath": "tests/"}
    
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

    assert np.allclose(two_pass, one_pass, atol=dec_place), message


def test_var():

    # 2 months of data 
    n_start = 31*24 + 28*24
    n_data =  n_start + 31*24 + 30*24
    step = 4
    
    pass_dic = {"stat": "var",
    "stat_freq": "continuous",
    "output_freq": "monthly",
    "percentile_list" : None,
    "thresh_exceed" : None,
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "out_filepath": "tests/"}
    
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


def test_max():

    # 2 months of data 
    n_start = 31*24 + 28*24
    n_data =  n_start + 31*24 + 30*24
    step = 4
    
    pass_dic = {"stat": "max",
    "stat_freq": "continuous",
    "output_freq": "monthly",
    "percentile_list" : None,
    "thresh_exceed" : None,
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests/",
    "out_filepath": "tests/"}

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

    two_pass = two_pass_max(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, atol=dec_place), message


def test_min():

    # 4 weeks of data 
    n_start = 4*24
    n_data =  n_start + 7*24*4
    step = 4

    pass_dic = {"stat": "min",
    "stat_freq": "continuous",
    "output_freq": "weekly",
    "percentile_list" : None,
    "thresh_exceed" : None,
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests/",
    "out_filepath": "tests/"}

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

    two_pass = two_pass_min(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, atol=dec_place), message


# def test_min_hourly():

#     n_start = 0
#     n_data = n_start + 1 # 3*30*24 + 2*24
#     step = 1

#     pass_dic = {"stat": "min",
#     "stat_freq": "continuous",
#     "output_freq": "monthly",
#     "percentile_list" : None,
#     "thresh_exceed" : None,
#     "time_step": 60,
#     "variable": "pr",
#     "save": False,
#     "checkpoint": False,
#     "checkpoint_filepath": "tests/",
#     "out_filepath": "tests/"}

#     data_arr = getattr(data, pass_dic["variable"])
#     message = "OPA " + str(pass_dic["stat"]) + " and numpy " + \
#         str(pass_dic["stat"]) + " not equal to " + str(dec_place) + " dp"

#     two_pass = two_pass_min(data_arr, n_start, n_data)
#     one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

#     assert np.allclose(two_pass, one_pass, atol = dec_place), message

# def test_percentile_daily():

#     n_start = 0
#     n_data = n_start + 24 # 3*30*24 + 2*24
#     step = 24

#     pass_dic = {"stat": "percentile",
#     "stat_freq": "continuous",
#     "output_freq": "monthly",
#     "percentile_list" : ["all"],
#     "thresh_exceed" : None,
#     "time_step": 60,
#     "variable": "pr",
#     "save": False,
#     "checkpoint": True,
#     "checkpoint_filepath": "tests/",
#     "out_filepath": "tests/"}

#     data_arr = getattr(data, pass_dic["variable"])
#     message = "OPA " + str(pass_dic["stat"]) + " and numpy " + \
#         str(pass_dic["stat"]) + " not equal to " + str(dec_place_per)

#     percentile_list = (np.linspace(0, 100, 101))
#     two_pass = two_pass_percentile(data_arr, n_start, n_data, percentile_list)
#     one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

#     assert np.allclose(two_pass, one_pass, rtol = dec_place_per, atol = dec_place_per), message
