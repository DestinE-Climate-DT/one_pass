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

def dm_roller(data, duration): 

    ### compute rolling sum & select maximum
    rolling_sum = data.rolling(time=duration, center=True).sum() 
    rolling_sum_max = rolling_sum.max(dim='time', skipna=True) 
    rolling_sum_max = rolling_sum_max.expand_dims(duration = ([duration])) 

    return rolling_sum_max

def iamser(data, years, durations):
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):

        dmax_list = [] 
        for durs in durations:
            dmax = dm_roller(data, durs) 
            dmax_list.append(dmax)
        iamsfile = xr.concat(dmax_list, dim='duration')

        return iamsfile 

def two_pass_iams(data):

    sel_durs = duration_pick('hours',1) 
    years= [2071] 
    iams = iamser(data, years, sel_durs)

    return iams.values

#################################### define opa test ###################################

def opa_stat_no_checkpoint(n_start, n_data, step, pass_dic):

    opa_stat = Opa(pass_dic)

    if pass_dic["stat"] == "iams":
        data1 = data.pr[:, 0:10, 0:10]
        for i in range(n_start, n_data, step):

            ds = data1.isel(time=slice(i, i + step))
            dm = opa_stat.compute(ds)

    else: 
        for i in range(n_start, n_data, step):

            ds = data.isel(time=slice(i, i + step))
            dm = opa_stat.compute(ds)

    # dm = getattr(dm, pass_dic["variable"])
    dm = getattr(dm, pass_dic["variable"])

    return dm

def opa_stat_with_checkpoint(n_start, n_data, step, pass_dic, keep_checkpoints=False):

    for i in range(n_start, n_data, step):

        opa_stat = Opa(pass_dic, keep_checkpoints=keep_checkpoints)
        ds = data.isel(time=slice(i, i + step))
        dm = opa_stat.compute(ds)

    # dm = getattr(dm, pass_dic["variable"])
    if pass_dic["stat"] != "histogram":
        dm = getattr(dm, pass_dic["variable"])

    if pass_dic["stat"] == "percentile":
        dm = dm[0, :, :]

    return dm

def mean_calc(data, keep_checkpoints=False):

    n_start = 0 
    n_data = (n_start + 24)*31
    n_start_2_pass = n_data - 24
    step = 24
    
    pass_dic = {"stat": "mean",
    "stat_freq": "daily",
    "output_freq": "monthly",
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

    two_pass = two_pass_mean(data_arr, n_start_2_pass, n_data)
    one_pass = opa_stat_with_checkpoint(
        n_start, n_data, step, pass_dic, keep_checkpoints=keep_checkpoints
    )

    return one_pass, two_pass, message

def std_calc(data, keep_checkpoints=False):

    n_start = 0
    n_data = n_start + 24*31*2 + 28*24
    n_start_2_pass = n_data - 24*31
    step = 1

    pass_dic = {"stat": "std",
    "stat_freq": "monthly",
    "output_freq": "3monthly",
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

    two_pass = two_pass_std(data_arr, n_start_2_pass, n_data)
    one_pass = opa_stat_with_checkpoint(
        n_start, n_data, step, pass_dic, keep_checkpoints=keep_checkpoints
    )

    return one_pass, two_pass, message

def min_calc(data):

    n_start = 24*4
    n_data = n_start + 7*24
    n_start_2_pass = n_data - 6
    step = 9

    pass_dic = {"stat": "min",
    "stat_freq": "6hourly",
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

    two_pass = two_pass_min(data_arr, n_start_2_pass, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    return one_pass, two_pass, message

def max_calc(data):

    # starting half way through the week so the final
    # length won't be the full week
    n_start = 0
    n_data = n_start + 7*24
    n_start_2_pass = n_data - 6
    step = 9

    pass_dic = {"stat": "min",
    "stat_freq": "6hourly",
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

    two_pass = two_pass_min(data_arr, n_start_2_pass, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    return one_pass, two_pass, message

def hist_calc(keep_checkpoints=False):

    n_start = 4*24
    n_data = n_start + 7*24
    step = 17

    pass_dic = {"stat": "histogram",
    "stat_freq": "12hourly",
    "output_freq": "weekly",
    "time_step": 60,
    "variable": "pr",
    "save": True,
    "compression": 1,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    one_pass = opa_stat_with_checkpoint(
        n_start, n_data, step, pass_dic, keep_checkpoints=keep_checkpoints
    )

    return one_pass

####################### py tests ##############################
def test_std_accuracy():
    # the two pass will just be over the last stat_freq
    one_pass, two_pass, message = std_calc(data)
    one_pass = one_pass[-1,:,:]

    assert np.allclose(two_pass, one_pass, rtol = dec_place, atol = dec_place), message

def test_std_length():
    # the two pass will just be over the last stat_freq
    one_pass = std_calc(data)[0]
    one_pass_dim = len(one_pass.time)

    assert one_pass_dim == 3

def test_mean_accuracy():
    # the two pass will just be over the last stat_freq
    one_pass, two_pass, message = mean_calc(data)
    one_pass = one_pass[-1,:,:]

    assert np.allclose(two_pass, one_pass, rtol = dec_place, atol = dec_place), message

def test_mean_length():
    # the two pass will just be over the last stat_freq
    one_pass = mean_calc(data)[0]
    one_pass_dim = len(one_pass.time)

    assert one_pass_dim == 31

def test_min_accuracy():
    # the two pass will just be over the last stat_freq
    one_pass, two_pass, message = min_calc(data)
    one_pass = one_pass[-1,:,:]

    assert np.allclose(two_pass, one_pass, rtol = dec_place, atol = dec_place), message

def test_min_length():
    # the two pass will just be over the last stat_freq
    one_pass = min_calc(data)[0]
    one_pass_dim = len(one_pass.time)

    assert one_pass_dim == 28

def test_max_length():
    # the two pass will just be over the last stat_freq
    one_pass = max_calc(data)[0]
    one_pass_dim = len(one_pass.time)

    assert one_pass_dim == 12
    
def test_max_accuracy():
    one_pass, two_pass, message = max_calc(data)
    one_pass = one_pass[-1,:,:]

    assert np.allclose(two_pass, one_pass, rtol = dec_place, atol = dec_place), message

def test_histogram_time_length():
    # the two pass will just be over the last stat_freq
    bin_edges = hist_calc()
    len_time = len(bin_edges[1].pr.time)

    assert len_time == 14
