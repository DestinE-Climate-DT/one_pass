# script for unit testing in python
# test_run_mean_OPA.py

import glob
import os
import sys
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
dec_place_per = 5e-4
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

def two_pass_thresh_exceed(data, n_start, n_data, thresh_exceed):

    # ds = data.isel(time=slice(n_start, n_data)) 
    # axNum = ds.get_axis_num('time')
    # np_thresh_exceed = np.where(ds > thresh_exceed, 1, 0)
    # np_thresh_exceed = np.sum(np_thresh_exceed, axis = axNum, keepdims=True)

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_thresh_exceed = np.where(ds > thresh_exceed, 1, 0)
    np_thresh_exceed = np.sum(np_thresh_exceed, axis = axNum, keepdims=True)

    return np_thresh_exceed

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
    n_data = n_start + 24 # + 3*30*24 + 2*24
    step = 24 
    
    pass_dic = {"stat": "mean",
    "stat_freq": "daily",
    "output_freq": "daily",
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

    two_pass = two_pass_mean(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message
    
def test_std():

    n_start = 0
    n_data = n_start + 24*31*2 + 28*24
    step = 1

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
    step = 24

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


def test_max():

    n_start = 48*24 
    n_data = n_start + 12 # 3*30*24 + 2*24 
    step = 2
    
    pass_dic = {"stat": "max",
    "stat_freq": "12hourly",
    "output_freq": "12hourly",
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

    two_pass = two_pass_max(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, atol=dec_place), message

def test_min():

    n_start = 0 
    n_data = n_start + 31*24 # 3*30*24 + 2*24 
    step = 24 
    
    pass_dic = {"stat": "min",
    "stat_freq": "monthly",
    "output_freq": "monthly",
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

    two_pass = two_pass_min(data_arr, n_start, n_data)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)
    
    assert np.allclose(two_pass, one_pass, atol = dec_place), message
      
def test_min_6hourly():

    n_start = 24*10 
    n_data = n_start + 6 # 3*30*24 + 2*24 
    step = 1
    
    pass_dic = {"stat": "min",
    "stat_freq": "6hourly",
    "output_freq": "6hourly",
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

    two_pass = two_pass_min(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, atol=dec_place), message

def test_percentile_daily():

    n_start = 0
    n_data = n_start + 24  # 3*30*24 + 2*24
    step = 24
    
    pass_dic = {"stat": "percentile",
    "stat_freq": "daily",
    "output_freq": "daily",
    "threshold_exceed" : 10,
    "percentile_list" : [],
    "thresh_exceed" : None,
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
        + str(dec_place_per)
    )

    percentile_list = np.linspace(0, 99, 100)
    two_pass = two_pass_percentile(data_arr, n_start, n_data, percentile_list)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(two_pass, one_pass, rtol = dec_place_per, atol = dec_place_per), message

def test_sum_monthly():

    n_start = 31*24
    n_data = n_start + 24*28 # 3*30*24 + 2*24 
    step = 24
    
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

def test_iams():

    n_start = 0
    n_data = 8760
    step = 24
    
    pass_dic = {"stat": "iams",
    "stat_freq": "annually",
    "output_freq": "annually",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    data_arr = getattr(data, pass_dic["variable"])
    data_arr_small = data_arr[:,0:10,0:10]
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + \
        str(pass_dic["stat"]) + " not equal to " + str(dec_place)

    two_pass = two_pass_iams(data_arr_small)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(
        two_pass, one_pass.values[0,:], rtol = dec_place, atol = dec_place
        ), message
    
def test_thresh_exceed():

    n_start = 31*24 + 24*28
    n_data = n_start + 31*24 # 3*30*24 + 2*24 
    step = 24

    pass_dic = {"stat": "thresh_exceed",
    "thresh_exceed" : [0.00001],
    "stat_freq": "monthly",
    "output_freq": "monthly",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    threshold = pass_dic["thresh_exceed"]
    
    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + \
        str(pass_dic["stat"]) + " not equal to " + str(dec_place)

    data_arr = getattr(data, pass_dic["variable"])
    two_pass = two_pass_thresh_exceed(data_arr, n_start, n_data, threshold)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(
        two_pass, one_pass.values[0,:], rtol = dec_place, atol = dec_place
        ), message


def test_thresh_exceed_two_values():

    n_start = 31*24 + 24*28
    n_data = n_start + 31*24 # 3*30*24 + 2*24 
    step = 24

    pass_dic = {"stat": "thresh_exceed",
    "thresh_exceed" : [0.001, 0.0005],
    "stat_freq": "monthly",
    "output_freq": "monthly",
    "time_step": 60,
    "variable": "pr",
    "save": False,
    "checkpoint": False,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    threshold = pass_dic["thresh_exceed"][1]

    message = "OPA " + str(pass_dic["stat"]) + " and numpy " + \
        str(pass_dic["stat"]) + " not equal to " + str(dec_place)

    data_arr = getattr(data, pass_dic["variable"])
    two_pass = two_pass_thresh_exceed(data_arr, n_start, n_data, threshold)
    one_pass = opa_stat_no_checkpoint(n_start, n_data, step, pass_dic)

    assert np.allclose(
        two_pass, one_pass.values[0,1:], rtol = dec_place, atol = dec_place
        ), message