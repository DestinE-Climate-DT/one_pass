import glob
import os
import sys
import warnings

import numpy as np
import pytest
import xarray as xr

path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)
os.chdir(path)

from one_pass.opa import Opa

""" 
Script to test the config file 
Make sure all the required key value pairs are there 
Make sure the correction settings are flagged 

"""

#### reading some data from disk on nord3 ####
file_path_data = os.path.realpath(
    os.path.join(os.path.dirname(__file__), 'pr_12_months.nc')
    )

fileList = glob.glob(file_path_data) 
fileList.sort() 
print(fileList[0])
data = xr.open_dataset(fileList[0], engine='netcdf4') # , chunks = 'auto') # open dataset
data = data.compute()
data = data.astype(np.float64)

############################# define functions ######################################


def missing_stat(data):

    pass_dic = {
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = (
        1  # only need to give it one data point as should throw error on initalisation
    )

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        daily_mean = Opa(pass_dic)
        dm = daily_mean.compute(ds)


def missing_stat_freq(data):

    pass_dic = {
        "stat": "mean",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def missing_output_freq(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)

def none_percentile_list(data):

    pass_dic = {
        "stat": "percentile",
        "percentile_list" : None,
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)
        
def missing_percentile_list(data):

    pass_dic = {
        "stat": "percentile",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def lower_output_freq(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "3hourly",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def missing_time_step(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def missing_variable(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def missing_save(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable" : "pr",
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def missing_checkpoint(data):
    # this is missing the checkpoint true false, 
    # will get set to True, with warning

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable" : "pr",
        "save": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def missing_checkpoint_filepath(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable" : "pr",
        "save": True,
        "checkpoint": True,
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def missing_save_filepath(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable" : "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def wrong_continuous_setting(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "continuous",
        "output_freq": "continuous",
        "time_step": 60,
        "variable" : "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def incorrect_freq(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "wrong_freq",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def incorrect_stat(data):

    pass_dic = {
        "stat": "wrong_name",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = (
        1  # only need to give it one data point as should throw error on initalisation
    )

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        daily_mean = Opa(pass_dic)
        dm = daily_mean.compute(ds)


def no_checkpointfile(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def wrong_checkpointfile(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/checkpoint.pkl",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def daily_stat_for_bc(data):

    pass_dic = {
        "stat": "bias_correction",
        "stat_freq": "monthly",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "test/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


def daily_output_for_bc(data):

    pass_dic = {
        "stat": "bias_correction",
        "stat_freq": "daily",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "test/",
        "save_filepath": "tests/",
    }

    n_start = 0
    n_data = 1

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1):
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute(ds)


####################### py tests ##############################


def test_missing_stat():

    with pytest.raises(KeyError):
        missing_stat(data)

def test_missing_stat_freq():

    with pytest.raises(KeyError):
        missing_stat_freq(data)

def test_missing_percentile_list():

    with pytest.warns(UserWarning):
        missing_percentile_list(data)

def test_None_percentile_list():
# raises warning
    with pytest.warns(UserWarning):
        none_percentile_list(data)
        
def test_lower_output_freq():

    with pytest.raises(ValueError):
        lower_output_freq(data)

def test_missing_variable():

    with pytest.raises(KeyError):
        missing_variable(data)

def test_missing_time_step():

    with pytest.raises(KeyError):
        missing_time_step(data)

def test_missing_save():

    with pytest.raises(KeyError):
        missing_save(data)

def test_missing_checkpoint():

    with pytest.raises(KeyError):
        missing_checkpoint(data)

def test_missing_save_filepath():

    with pytest.raises(KeyError):
        missing_save_filepath(data)

def test_missing_checkpoint_filepath():

    with pytest.raises(KeyError):
        missing_checkpoint_filepath(data)

def test_wrong_continuous_setting():

    with pytest.raises(ValueError):
        wrong_continuous_setting(data)

def test_raises_stat_error():

    with pytest.raises(ValueError):
        incorrect_stat(data)

def test_raises_freq_error():

    with pytest.raises(ValueError):
        incorrect_freq(data)

def test_wrong_checkpointfile():

    with pytest.warns(UserWarning):
        wrong_checkpointfile(data)

def test_no_checkpointfile():

    with pytest.raises(ValueError):
        no_checkpointfile(data)

def test_daily_stat_for_bc():

    with pytest.raises(ValueError):
        daily_stat_for_bc(data)

def test_output_stat_for_bc():

    with pytest.raises(ValueError):
        daily_output_for_bc(data)
