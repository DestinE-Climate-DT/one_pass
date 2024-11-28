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
if len(fileList) == 0:
    exit("ERROR: input file is missing. Please download using provided script (tests/get_data.sh)")
print(fileList[0])
# Error out early if test data is missing
if not os.path.isfile(fileList[0]):
    exit("ERROR: input file is missing. Please download using provided script (tests/get_data.sh)")
data = xr.open_dataset(fileList[0], engine='netcdf4') # , chunks = 'auto') # open dataset
data = data.compute()
data = data.astype(np.float64)

############################# define functions ######################################


def missing_stat():

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

    Opa(pass_dic)

def missing_stat_freq():

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

    Opa(pass_dic)

def missing_output_freq():

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

    Opa(pass_dic)

def none_percentile_list():

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

    Opa(pass_dic)
        
def missing_percentile_list():

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

    Opa(pass_dic)

def lower_output_freq():

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

    Opa(pass_dic)

def missing_time_step():

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

    Opa(pass_dic)

def missing_variable():

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

    Opa(pass_dic)

def missing_save():

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

    Opa(pass_dic)

def missing_checkpoint():
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

    Opa(pass_dic)

def missing_checkpoint_filepath():

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

    Opa(pass_dic)

def missing_save_filepath():

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

    Opa(pass_dic)

def wrong_continuous_setting():

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

    Opa(pass_dic)

def incorrect_freq():

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

    Opa(pass_dic)

def incorrect_stat():

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

    Opa(pass_dic)

def no_checkpointfile():

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

    Opa(pass_dic)

def wrong_checkpointfile():

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

    Opa(pass_dic)

def daily_stat_for_bc():

    pass_dic = {
        "stat": "bias_correction",
        "stat_freq": "monthly",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    Opa(pass_dic)

def daily_output_for_bc():

    pass_dic = {
        "stat": "bias_correction",
        "stat_freq": "daily",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    Opa(pass_dic)

def too_high_output_freq_than_stat_freq():

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "annually",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    Opa(pass_dic)

def no_freq_for_raw():

    pass_dic = {
        "stat": "raw",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    Opa(pass_dic)

def lower_output():

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

    Opa(pass_dic)

def bad_timestep(data):

    pass_dic = {
        "stat": "sum",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 27.4,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    opa_stat = Opa(pass_dic)
    
    n_start = 0
    n_data = 1
    step = 1

    for i in range(n_start, n_data, step):

        opa_stat = Opa(pass_dic)
        ds = data.isel(time=slice(i, i + step))
        dm = opa_stat.compute(ds)

def check_attributes(data):

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    opa_stat = Opa(pass_dic)
    
    n_start = 0
    n_data = 1
    step = 1

    for i in range(n_start, n_data, step):

        opa_stat = Opa(pass_dic)
        ds = data.isel(time=slice(i, i + step))
        dm = opa_stat.compute(ds)

def check_threshold_missing():

    pass_dic = {
        "stat": "thresh_exceed",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    Opa(pass_dic)

def check_threshold_str():

    pass_dic = {
        "stat": "thresh_exceed",
        "thresh_exceed" : '0.2',
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    Opa(pass_dic)
    
def check_bias_adjustment_wrong_stat():

    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
        "bias_adjustment" : True
    }

    Opa(pass_dic)
    
def check_bias_adjustment_no_method():

    pass_dic = {
        "stat": "raw",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
        "bias_adjustment" : True
    }

    Opa(pass_dic)

def check_bias_adjustment_wrong_method():

    pass_dic = {
        "stat": "raw",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
        "bias_adjustment" : True,
        "bias_adjustment_method" : 'Red'
    }

    Opa(pass_dic)
####################### py tests ##############################

def test_raises_timing_error():
    with pytest.raises(ValueError):
        lower_output()

def test_bad_timestep():
    with pytest.raises(Exception):
        bad_timestep(data)

def test_attributes():
    with pytest.raises(Exception):
        check_attributes(data)

def test_missing_stat():
    with pytest.raises(KeyError):
        missing_stat()

def test_missing_stat_freq():
    with pytest.raises(KeyError):
        missing_stat_freq()

def test_missing_output_freq(caplog):
    missing_output_freq()
    assert "WARNING" in caplog.text

def test_missing_percentile_list(caplog):
    missing_percentile_list()
    assert "WARNING" in caplog.text

def test_None_percentile_list(caplog):
    none_percentile_list()
    assert "WARNING" in caplog.text

def test_lower_output_freq():
    with pytest.raises(ValueError):
        lower_output_freq()

def test_missing_variable():
    with pytest.raises(KeyError):
        missing_variable()

def test_missing_time_step():
    with pytest.raises(KeyError):
        missing_time_step()

def test_missing_save():
    with pytest.raises(KeyError):
        missing_save()

def test_missing_checkpoint():
    with pytest.raises(KeyError):
        missing_checkpoint()

def test_missing_save_filepath():
    with pytest.raises(KeyError):
        missing_save_filepath()

def test_missing_checkpoint_filepath():
    with pytest.raises(KeyError):
        missing_checkpoint_filepath()

def test_wrong_continuous_setting():
    with pytest.raises(ValueError):
        wrong_continuous_setting()

def test_raises_stat_error():
    with pytest.raises(ValueError):
        incorrect_stat()

def test_raises_freq_error():
    with pytest.raises(ValueError):
        incorrect_freq()

def test_wrong_checkpointfile(caplog):
        wrong_checkpointfile()
        assert "WARNING" in caplog.text

def test_no_checkpointfile():
    with pytest.raises(ValueError):
        no_checkpointfile()

def test_daily_stat_for_bc():
    with pytest.raises(ValueError):
        daily_stat_for_bc()

def test_output_stat_for_bc():

    with pytest.raises(ValueError):
        daily_output_for_bc()

def test_too_high_output_freq_than_stat_freq(caplog):
    too_high_output_freq_than_stat_freq()
    assert "WARNING" in caplog.text

def test_no_freq_for_raw(caplog):
    no_freq_for_raw()
    assert "INFO" in caplog.text

def test_check_threshold_missing():
    with pytest.raises(AttributeError):
        check_threshold_missing()

def test_check_threshold_str():
    with pytest.raises(ValueError):
        check_threshold_str()

def test_bias_adjustment_wrong_stat():
    with pytest.raises(ValueError):
        check_bias_adjustment_wrong_stat()

def test_bias_adjustment_no_method(caplog):
    check_bias_adjustment_no_method()
    assert "WARNING" in caplog.text

def test_bias_adjustment_wrong_method(caplog):
    check_bias_adjustment_wrong_method()
    assert "WARNING" in caplog.text