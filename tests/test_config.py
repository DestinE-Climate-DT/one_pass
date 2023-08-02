import pytest
import xarray as xr
import numpy as np 
import glob 
import os 
import sys 

path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
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
    os.path.join(os.path.dirname(__file__), 'uas_10_months.nc')
)

fileList = glob.glob(file_path_data) 
fileList.sort() 
data = xr.open_dataset(fileList[0])  # , chunks = 'auto') # open dataset
data = data.astype(np.float64)

############################# define functions ######################################

def missing_stat(data):

    pass_dic = {"stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 # only need to give it one data point as should throw error on initalisation 

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        daily_mean = Opa(pass_dic)
        dm = daily_mean.compute(ds)


def missing_stat_freq(data):

    pass_dic = {"stat": "mean",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)


def missing_output_freq(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)

def missing_percentile_list(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)

def missing_threshold_exceed(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)

def missing_time_step(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 
    
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
def missing_variable(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 
    
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)


def missing_save(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable" : "uas",
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 
    
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
        
def missing_checkpoint(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable" : "uas",
        "save": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 
    
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
        
def missing_checkpoint_filepath(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable" : "uas",
        "save": True,
        "checkpoint": True,
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 
    
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
        
def missing_out_filepath(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable" : "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 
    
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
def wrong_continuous_setting(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "continuous",
        "output_freq": "continuous",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable" : "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "output_filepath" : "tests/"}
    
    n_start = 0 
    n_data = 1 
    
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
def incorrect_freq(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "wrong_freq",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
def incorrect_stat(data):

    pass_dic = {"stat": "wrong_name",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 # only need to give it one data point as should throw error on initalisation 

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        daily_mean = Opa(pass_dic)
        dm = daily_mean.compute(ds)
        
def no_checkpointfile(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
def wrong_checkpointfile(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/checkpoint.pkl",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
def daily_stat_for_bc(data):

    pass_dic = {"stat": "bias_correction",
        "stat_freq": "monthly",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "test/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
def daily_output_for_bc(data):

    pass_dic = {"stat": "bias_correction",
        "stat_freq": "daily",
        "output_freq": "weekly",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "test/",
        "out_filepath": "tests/"}
    
    n_start = 0 
    n_data = 1 

    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
        
####################### py tests ##############################

def test_missing_stat():

    with pytest.raises(KeyError):
        missing_stat(data)
        
def test_missing_stat_freq():

    with pytest.raises(KeyError):
        missing_stat_freq(data)

def test_missing_percentile_list():

    with pytest.raises(KeyError):
        missing_percentile_list(data)

def test_missing_threshold_exceed():

    with pytest.raises(KeyError):
        missing_threshold_exceed(data)

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
              
def test_missing_output_filepath():

    with pytest.raises(KeyError):
        missing_out_filepath(data)
        
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

    with pytest.raises(ValueError):
        wrong_checkpointfile(data)
        # this will flag a KeyError if the checkpoint file is not found 
        # if you give an incorrect file path however it will flag a filepath not found error 

def test_no_checkpointfile():

    with pytest.raises(ValueError):
        no_checkpointfile(data)

def test_daily_stat_for_bc():

    with pytest.raises(ValueError):
        daily_stat_for_bc(data)
        
def test_output_stat_for_bc():

    with pytest.raises(ValueError):
        daily_output_for_bc(data)