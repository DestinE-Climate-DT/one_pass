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
# testing for general functionality and error handeling 

Outline of testing functions in this script: 

1. Checking correct error is thrown if you pass a lower value
    in output_freq as opposed to stat_freq 
2. Checking it will throw an error when you give a non-integer
    timestep 
3. Checking it will throw an error if you pass a variable 
    that doesn't correspond to the dataSet 
4. Checking that when you run bias_correction 3 output files are 
    produced

"""

############### load data ##################################

# os.chdir('/home/b/b382291/git/AQUA/') 
# sys.path.insert(0, '/home/b/b382291/git/AQUA/')
# from aqua import Reader
# from aqua.reader import catalogue

# #### get data from Levante#### 
# reader = Reader(model="IFS", exp="tco2559-ng5", 
# source="ICMGG_atm2d", regrid="r020")
# data = reader.retrieve(fix=False)
# data = reader.regrid(data)
# data = data.es 

#### reading some data from disk on nord3 #### 
file_path_data = os.path.realpath(
    os.path.join(os.path.dirname(__file__), 'uas_10_months.nc')
    )

fileList = glob.glob(file_path_data) 
fileList.sort() 
data = xr.open_dataset(fileList[0])  
data = data.astype(np.float64)

############################# define functions #########################

def lower_output(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "3hourly",
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

def bad_timestep(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 37.3,
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


def check_attributes(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "es",
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
        
        
def outputs_for_bc(data, file_path):

    pass_dic = {"stat": "bias_correction",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list" : None,
        "thresh_exceed" : None,
        "time_step": 60,
        "variable": "uas",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "out_filepath": file_path}
    
    n_start = 0 
    n_data = 24 
        
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, 1): 
        ds = data.isel(time=slice(i,i+1)) # extract moving window
        dm = daily_mean.compute(ds)
    
    return dm

####################### py tests ##############################


def test_raises_timing_error():

    with pytest.raises(ValueError):
        lower_output(data)

def test_bad_timestep():

    with pytest.raises(Exception):
        bad_timestep(data)

def test_attributes():

    with pytest.raises(Exception):
        check_attributes(data)
        
def test_output_for_bc():
    
    # test to check that the number of files is 3 produced by
    # bias correction 
    # delete files in this path 
    file_path = os.path.realpath(
        os.path.join(os.path.dirname(__file__), 'output_for_bc/')
        )
    
    if os.path.exists(file_path): 
        fileList = os.listdir(file_path) 
        num_of_files = np.size(fileList)

        for files in range(num_of_files): 
            os.remove(os.path.join(file_path, fileList[files]))
    
    outputs_for_bc(data, file_path)
    
    fileList = os.listdir(file_path) 
    num_of_files = np.size(fileList)
    
    assert num_of_files == 3