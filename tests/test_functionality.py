# script for unit testing in python 
# testing for general functionality and error handeling 

import pytest
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


""" 
Outline of testing functions in this script: 

Will throw correct errors when: 
1. pass incorrect statistic name
2. pass incorrect stat_freq or output freq 
3. pass out put freq less than stat freq 
4. pass a timestep that doesn't fit into the stat freq 
    - bad timestep 
5. path to checkpoint files and output files exist 
    (check outfile at the beginning instead of at the end)
6. If variable is wrong / doesn't exisit 
7. MAYBE - pass a timestep that is incorrect (when count == n data 
    throws error that time step isn't what was expected)

Correct does: 
1. writes checkpoint files 
2. writes slow checkpoint files as netCDF

"""


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

def lower_output(data):

    pass_dic = {"stat": "mean",
        "stat_freq": "daily",
        "output_freq": "3hourly",
        "percentile_list" : None,
        "threshold_exceed" : None,
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
        "threshold_exceed" : None,
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
        "threshold_exceed" : None,
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