import glob
import os
import sys
import pickle
import numpy as np
import pytest
import xarray as xr

path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)
os.chdir(path)

from one_pass.opa import Opa

""" 
# testing for general functionality of the bias correction
"""

############### load data ##################################
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

dec_place = 1e-6

############################# define functions #########################

def two_pass_sum(data, n_start, n_data):

    ds = data.isel(time=slice(n_start, n_data)) 
    axNum = ds.get_axis_num('time')
    np_sum = np.sum(ds, axis = axNum, keepdims = True)
    
    return np_sum

def outputs_for_bc(data, file_path):

    pass_dic = {
        "stat": "bias_correction",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": file_path,
        "save_filepath": file_path,
    }

    n_start = 0
    n_data = 24

    for i in range(n_start, n_data, 1):
        daily_mean = Opa(pass_dic)
        ds = data.isel(time=slice(i, i + 1))  # extract moving window
        dm = daily_mean.compute_bias_correction(ds)

    return dm

def pickle_for_bc(data, file_path, days):

    pass_dic = {
        "stat": "bias_correction",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": file_path,
        "save_filepath": file_path,
    }

    n_start = 0
    n_data = 24*days
    step = 5
    daily_mean = Opa(pass_dic)

    for i in range(n_start, n_data, step):
        ds = data.isel(time=slice(i, i + step))  # extract moving window
        dm = daily_mean.compute_bias_correction(ds)

    return dm

def opa_stat_with_checkpoint(n_start, n_data, step, pass_dic):

    for i in range(n_start, n_data, step):
        opa_stat = Opa(pass_dic)
        ds = data.isel(time=slice(i, i + step))
        dm_raw, dm = opa_stat.compute_bias_correction(ds)

    dm = getattr(dm, pass_dic["variable"])

    return dm

def bc_sum(data):

    n_start = 48 
    n_data = n_start + 24# + 3*30*24 + 2*24
    step = 27

    pass_dic = {"stat": "bias_correction",
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

    two_pass = two_pass_sum(data_arr, n_start, n_data)
    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)
    return two_pass, one_pass, message

####################### py tests ##############################

def test_sum_accuracy():

    two_pass, one_pass, message = bc_sum(data)

    assert np.allclose(two_pass, one_pass, atol = dec_place), message

def test_output_for_bc():
    # test to check that the number of files produced by
    # bias correction delete files in this path
    file_path = os.path.realpath(
        os.path.join(os.path.dirname(__file__), 'output_for_bc')
        )

    # delete whatever files are in there
    if os.path.exists(file_path):
        fileList = os.listdir(file_path)
        num_of_files = np.size(fileList)

        for files in range(num_of_files):
            os.remove(os.path.join(file_path, fileList[files]))
    else:
        os.mkdir(file_path)

    outputs_for_bc(data, file_path)

    fileList = os.listdir(file_path)
    num_of_files = np.size(fileList)

    assert num_of_files == 26
    
def test_pickle_for_bc():
    # test to check that the number of files produced by
    # bias correction delete files in this path
    file_path = os.path.realpath(
        os.path.join(os.path.dirname(__file__), 'output_for_bc')
        )

    # delete whatever files are in there
    # or recreate the folder 
    if os.path.exists(file_path):
        fileList = os.listdir(file_path)
        num_of_files = np.size(fileList)

        for files in range(num_of_files):
            os.remove(os.path.join(file_path, fileList[files]))
    else:
        os.mkdir(file_path)
    
    days = 5
    pickle_for_bc(data, file_path, days)

    fileList = os.listdir(file_path)
    num_of_files = np.size(fileList)

    pickle_month_file = os.path.realpath(
        os.path.join(os.path.dirname(__file__), 'output_for_bc',
                    "month_01_pr_bias_correction.pkl")
        )

    with open(pickle_month_file, 'rb') as f:
        temp_self = pickle.load(f)
    f.close()

    # remove the files and the folder
    if os.path.exists(file_path):
        for files in range(num_of_files):
            os.remove(os.path.join(file_path, fileList[files]))
        os.rmdir(file_path)

    assert int(temp_self.pr.values[0,0,0].size()) == days
