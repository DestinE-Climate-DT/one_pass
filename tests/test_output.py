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
Testing the output structure of the statistics that include extra
dimensions
"""

############### load data ##################################
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

############################# define functions #########################
def opa_stat_with_checkpoint(n_start, n_data, step, pass_dic):

    for i in range(n_start, n_data, step):

        opa_stat = Opa(pass_dic)
        ds = data.isel(time=slice(i, i + step))
        dm = opa_stat.compute(ds)

    # dm = getattr(dm, pass_dic["variable"])
    if pass_dic["stat"] != "histogram":
        dm = getattr(dm, pass_dic["variable"])

    return dm

def hist_calc():

    n_start = 4*24
    n_data = n_start + 7*24
    step = 17

    pass_dic = {"stat": "histogram",
    "stat_freq": "12hourly",
    "bins" : 10,
    "output_freq": "weekly",
    "time_step": 60,
    "variable": "pr",
    "save": True,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    return one_pass

def thresh_exceed_calc():

    n_start = 4*24
    n_data = n_start + 7*24
    step = 17

    pass_dic = {"stat": "thresh_exceed",
    "thresh_exceed" : [0.6, 0.02, 0.01, 0.008],
    "stat_freq": "12hourly",
    "output_freq": "weekly",
    "time_step": 60,
    "variable": "pr",
    "save": True,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    return one_pass

def percentile_calc():

    n_start = 24
    n_data = n_start + 24
    step = 17

    pass_dic = {"stat": "percentile",
    "percentile" : [],
    "stat_freq": "daily",
    "output_freq": "daily",
    "time_step": 60,
    "variable": "pr",
    "save": True,
    "checkpoint": True,
    "checkpoint_filepath": "tests/",
    "save_filepath": "tests/"}

    one_pass = opa_stat_with_checkpoint(n_start, n_data, step, pass_dic)

    return one_pass
####################### py tests ##############################

def test_histogram_count_length():
    # the two pass will just be over the last stat_freq
    bin_counts = hist_calc()
    print(bin_counts)
    len_bin_counts = len(bin_counts[0].pr.bin_count)
    assert len_bin_counts == 10

def test_threshold_length():
    # the two pass will just be over the last stat_freq
    output = thresh_exceed_calc()
    len_threshold = len(output.thresholds)
    assert len_threshold == 4

def test_percentile_length():
    # the two pass will just be over the last stat_freq
    output = percentile_calc()
    len_percentile = len(output.percentile)
    assert len_percentile == 100