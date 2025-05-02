import glob
import os
import sys
import pytest

import numpy as np
import xarray as xr

from test_time_append import mean_calc, std_calc, hist_calc

path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)
os.chdir(path)


@pytest.fixture
def data(scope="module"):
    file_path_data = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "pr_12_months.nc")
    )

    file_list = glob.glob(file_path_data)
    file_list.sort()
    if len(file_list) == 0 or not os.path.isfile(file_list[0]):
        raise RuntimeError(
            "Input file is missing. Please download using provided script (tests/get_data.sh)"
        )
    data = xr.open_dataset(
        file_list[0], engine="netcdf4"
    )  # , chunks = 'auto') # open dataset
    data = data.compute()
    data = data.astype(np.float64)
    return data


@pytest.fixture(scope="module")
def clean_tests():
    for file in glob.glob("tests/checkpoint_*.pkl"):
        os.remove(file)


def test_keep_checkpoints_mean(data, clean_tests):
    # the two pass will just be over the last stat_freq
    mean_calc(data, keep_checkpoints=True)
    assert glob.glob("tests/checkpoint_*_mean.pkl")


def test_keep_checkpoints_std(data, clean_tests):
    # the two pass will just be over the last stat_freq
    std_calc(data, keep_checkpoints=True)
    assert glob.glob("tests/checkpoint_*_std.pkl")


def test_keep_checkpoints_histogram(data, clean_tests):
    # the two pass will just be over the last stat_freq
    hist_calc(keep_checkpoints=True)
    assert glob.glob("tests/checkpoint_*_histogram.pkl")
