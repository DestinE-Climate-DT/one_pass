import glob
import os
import sys
import pytest

import numpy as np
import xarray as xr

from init_tests import get_opa_stat


@pytest.fixture(scope="module")
def clean_tests():
    for file in glob.glob("tests/checkpoint_*.pkl"):
        os.remove(file)


def test_keep_checkpoints_mean(clean_tests):
    # the two pass will just be over the last stat_freq
    n_start = 0
    n_data = (n_start + 24) * 31
    step = 24
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    get_opa_stat(n_start, n_data, step, pass_dic, keep_checkpoints=True)
    assert glob.glob("tests/checkpoint_*_mean.pkl")


def test_keep_checkpoints_std(clean_tests):
    # the two pass will just be over the last stat_freq
    n_start = 0
    n_data = n_start + 24 * 31 * 2 + 28 * 24
    step = 1
    pass_dic = {
        "stat": "std",
        "stat_freq": "monthly",
        "output_freq": "3monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    get_opa_stat(n_start, n_data, step, pass_dic, keep_checkpoints=True)
    assert glob.glob("tests/checkpoint_*_std.pkl")


def test_keep_checkpoints_histogram(clean_tests):
    # the two pass will just be over the last stat_freq
    n_start = 4 * 24
    n_data = n_start + 7 * 24
    step = 17
    pass_dic = {
        "bins": 10,
        "stat": "histogram",
        "stat_freq": "12hourly",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    get_opa_stat(n_start, n_data, step, pass_dic, keep_checkpoints=True)
    assert glob.glob("tests/checkpoint_*_histogram.pkl")
