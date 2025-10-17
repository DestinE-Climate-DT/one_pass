# script for unit testing in python
# test_run_mean_OPA.py

import numpy as np

from init_tests import assert_comparison, temp_directory


TOLERANCE_MEAN = 1e-4


def test_mean(temp_directory):
    n_start = 0
    n_data = n_start + 24 + 13  # + 3*30*24 + 2*24
    step = 26
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily_noon",
        "output_freq": "daily_noon",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.mean
    kwargs = {"keepdims": True}
    assert_comparison(
        n_start, n_data, step, pass_dic, two_pass_fun, tol=TOLERANCE_MEAN, **kwargs
    )


def test_std(temp_directory):
    n_start = 0
    n_data = n_start + 24 * 31 * 2 + 28 * 24
    step = 19
    pass_dic = {
        "stat": "std",
        "stat_freq": "3monthly",
        "output_freq": "3monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.std
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_var(temp_directory):
    n_start = 24 * 4
    n_data = n_start + 7 * 24
    step = 13
    pass_dic = {
        "stat": "var",
        "stat_freq": "weekly",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.var
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_sum_monthly(temp_directory):
    n_start = 31 * 24
    n_data = n_start + 24 * 28  # 3*30*24 + 2*24
    step = 240
    pass_dic = {
        "stat": "sum",
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.sum
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_sum_daily_noon(temp_directory):
    n_start = 40 * 24 + 12
    n_data = n_start + 24  # 3*30*24 + 2*24
    step = 1
    pass_dic = {
        "stat": "sum",
        "stat_freq": "daily_noon",
        "output_freq": "daily_noon",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.sum
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)
