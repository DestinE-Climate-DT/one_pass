# script for unit testing in python
# test_run_mean_OPA.py

import os
import dask

import numpy as np
import xarray as xr

from one_pass.statistics.iams import Iams
from init_tests import (
    assert_comparison,
    get_opa_stat,
    get_test_data,
    temp_directory,
    TOLERANCE,
    _not_close_message,
)


TEST_DATA = get_test_data()
TOLERANCE_PERCENTILE = 5e-4


def two_pass_thresh_exceed(TEST_DATA, n_start, n_data, thresh_exceed):
    ds = TEST_DATA.isel(time=slice(n_start, n_data))
    axis_num = ds.get_axis_num("time")
    np_thresh_exceed = np.where(ds > thresh_exceed, 1, 0)
    np_thresh_exceed = np.sum(np_thresh_exceed, axis=axis_num, keepdims=True)
    return np_thresh_exceed


def dm_roller(TEST_DATA, duration):

    ### compute rolling sum & select maximum
    rolling_sum = TEST_DATA.rolling(time=duration, center=True).sum()
    rolling_sum_max = rolling_sum.max(dim="time", skipna=True)
    rolling_sum_max = rolling_sum_max.expand_dims(duration=([duration]))

    return rolling_sum_max


def iamser(TEST_DATA, years, durations):
    with dask.config.set(**{"array.slicing.split_large_chunks": True}):
        dmax_list = []
        for durs in durations:
            dmax = dm_roller(TEST_DATA, durs)
            dmax_list.append(dmax)
        iamsfile = xr.concat(dmax_list, dim="duration")
        return iamsfile


def two_pass_iams(TEST_DATA):
    sel_durs = Iams.duration_pick(1, "hours")
    years = [2071]
    iams = iamser(TEST_DATA, years, sel_durs)
    return iams.values


def test_mean(temp_directory):
    n_start = 0
    n_data = n_start + 24  # + 3*30*24 + 2*24
    step = 24
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.mean
    kwargs = {"dtype": np.float64, "keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_mean_2(temp_directory):
    # 3 months of TEST_DATA
    n_start = 0
    n_data = n_start + 31 * 24 * 2 + 28 * 24
    step = 4
    pass_dic = {
        "stat": "mean",
        "stat_freq": "continuous",
        "output_freq": "monthly",
        "percentile_list": None,
        "thresh_exceed": None,
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.mean
    kwargs = {"dtype": np.float64, "keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_std(temp_directory):
    n_start = 0
    n_data = n_start + 24 * 31 * 2 + 28 * 24
    step = 1
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
    kwargs = {"dtype": np.float64, "keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_std_2(temp_directory):
    # 3 months of TEST_DATA
    n_start = 31 * 24 + 28 * 24
    n_data = n_start + 31 * 24 * 2 + 30 * 24
    step = 6

    pass_dic = {
        "stat": "std",
        "stat_freq": "continuous",
        "output_freq": "monthly",
        "percentile_list": None,
        "thresh_exceed": None,
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.std
    kwargs = {"dtype": np.float64, "keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_var(temp_directory):
    n_start = 24 * 4
    n_data = n_start + 7 * 24
    step = 24
    pass_dic = {
        "stat": "var",
        "stat_freq": "weekly",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "save_filepath": temp_directory,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
    }
    two_pass_fun = np.var
    kwargs = {"dtype": np.float64, "keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_var_2(temp_directory):

    # 2 months of TEST_DATA
    n_start = 31 * 24 + 28 * 24
    n_data = n_start + 31 * 24 + 30 * 24
    step = 4

    pass_dic = {
        "stat": "var",
        "stat_freq": "continuous",
        "output_freq": "monthly",
        "percentile_list": None,
        "thresh_exceed": None,
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.var
    kwargs = {"dtype": np.float64, "keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_max(temp_directory):
    n_start = 48 * 24
    n_data = n_start + 12  # 3*30*24 + 2*24
    step = 2
    pass_dic = {
        "stat": "max",
        "stat_freq": "12hourly",
        "output_freq": "12hourly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.max
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_max_2(temp_directory):
    # 2 months of TEST_DATA
    n_start = 31 * 24 + 28 * 24
    n_data = n_start + 31 * 24 + 30 * 24
    step = 4
    pass_dic = {
        "stat": "max",
        "stat_freq": "continuous",
        "output_freq": "monthly",
        "percentile_list": None,
        "thresh_exceed": None,
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.max
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_min(temp_directory):
    n_start = 0
    n_data = n_start + 31 * 24  # 3*30*24 + 2*24
    step = 24
    pass_dic = {
        "stat": "min",
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.min
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_min_2(temp_directory):
    # 4 weeks of TEST_DATA
    n_start = 4 * 24
    n_data = n_start + 7 * 24 * 4
    step = 4
    pass_dic = {
        "stat": "min",
        "stat_freq": "continuous",
        "output_freq": "weekly",
        "percentile_list": None,
        "thresh_exceed": None,
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.min
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_min_6hourly(temp_directory):
    n_start = 24 * 10
    n_data = n_start + 6  # 3*30*24 + 2*24
    step = 1
    pass_dic = {
        "stat": "min",
        "stat_freq": "6hourly",
        "output_freq": "6hourly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.min
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_percentile_daily(temp_directory):
    n_start = 0
    n_data = n_start + 24  # 3*30*24 + 2*24
    step = 24
    pass_dic = {
        "stat": "percentile",
        "stat_freq": "daily",
        "output_freq": "daily",
        "percentile_list": [],
        "thresh_exceed": None,
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.percentile
    kwargs = {"q": np.linspace(0, 99, 100)}
    assert_comparison(
        n_start,
        n_data,
        step,
        pass_dic,
        two_pass_fun,
        tol=TOLERANCE_PERCENTILE,
        **kwargs,
    )


def test_sum_daily(temp_directory):
    # 4 days of TEST_DATA
    n_start = 31 * 24 + 28 * 24
    n_data = n_start + 24 * 4
    step = 2
    pass_dic = {
        "stat": "sum",
        "stat_freq": "continuous",
        "output_freq": "daily",
        "percentile_list": None,
        "thresh_exceed": None,
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.sum
    kwargs = {"keepdims": True}
    assert_comparison(n_start, n_data, step, pass_dic, two_pass_fun, **kwargs)


def test_sum_monthly(temp_directory):
    n_start = 31 * 24
    n_data = n_start + 24 * 28  # 3*30*24 + 2*24
    step = 24
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


def test_iams(temp_directory):
    n_start = 0
    n_data = 8760
    step = 24
    pass_dic = {
        "stat": "iams",
        "stat_freq": "yearly",
        "output_freq": "yearly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    data_arr = TEST_DATA[pass_dic["variable"]]
    data_arr_small = data_arr[:, 0:10, 0:10]
    two_pass = two_pass_iams(data_arr_small)
    one_pass = get_opa_stat(n_start, n_data, step, pass_dic)
    assert np.allclose(
        two_pass, one_pass.values[0, :], rtol=TOLERANCE, atol=TOLERANCE
    ), _not_close_message(pass_dic)


def test_thresh_exceed(temp_directory):
    n_start = 31 * 24 + 24 * 28
    n_data = n_start + 31 * 24  # 3*30*24 + 2*24
    step = 24
    pass_dic = {
        "stat": "thresh_exceed",
        "thresh_exceed": [0.00001],
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    threshold = pass_dic["thresh_exceed"]
    data_arr = TEST_DATA[pass_dic["variable"]]
    two_pass = two_pass_thresh_exceed(data_arr, n_start, n_data, threshold)
    one_pass = get_opa_stat(n_start, n_data, step, pass_dic)
    assert np.allclose(
        two_pass, one_pass.values[0, :], rtol=TOLERANCE, atol=TOLERANCE
    ), _not_close_message(pass_dic)


def test_thresh_exceed_two_values(temp_directory):
    n_start = 31 * 24 + 24 * 28
    n_data = n_start + 31 * 24  # 3*30*24 + 2*24
    step = 24
    pass_dic = {
        "stat": "thresh_exceed",
        "thresh_exceed": [0.001, 0.0005],
        "stat_freq": "monthly",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": False,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    threshold = pass_dic["thresh_exceed"][1]
    data_arr = TEST_DATA[pass_dic["variable"]]
    two_pass = two_pass_thresh_exceed(data_arr, n_start, n_data, threshold)
    one_pass = get_opa_stat(n_start, n_data, step, pass_dic)
    assert np.allclose(
        two_pass, one_pass.values[0, 1:], rtol=TOLERANCE, atol=TOLERANCE
    ), _not_close_message(pass_dic)
