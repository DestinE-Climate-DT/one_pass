# script for unit testing in python
# test_run_mean_OPA.py

from typing import Tuple

import numpy as np

from init_tests import compute_two_pass, get_opa_stat, temp_directory, TOLERANCE


def _not_close_message(pass_dic, dp=None):
    erred_variable_name = pass_dic["stat"]
    if dp is None:
        dp = TOLERANCE
    return f"OPA and numpy {erred_variable_name} are not close to {dp} decimal places."


# TODO: Do not reuse the same value for rtol and rtol
def assert_comparison(
    n_start: Tuple[int, int],
    n_data,
    step,
    pass_dic,
    two_pass_fun,
    *,
    tol=TOLERANCE,
    **kwargs,
):
    variable = pass_dic["variable"]
    two_pass = compute_two_pass(variable, n_start[1], n_data, two_pass_fun, **kwargs)
    one_pass = get_opa_stat(n_start[0], n_data, step, pass_dic, keep_checkpoints=False)
    assert np.allclose(
        two_pass, one_pass[-1, :, :], rtol=tol, atol=tol
    ), _not_close_message(pass_dic)
    return one_pass, two_pass


def test_mean_accuracy_and_length(temp_directory):
    # the two pass will just be over the last stat_freq
    n_start = 0
    n_data = (n_start + 24) * 31
    n_start_2_pass = n_data - 24
    step = 24
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.mean
    kwargs = {"dtype": np.float64, "keepdims": True}
    one_pass, _ = assert_comparison(
        (n_start, n_start_2_pass), n_data, step, pass_dic, two_pass_fun, **kwargs
    )
    assert len(one_pass) == 31


def test_std_accuracy_and_length(temp_directory):
    # the two pass will just be over the last stat_freq
    n_start = 0
    n_data = n_start + 24 * 31 * 2 + 28 * 24
    n_start_2_pass = n_data - 24 * 31
    step = 1
    pass_dic = {
        "stat": "std",
        "stat_freq": "monthly",
        "output_freq": "3monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.std
    kwargs = {"dtype": np.float64, "keepdims": True}
    one_pass, _ = assert_comparison(
        (n_start, n_start_2_pass), n_data, step, pass_dic, two_pass_fun, **kwargs
    )
    assert len(one_pass) == 3


def test_min_accuracy_and_length(temp_directory):
    # the two pass will just be over the last stat_freq
    n_start = 24 * 4
    n_data = n_start + 7 * 24
    n_start_2_pass = n_data - 6
    step = 9
    pass_dic = {
        "stat": "min",
        "stat_freq": "6hourly",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.min
    kwargs = {"keepdims": True}
    one_pass, _ = assert_comparison(
        (n_start, n_start_2_pass), n_data, step, pass_dic, two_pass_fun, **kwargs
    )
    assert len(one_pass) == 28


def test_max_accuracy_and_length(temp_directory):
    # the two pass will just be over the last stat_freq
    n_start = 0
    n_data = n_start + 7 * 24
    n_start_2_pass = n_data - 6
    step = 9
    pass_dic = {
        "stat": "max",
        "stat_freq": "6hourly",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    two_pass_fun = np.max
    kwargs = {"keepdims": True}
    one_pass, _ = assert_comparison(
        (n_start, n_start_2_pass), n_data, step, pass_dic, two_pass_fun, **kwargs
    )
    # TODO: Take into account when output_frequency for start.
    assert len(one_pass) == 28


def test_histogram_time_length(temp_directory):
    # the two pass will just be over the last stat_freq
    n_start = 4 * 24
    n_data = n_start + 7 * 24
    step = 7
    pass_dic = {
        "bins": 10,
        "stat": "histogram",
        "stat_freq": "12hourly",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "compression": 1,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }
    bin_edges = get_opa_stat(n_start, n_data, step, pass_dic, keep_checkpoints=False)
    len_time = len(bin_edges[1].pr.time)
    assert len_time == 14
