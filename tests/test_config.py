"""
Script to test the config file
Make sure all the required key value pairs are there
Make sure the correction settings are flagged
"""

import pytest

from one_pass.opa import Opa
from one_pass.request import RequestError
from init_tests import get_test_data


TEST_DATA = get_test_data()


def test_raises_timing_error():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "3hourly",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(ValueError):
        Opa(pass_dic)


def test_bad_timestep():
    n_start = 0
    n_data = 1
    step = 1
    pass_dic = {
        "stat": "sum",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 27.4,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    # TODO: Specify exception
    with pytest.raises(Exception):
        opa_stat = Opa(pass_dic)
        for i in range(n_start, n_data, step):
            ds = TEST_DATA.isel(time=slice(i, i + step))
            opa_stat.compute(ds)


def test_attributes():
    n_start = 0
    n_data = 1
    step = 1
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(Exception):
        opa_stat = Opa(pass_dic)
        for i in range(n_start, n_data, step):
            ds = TEST_DATA.isel(time=slice(i, i + step))
            opa_stat.compute(ds)


def test_missing_stat():
    pass_dic = {
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_missing_stat_freq():
    pass_dic = {
        "stat": "mean",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }

    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_missing_output_freq(caplog):
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    Opa(pass_dic)
    assert "WARNING" in caplog.text


def test_unrecognized_fields():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "chekpoint": "mean",  # Misspelled 'checkpoint'
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(RequestError):
        Opa(pass_dic)


def test_missing_percentile_list(caplog):
    pass_dic = {
        "stat": "percentile",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "compression": 1,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    Opa(pass_dic)
    assert "WARNING" in caplog.text


def test_None_percentile_list(caplog):
    pass_dic = {
        "stat": "percentile",
        "percentile_list": None,
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "compression": 1,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    Opa(pass_dic)
    assert "WARNING" in caplog.text


def test_lower_output_freq():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "3hourly",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(ValueError):
        Opa(pass_dic)


def test_missing_variable():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_missing_time_step():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_missing_save():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_missing_checkpoint():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_missing_save_filepath():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
    }
    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_missing_checkpoint_filepath():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "save_filepath": "tests/",
    }
    with pytest.raises(KeyError):
        Opa(pass_dic)


def test_wrong_continuous_setting():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "continuous",
        "output_freq": "continuous",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(ValueError):
        Opa(pass_dic)


def test_raises_stat_error():
    pass_dic = {
        "stat": "wrong_name",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(ValueError):
        Opa(pass_dic)


def test_raises_freq_error():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "wrong_freq",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(ValueError):
        Opa(pass_dic)


def test_wrong_checkpointfile(caplog):
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/checkpoint.pkl",
        "save_filepath": "tests/",
    }
    Opa(pass_dic)
    assert "WARNING" in caplog.text


def test_no_checkpointfile():
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "",
        "save_filepath": "tests/",
    }
    with pytest.raises(RequestError):
        Opa(pass_dic)


def test_too_high_output_freq_than_stat_freq(caplog):
    pass_dic = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "annually",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    Opa(pass_dic)
    assert "WARNING" in caplog.text


def test_no_freq_for_raw(caplog):
    pass_dic = {
        "stat": "raw",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    Opa(pass_dic)
    assert "INFO" in caplog.text


def test_threshold_missing():
    pass_dic = {
        "stat": "thresh_exceed",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(AttributeError):
        Opa(pass_dic)


def test_threshold_str():
    pass_dic = {
        "stat": "thresh_exceed",
        "thresh_exceed": "0.2",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "es",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "tests/",
        "save_filepath": "tests/",
    }
    with pytest.raises(TypeError):
        Opa(pass_dic)
