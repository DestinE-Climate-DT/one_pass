from datetime import datetime
from functools import lru_cache
from tempfile import TemporaryDirectory
import os

import numpy as np
import pytest
import xarray as xr

from one_pass.config import PATH_DATA
from one_pass.opa import Opa
from one_pass.options import StatKey


FILEPATH_DATA = PATH_DATA / "pr_12_months.nc"
TOLERANCE = 1e-1


@pytest.fixture(scope="package")
def temp_directory():
    with TemporaryDirectory() as tmpdir:
        yield tmpdir


@lru_cache(maxsize=None)
def get_test_data():
    if not FILEPATH_DATA.exists():
        raise FileNotFoundError(
            "Test file is missing. Please run `tools/get_test_data.sh)`."
        )
    data = xr.open_dataset(FILEPATH_DATA, engine="netcdf4")
    data = data.compute()
    data = data.astype(np.float64)
    return data


def _opa_stat_with_checkpoint(
    n_start, n_data, step, pass_dic, *, data=None, keep_checkpoints
):
    if data is None:
        data = get_test_data()
    stat = pass_dic["stat"]
    variable = pass_dic["variable"]
    dm = None
    for i in range(n_start, n_data, step):
        opa_stat = Opa(pass_dic, keep_checkpoints=keep_checkpoints)
        ds = data.isel(time=slice(i, i + step))
        dm = opa_stat.compute(ds)
    if stat == StatKey.HISTOGRAM:
        return dm
    if dm is None:
        raise ValueError(
            f"Values n_start={n_start}, n_data={n_data} and step={step} produced no Opa object."
        )
    dm = dm[variable]
    if stat == "percentile":
        dm = dm[0, :, :]
    return dm


def _opa_stat_without_checkpoint(n_start, n_data, step, pass_dic, *, data=None):
    if data is None:
        data = get_test_data()
    opa_stat = Opa(pass_dic)
    dm = None
    if pass_dic["stat"] == "iams":
        data1 = data.pr[:, 0:10, 0:10]
        for i in range(n_start, n_data, step):
            ds = data1.isel(time=slice(i, i + step))
            dm = opa_stat.compute(ds)
    else:
        for i in range(n_start, n_data, step):
            ds = data.isel(time=slice(i, i + step))
            dm = opa_stat.compute(ds)
    if dm is None:
        raise ValueError(
            f"Values n_start={n_start}, n_data={n_data} and step={step} produced no Opa object."
        )
    dm = dm[pass_dic["variable"]]
    return dm


def get_opa_stat(n_start, n_data, step, pass_dic, *, data=None, keep_checkpoints=False):
    if pass_dic["checkpoint"]:
        return _opa_stat_with_checkpoint(
            n_start,
            n_data,
            step,
            pass_dic,
            data=data,
            keep_checkpoints=keep_checkpoints,
        )
    return _opa_stat_without_checkpoint(n_start, n_data, step, pass_dic, data=data)


def compute_two_pass(variable, n_start, n_data, np_fun, *, data=None, **kwargs):
    if data is None:
        data = get_test_data()
    data_arr = data[variable]
    ds = data_arr.isel(time=slice(n_start, n_data))
    axis_num = ds.get_axis_num("time")
    return np_fun(ds, axis=axis_num, **kwargs)


def _not_close_message(pass_dic, tolerance=None, **kwargs):
    erred_variable_name = pass_dic["stat"]
    if tolerance is None:
        tolerance = TOLERANCE
    return (
        f"OPA and numpy {erred_variable_name} are not close to tolerance {tolerance}."
        + (
            ""
            if not kwargs
            else f" {', '.join(f'{k}: {v}' for k, v in kwargs.items())}"
        )
    )


# TODO: Do not reuse the same value for rtol and rtol
def assert_comparison(
    n_start, n_data, step, pass_dic, two_pass_fun, *, tol=TOLERANCE, **kwargs
):
    variable = pass_dic["variable"]
    two_pass = compute_two_pass(variable, n_start, n_data, two_pass_fun, **kwargs)
    one_pass = get_opa_stat(n_start, n_data, step, pass_dic)
    assert np.allclose(two_pass, one_pass, rtol=tol, atol=tol), _not_close_message(
        pass_dic, tol, difference=np.max(np.abs(two_pass - one_pass))
    )
