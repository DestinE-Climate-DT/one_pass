"""Tests for integration with bias_correction package"""

import glob
import os
import sys
import pickle
import numpy as np
import pytest
import xarray as xr
import pandas as pd
import shutil

import pytest
from one_pass.opa import Opa

try:
    from bias_corr.core import TDigestList
    from bias_corr.estimate_tdigest import estimate
    from bias_corr.map_tdigest import bias_adjust
    from scipy.stats import skewnorm
except ImportError:
    bias_adjust = None
    TDigestList = None
    estimate = None
    skewnorm = None

# Tolerance for comparing
dec_place = 1e-6

# Configuration parameters for synthetic data
GRID_SHAPE = (3, 3)
TIME_LEN = 5 * 30
MEAN_SHIFT = 2.0
SKEWNESS = 5.0
TREND = None
TREND = 0.1
FREQ = "h" # D or h

# Directories, checkpoint, input and reference
save_dir = os.path.abspath(os.path.dirname(__file__))
save_ba_dir = os.path.join(save_dir, "ba")
output_dir = os.path.join(save_dir, "test_opa_ba_integration")
tdigest_model_dir = os.path.join(output_dir, "tdigest_model", "ba")
input_data_dir = os.path.join(output_dir, "input_data")


# check FREQ
step = [1, 2, 3, 4, 6, 8, 12]
if FREQ not in ["D", "h"] + [f"{i}h" for i in step]:
    raise ValueError(f"{FREQ=} not valid")
sub = 1
if FREQ == "h":
    sub = 24
for i in step:
    if FREQ == f"{i}h":
        sub = 24 // i
TIME_LEN *= sub


time = pd.date_range("1990-01-01", periods=TIME_LEN, freq="D")
time_projection = pd.date_range("2030-01-01", periods=TIME_LEN, freq="D")
lat = np.linspace(-10, 10, GRID_SHAPE[0])
lon = np.linspace(-10, 10, GRID_SHAPE[1])


def setup_directories():
    for directory in [output_dir, input_data_dir, tdigest_model_dir, save_ba_dir]:
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.makedirs(directory, mode=0o775, exist_ok=True)


@pytest.fixture(scope="module")
def ds_model():
    setup_directories()

    # Step 1: Create reference data (normal distribution) - we need to store those, 
    # as estimate currently expects file input - usually the user will provide the output
    # of the estimate functions via the ba_reference_dir 
    data_reference = np.random.normal(loc=0.0, scale=1.0, size=(TIME_LEN, *GRID_SHAPE))
    if TREND:
        trend = np.array([[[(x.year - 1990) * TREND]] for x in time])
        data_reference += trend
    ds_reference = xr.Dataset(
        {"myvar": (("time", "lat", "lon"), data_reference)},
        coords={"time": time, "lat": lat, "lon": lon},
    )
    ds_reference_path = os.path.join(input_data_dir, "reference_data.nc")
    ds_reference.to_netcdf(ds_reference_path)

    # Now we created the needed reference t-digest objects in the output_dir
    estimate(
        in_path=input_data_dir,
        fname="reference_data.nc",
        vname="myvar",
        lower_threshold=-np.inf,
        mask_file=None,
        output_tdigest=output_dir,
    )

    # Step 2: Create some artificial model data (skewed distribution)
    data_model = skewnorm.rvs(
        SKEWNESS, loc=MEAN_SHIFT, scale=1.0, size=(TIME_LEN, *GRID_SHAPE)
    )
    if TREND:
        trend = np.array([[[(x.year - 1990) * TREND]] for x in time])
        data_model += trend
    return xr.Dataset(
        {"myvar": (("time", "lat", "lon"), data_model)},
        coords={"time": time, "lat": lat, "lon": lon},
    )


@pytest.fixture(scope="module")
def da_model_bias_adjusted(ds_model):
    # Configuration parameters for synthetic data

    # Create some more model data, this should be for future i.e. set ba_future_start_date to some date before
    # this start date, e.g. 2025-01-01
    data_model_proj = skewnorm.rvs(
        SKEWNESS + 2, loc=MEAN_SHIFT + 1, scale=1.0, size=(TIME_LEN, *GRID_SHAPE)
    )
    if TREND:
        trend = np.array([[[(x.year - 1990) * TREND]] for x in time_projection])
        data_model_proj += trend
    ds_model_proj = xr.Dataset(
        {"myvar": (("time", "lat", "lon"), data_model_proj)},
        coords={"time": time_projection, "lat": lat, "lon": lon},
    )

    # Step 4: Run the map function on the model data, using the t-digests from the reference data
    return bias_adjust(
        data=ds_model["myvar"],
        tdigest_ref=output_dir,
        tdigest_model=tdigest_model_dir,
        agg_meth="mean",
        evaluate=False,
        proceed=False,
        detrend=True,
    )


@pytest.mark.skipif(bias_adjust is None, reason="Optional dependency bias_corr not installed")
def test_bias_adjust(ds_model, da_model_bias_adjusted):
    pass_dic = {
        "stat": "raw",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "myvar",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": save_dir,
        "save_filepath": save_dir,
        "bias_adjust": True,
        "ba_reference_dir": output_dir,
        "ba_detrend": TREND is not None,
        "ba_agg_method": "mean",
    }
    opa = Opa(pass_dic)
    res = opa.compute(ds_model["myvar"])

    assert np.allclose(
        res["myvar"].data, da_model_bias_adjusted.data, atol=dec_place
    )


def test_error_no_reference_dir():
    with pytest.raises(ValueError):
        pass_dic = {
            "stat": "raw",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "myvar",
            "save": False,
            "checkpoint": True,
            "checkpoint_filepath": save_dir,
            "save_filepath": save_dir,
            "bias_adjust": True,

        }
        opa = Opa(pass_dic)


def test_error_bad_agg_method():
    with pytest.raises(ValueError):
        pass_dic = {
            "stat": "raw",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "myvar",
            "save": False,
            "checkpoint": True,
            "checkpoint_filepath": save_dir,
            "save_filepath": save_dir,
            "bias_adjust": True,
            "ba_reference_dir": output_dir,
            "ba_agg_method": "badvalue",
        }
        opa = Opa(pass_dic)


def test_error_bad_future_method():
    with pytest.raises(ValueError):
        pass_dic = {
            "stat": "raw",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "myvar",
            "save": False,
            "checkpoint": True,
            "checkpoint_filepath": save_dir,
            "save_filepath": save_dir,
            "bias_adjust": True,
            "ba_reference_dir": output_dir,
            "ba_future_method": "badvalue",
        }
        opa = Opa(pass_dic)


def test_error_bad_start_date():
    with pytest.raises(ValueError):
        pass_dic = {
            "stat": "raw",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "myvar",
            "save": False,
            "checkpoint": True,
            "checkpoint_filepath": save_dir,
            "save_filepath": save_dir,
            "bias_adjust": True,
            "ba_reference_dir": output_dir,
            "ba_future_start_date": "abad-da-te"
        }
        opa = Opa(pass_dic)
