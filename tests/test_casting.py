"""
Casting tests for float32 conversion and float64 preservation based on
file outputs written by the library.
"""

from pathlib import Path
import xarray as xr

from one_pass.opa import Opa
from init_tests import get_test_data, temp_directory


def test_float32_conversion(temp_directory):
    """Test that data is properly converted to float32 when as_float32=True"""

    n_start = 24
    n_end = n_start + 24

    pass_dic = {
        "stat": "mean",
        "stat_freq": "2hourly",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "compression": 1,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }

    # Test with float32 conversion
    opa = Opa(pass_dic, as_float32=True)

    # Ensure data is float64 before computation
    data = get_test_data()
    ds = data.isel(time=slice(n_start, n_end)).astype("float64")
    _ = opa.compute(ds)

    # Validate written file dtype
    out_dir = Path(temp_directory)
    nc_files = sorted(out_dir.glob("*.nc"))
    mean_files = [p for p in nc_files if p.name.endswith("_mean.nc")]
    assert mean_files, "No mean output file found"
    ds_out = xr.open_dataset(mean_files[0])
    try:
        var = opa.request.variable
        assert ds_out[var].dtype == "float32", f"Variable {var} is not float32 in file"
    finally:
        ds_out.close()


def test_float64_preservation(temp_directory):
    """Test that data remains float64 when as_float32=False"""

    n_start = 24
    n_end = n_start + 24

    pass_dic = {
        "stat": "mean",
        "stat_freq": "2hourly",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "compression": 1,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }

    # Test without float32 conversion
    opa = Opa(pass_dic, as_float32=False)

    # Ensure data is float64 before computation
    data = get_test_data()
    ds = data.isel(time=slice(n_start, n_end)).astype("float64")
    _ = opa.compute(ds)

    # Validate written file dtype remains float64
    out_dir = Path(temp_directory)
    nc_files = sorted(out_dir.glob("*.nc"))
    mean_files = [p for p in nc_files if p.name.endswith("_mean.nc")]
    assert mean_files, "No mean output file found"
    ds_out = xr.open_dataset(mean_files[0])
    try:
        var = opa.request.variable
        assert ds_out[var].dtype == "float64", f"Variable {var} is not float64 in file"
    finally:
        ds_out.close()


def test_float32_conversion_raw(temp_directory):
    """Test that data is properly converted to float32 when as_float32=True (raw)"""

    n_start = 24
    n_end = n_start + 24

    pass_dic = {
        "stat": "raw",
        "time_step": 60,
        "variable": "pr",
        "compression": 1,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }

    # Test with float32 conversion
    opa = Opa(pass_dic, as_float32=True)

    data = get_test_data()
    # Ensure data is float64 before computation
    ds = data.isel(time=slice(n_start, n_end)).astype("float64")
    _ = opa.compute(ds)

    # Validate written file dtype for raw
    out_dir = Path(temp_directory)
    nc_files = sorted(out_dir.glob("*.nc"))
    raw_files = [p for p in nc_files if p.name.endswith("_raw_data.nc")]
    assert raw_files, "No raw output file found"
    ds_out = xr.open_dataset(raw_files[0])
    try:
        var = opa.request.variable
        assert ds_out[var].dtype == "float32", f"Variable {var} is not float32 in file"
    finally:
        ds_out.close()


def test_float64_preservation_raw(temp_directory):
    """Test that data remains float64 when as_float32=False (raw)"""

    n_start = 24
    n_end = n_start + 24

    pass_dic = {
        "stat": "raw",
        "time_step": 60,
        "variable": "pr",
        "compression": 1,
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": temp_directory,
        "save_filepath": temp_directory,
    }

    # Test without float32 conversion
    opa = Opa(pass_dic, as_float32=False)

    data = get_test_data()
    # Ensure data is float64 before computation
    ds = data.isel(time=slice(n_start, n_end)).astype("float64")
    _ = opa.compute(ds)

    # Validate written file dtype remains float64 for raw
    out_dir = Path(temp_directory)
    nc_files = sorted(out_dir.glob("*.nc"))
    raw_files = [p for p in nc_files if p.name.endswith("_raw_data.nc")]
    assert raw_files, "No raw output file found"
    ds_out = xr.open_dataset(raw_files[0])
    try:
        var = opa.request.variable
        assert ds_out[var].dtype == "float64", f"Variable {var} is not float64 in file"
    finally:
        ds_out.close()
