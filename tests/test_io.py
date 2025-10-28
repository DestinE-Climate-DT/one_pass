"""Tests for OPA I/O methods: checkpoint() and save_nc() with custom directory support."""

# pylint: disable=redefined-outer-name

import glob
import os
import pickle
import pytest
import shutil
import tempfile

import xarray as xr

from init_tests import get_test_data
from one_pass.opa import Opa


@pytest.fixture(scope="module")
def temp_io_dir():
    """Create a temporary directory for I/O tests."""
    temp_dir = tempfile.mkdtemp(prefix="opa_io_test_")
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture(scope="module")
def sample_data():
    """Load sample test data."""
    return get_test_data()


def test_checkpoint_default_directory(temp_io_dir, sample_data):
    """Test checkpoint() uses request.checkpoint_filepath when no directory argument provided."""
    request_dict = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "monthly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,
        "checkpoint_filepath": temp_io_dir,
    }

    opa = Opa(request_dict, keep_checkpoints=True)
    opa.compute(sample_data.isel(time=slice(0, 48)))
    opa.checkpoint()

    expected_file = os.path.join(
        temp_io_dir, "checkpoint_pr_daily_timestep_60_monthly_mean.pkl"
    )
    assert os.path.exists(expected_file)

    # Verify it can be loaded
    with open(expected_file, "rb") as f:
        loaded_opa = pickle.load(f)
        assert loaded_opa.request.stat.value == "mean"

    os.remove(expected_file)


def test_checkpoint_custom_directory(temp_io_dir, sample_data):
    """Test checkpoint() uses custom directory when provided as argument."""
    default_dir = os.path.join(temp_io_dir, "default")
    custom_dir = os.path.join(temp_io_dir, "custom")
    os.makedirs(default_dir, exist_ok=True)
    os.makedirs(custom_dir, exist_ok=True)

    request_dict = {
        "stat": "std",
        "stat_freq": "daily",
        "output_freq": "weekly",
        "time_step": 60,
        "variable": "pr",
        "save": False,
        "checkpoint": True,  # Enable checkpointing
        "checkpoint_filepath": default_dir,
    }

    opa = Opa(request_dict, keep_checkpoints=True)
    opa.compute(sample_data.isel(time=slice(0, 72)))

    # Clear any auto-created checkpoint from default dir
    default_checkpoint = os.path.join(
        default_dir, "checkpoint_pr_daily_timestep_60_weekly_std.pkl"
    )
    if os.path.exists(default_checkpoint):
        os.remove(default_checkpoint)

    # Now checkpoint to custom directory
    opa.checkpoint(directory=custom_dir)

    expected_filename = "checkpoint_pr_daily_timestep_60_weekly_std.pkl"
    custom_path = os.path.join(custom_dir, expected_filename)
    default_path = os.path.join(default_dir, expected_filename)

    assert os.path.exists(custom_path), f"File not in custom dir: {custom_path}"
    assert not os.path.exists(
        default_path
    ), f"File should not be in default dir: {default_path}"

    # Verify it can be loaded
    with open(custom_path, "rb") as f:
        loaded_opa = pickle.load(f)
        assert loaded_opa.request.stat.value == "std"

    shutil.rmtree(default_dir)
    shutil.rmtree(custom_dir)
