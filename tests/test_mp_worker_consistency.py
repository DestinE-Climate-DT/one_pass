"""Test multiprocessing worker consistency for TDigest statistics.

This module tests that histogram and percentile statistics produce identical
results regardless of the number of workers used (1, 2, or 4).
"""

import os
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from one_pass.opa import Opa
from one_pass.options import StatKey, FrequencyKey


class TestMPWorkerConsistency:
    """Test that MP workers produce consistent results."""

    @pytest.fixture
    def temp_directory(self):
        """Provide temporary directory for checkpoints."""
        with TemporaryDirectory() as tmpdir:
            yield tmpdir

    def create_seeded_test_data(
        self,
        time_steps=48,
        lat_size=12,
        lon_size=10,
        seed=12345,
        start_date="2021-01-01",
    ):
        """Create deterministic test data with a fixed random seed.

        Args:
            time_steps: Number of time steps (default: 48)
            lat_size: Number of latitude points (default: 12)
            lon_size: Number of longitude points (default: 10)
            seed: Random seed for reproducibility (default: 12345)
            start_date: Start date for the time coordinate (default: "2021-01-01")

        Returns:
            xarray.Dataset with synthetic precipitation-like data
        """
        np.random.seed(seed)

        # Create realistic precipitation-like data using exponential distribution
        # This mimics actual precipitation patterns better than normal distribution
        data = np.random.exponential(scale=2.0, size=(time_steps, lat_size, lon_size))

        # Add spatial patterns (gradient from south to north)
        for i in range(lat_size):
            data[:, i, :] *= 1.0 + 0.3 * (i / lat_size)

        # Add temporal variation (diurnal cycle)
        for t in range(time_steps):
            cycle_factor = 1.0 + 0.2 * np.sin(2 * np.pi * t / 24)
            data[t, :, :] *= cycle_factor

        # Add some zeros (dry periods) and extreme values for edge case testing
        data[data < 0.05] = 0.0
        extreme_mask = np.random.random(data.shape) < 0.05
        data[extreme_mask] *= 5.0

        # Create coordinate arrays
        time_coords = pd.date_range(start_date, periods=time_steps, freq="h")
        lat_coords = np.linspace(-30, 30, lat_size)
        lon_coords = np.linspace(-50, 50, lon_size)

        dataset = xr.Dataset(
            {
                "pr": xr.DataArray(
                    data,
                    dims=["time", "lat", "lon"],
                    coords={
                        "time": time_coords,
                        "lat": lat_coords,
                        "lon": lon_coords,
                    },
                )
            }
        )

        return dataset

    def run_computation(self, request, test_data, chunk_size=6, workers=1):
        """Run OPA computation with specified configuration.

        Args:
            request: Configuration dictionary for Opa
            test_data: Input dataset
            chunk_size: Size of chunks to process (default: 6)
            workers: Number of worker processes (default: 1)

        Returns:
            Result dataset from computation
        """
        opa = Opa(request, keep_checkpoints=False, workers=workers)

        result = None
        time_steps = test_data.sizes["time"]
        for i in range(0, time_steps, chunk_size):
            end_idx = min(i + chunk_size, time_steps)
            chunk = test_data.isel(time=slice(i, end_idx))
            result = opa.compute(chunk)

        return result

    @pytest.mark.parametrize(
        "stat_freq",
        [
            FrequencyKey.DAILY,
            FrequencyKey.WEEKLY,
            FrequencyKey.MONTHLY,
            FrequencyKey.THREE_MONTHLY,
        ],
    )
    def test_histogram_worker_consistency(self, temp_directory, stat_freq):
        """Test that histogram results are identical across different worker counts."""
        # Set output_freq to the same as stat_freq (smallest valid value)
        output_freq = stat_freq

        # Adjust time_steps and start_date based on frequency to ensure data
        # covers exactly N complete periods aligned to period boundaries.
        # Weekly starts on Monday (2021-01-04). Monthly/3monthly use exact
        # hours for complete calendar periods from Jan 1.
        freq_configs = {
            FrequencyKey.DAILY: {"time_steps": 48, "start_date": "2021-01-01"},
            FrequencyKey.WEEKLY: {"time_steps": 336, "start_date": "2021-01-04"},
            FrequencyKey.MONTHLY: {"time_steps": 1416, "start_date": "2021-01-01"},
            FrequencyKey.THREE_MONTHLY: {
                "time_steps": 4344,
                "start_date": "2021-01-01",
            },
        }

        base_request = {
            "stat": StatKey.HISTOGRAM,
            "stat_freq": stat_freq,
            "output_freq": output_freq,
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": True,
            "bins": 15,
            "range": [0.0, 10.0],  # Fixed range for consistent comparison
        }

        # Create deterministic test data with appropriate time steps
        cfg = freq_configs[stat_freq]
        test_data = self.create_seeded_test_data(
            time_steps=cfg["time_steps"],
            lat_size=12,
            lon_size=10,
            seed=42,
            start_date=cfg["start_date"],
        )

        results = {}
        worker_counts = [1, 2, 4]

        # Run computation with different worker counts, using separate checkpoint dirs
        for workers in worker_counts:
            worker_dir = os.path.join(temp_directory, f"workers_{workers}")
            os.makedirs(worker_dir, exist_ok=True)
            request = {
                **base_request,
                "checkpoint_filepath": worker_dir,
                "save_filepath": worker_dir,
            }

            result = self.run_computation(
                request, test_data, chunk_size=6, workers=workers
            )
            assert (
                result is not None
            ), f"Result should not be None for workers={workers}"

            # Store the bin counts for comparison
            # Histogram returns a tuple: (bin_counts_dataset, bin_edges_dataset)
            bin_counts_dataset = result[0]
            results[workers] = bin_counts_dataset["pr"].values.copy()

        # Compare all results against workers=1 (serial baseline)
        baseline = results[1]

        for workers in [2, 4]:
            np.testing.assert_allclose(
                baseline,
                results[workers],
                rtol=1e-12,
                atol=1e-12,
                err_msg=f"Histogram results differ between workers=1 and workers={workers} for stat_freq={stat_freq.value}",
            )

        # Additional sanity checks
        assert baseline.size > 0, "Result should have data"
        assert np.sum(baseline) > 0, "Should have non-zero histogram counts"

    @pytest.mark.parametrize(
        "stat_freq",
        [
            FrequencyKey.DAILY,
            FrequencyKey.WEEKLY,
            FrequencyKey.MONTHLY,
            FrequencyKey.THREE_MONTHLY,
        ],
    )
    def test_percentile_worker_consistency(self, temp_directory, stat_freq):
        """Test that percentile results are identical across different worker counts."""
        # Set output_freq to the same as stat_freq (smallest valid value)
        output_freq = stat_freq

        # Adjust time_steps and start_date based on frequency to ensure data
        # covers exactly N complete periods aligned to period boundaries.
        freq_configs = {
            FrequencyKey.DAILY: {"time_steps": 48, "start_date": "2021-01-01"},
            FrequencyKey.WEEKLY: {"time_steps": 336, "start_date": "2021-01-04"},
            FrequencyKey.MONTHLY: {"time_steps": 1416, "start_date": "2021-01-01"},
            FrequencyKey.THREE_MONTHLY: {
                "time_steps": 4344,
                "start_date": "2021-01-01",
            },
        }

        base_request = {
            "stat": StatKey.PERCENTILE,
            "stat_freq": stat_freq,
            "output_freq": output_freq,
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": True,
            "percentile_list": [0.01, 0.25, 0.5, 0.75, 0.99],
        }

        # Create deterministic test data with appropriate time steps
        cfg = freq_configs[stat_freq]
        test_data = self.create_seeded_test_data(
            time_steps=cfg["time_steps"],
            lat_size=12,
            lon_size=10,
            seed=67890,
            start_date=cfg["start_date"],
        )

        results = {}
        worker_counts = [1, 2, 4]

        # Run computation with different worker counts, using separate checkpoint dirs
        for workers in worker_counts:
            worker_dir = os.path.join(temp_directory, f"workers_{workers}")
            os.makedirs(worker_dir, exist_ok=True)
            request = {
                **base_request,
                "checkpoint_filepath": worker_dir,
                "save_filepath": worker_dir,
            }

            result = self.run_computation(
                request, test_data, chunk_size=8, workers=workers
            )
            assert (
                result is not None
            ), f"Result should not be None for workers={workers}"

            # Store percentile values for comparison
            results[workers] = result["pr"].values.copy()

        # Compare all results against workers=1 (serial baseline)
        baseline = results[1]

        for workers in [2, 4]:
            # Percentiles from TDigest may have small differences due to approximation
            # but should be extremely close for the same input data
            np.testing.assert_allclose(
                baseline,
                results[workers],
                rtol=1e-12,
                atol=1e-12,
                err_msg=f"Percentile results differ between workers=1 and workers={workers} for stat_freq={stat_freq.value}",
            )

        # Verify reasonable percentile values
        assert baseline.size > 0, "Result should have data"

        # Percentiles should be sorted (or close to sorted given TDigest approximation)
        # Check for at least some variation
        assert np.std(baseline) > 0, "Percentiles should show variation"
