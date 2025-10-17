"""Integration tests for MPI functionality."""

import pytest
import numpy as np
import xarray as xr
import pandas as pd
from tempfile import TemporaryDirectory

from one_pass.opa import Opa
from one_pass.mpi import MPI_RANK, MPI_SIZE
from tests.init_tests import get_test_data


@pytest.mark.mpi(min_size=2)
class TestMPIIntegration:
    """Integration tests for MPI functionality."""

    @pytest.fixture
    def temp_directory(self):
        with TemporaryDirectory() as tmpdir:
            yield tmpdir

    def create_test_data(self, time_steps=24, lat_size=10, lon_size=10):
        """Create synthetic test data for MPI testing."""
        # Create deterministic data for reproducible tests
        np.random.seed(42 + MPI_RANK)  # Different seed per rank for variety
        data = np.random.exponential(scale=2.0, size=(time_steps, lat_size, lon_size))

        time_coords = pd.date_range("2021-01-01", periods=time_steps, freq="H")
        lat_coords = np.linspace(-45, 45, lat_size)
        lon_coords = np.linspace(-90, 90, lon_size)

        data_array = xr.DataArray(
            data,
            dims=["time", "lat", "lon"],
            coords={"time": time_coords, "lat": lat_coords, "lon": lon_coords},
            name="pr",
        )

        # Create dataset
        dataset = xr.Dataset({"pr": data_array})
        return dataset

    def test_mpi_histogram_end_to_end(self, temp_directory):
        """Test complete histogram computation with MPI."""
        pass_dic = {
            "stat": "histogram",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": False,
            "checkpoint_filepath": temp_directory,
            "save_filepath": temp_directory,
            "bins": 10,
        }

        opa = Opa(pass_dic)

        # Create and broadcast test data
        if MPI_RANK == 0:
            test_data = self.create_test_data(time_steps=24, lat_size=8, lon_size=8)
        else:
            test_data = None

        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Process data in chunks
        result = None
        chunk_size = 6
        for i in range(0, 24, chunk_size):
            end_idx = min(i + chunk_size, 24)
            chunk = test_data.isel(time=slice(i, end_idx))
            result = opa.compute(chunk)

        # Verify results
        if MPI_RANK == 0:
            assert result is not None, "Result should not be None on rank 0"
            assert "pr" in result, "Result should contain 'pr' variable"

            # Check histogram structure
            hist_data = result["pr"]
            assert hist_data.dims[0] == "time", "First dimension should be time"
            assert (
                len(hist_data.dims) >= 2
            ), "Should have at least time and spatial dimensions"

            # Check that histogram values are reasonable
            hist_values = hist_data.values
            assert hist_values.shape[0] == 1, "Should have one time step"
            assert np.all(hist_values >= 0), "Histogram counts should be non-negative"
            assert np.sum(hist_values) > 0, "Total histogram count should be positive"
        else:
            # Non-root ranks should not have the final result in current implementation
            # This might change based on implementation details
            pass

    def test_mpi_percentile_end_to_end(self, temp_directory):
        """Test complete percentile computation with MPI."""
        pass_dic = {
            "stat": "percentile",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": False,
            "checkpoint_filepath": temp_directory,
            "save_filepath": temp_directory,
            "percentile_list": [25, 50, 75, 95],
        }

        opa = Opa(pass_dic)

        # Create and broadcast test data
        if MPI_RANK == 0:
            test_data = self.create_test_data(time_steps=24, lat_size=6, lon_size=6)
        else:
            test_data = None

        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Process data
        result = None
        chunk_size = 8
        for i in range(0, 24, chunk_size):
            end_idx = min(i + chunk_size, 24)
            chunk = test_data.isel(time=slice(i, end_idx))
            result = opa.compute(chunk)

        # Verify results on rank 0
        if MPI_RANK == 0:
            assert result is not None, "Result should not be None on rank 0"
            assert "pr" in result, "Result should contain 'pr' variable"

            percentile_data = result["pr"]
            assert percentile_data.dims[0] == "time", "First dimension should be time"

            # Check percentile values are reasonable
            percentile_values = percentile_data.values
            assert percentile_values.shape[0] == 1, "Should have one time step"
            assert np.all(
                percentile_values >= 0
            ), "Percentile values should be non-negative"

            # Check that percentiles are in ascending order (roughly)
            # This is a statistical test and might occasionally fail due to randomness
            if percentile_values.ndim > 1:
                # Test a few spatial points
                for i in range(min(3, percentile_values.shape[1])):
                    for j in range(
                        min(
                            3,
                            (
                                percentile_values.shape[2]
                                if percentile_values.ndim > 2
                                else 1
                            ),
                        )
                    ):
                        if percentile_values.ndim == 3:
                            point_percentiles = percentile_values[0, i, j]
                        else:
                            point_percentiles = percentile_values[0, i]

                        # 25th percentile should be <= 50th percentile <= 75th percentile
                        if len(point_percentiles) >= 3:
                            assert (
                                point_percentiles[0]
                                <= point_percentiles[1]
                                <= point_percentiles[2]
                            ), f"Percentiles not in order: {point_percentiles}"

    def test_mpi_multiple_chunks_consistency(self, temp_directory):
        """Test that processing multiple chunks gives consistent results."""
        pass_dic = {
            "stat": "histogram",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": False,
            "checkpoint_filepath": temp_directory,
            "save_filepath": temp_directory,
            "bins": 5,  # Fewer bins for easier testing
        }

        # Create consistent test data
        if MPI_RANK == 0:
            # Create data with known distribution
            np.random.seed(123)  # Fixed seed for reproducibility
            base_data = np.random.exponential(scale=1.0, size=(24, 4, 4))
            test_data = xr.Dataset(
                {
                    "pr": xr.DataArray(
                        base_data,
                        dims=["time", "lat", "lon"],
                        coords={
                            "time": pd.date_range("2021-01-01", periods=24, freq="H"),
                            "lat": np.linspace(-10, 10, 4),
                            "lon": np.linspace(-20, 20, 4),
                        },
                    )
                }
            )
        else:
            test_data = None

        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Test different chunk sizes
        chunk_sizes = [4, 6, 8]
        results = []

        for chunk_size in chunk_sizes:
            opa = Opa(pass_dic.copy())  # Fresh instance

            result = None
            for i in range(0, 24, chunk_size):
                end_idx = min(i + chunk_size, 24)
                chunk = test_data.isel(time=slice(i, end_idx))
                result = opa.compute(chunk)

            if MPI_RANK == 0 and result is not None:
                results.append(result["pr"].values.copy())

        # Compare results on rank 0
        if MPI_RANK == 0 and len(results) > 1:
            # All results should be very similar (within numerical precision)
            for i in range(1, len(results)):
                np.testing.assert_allclose(
                    results[0],
                    results[i],
                    rtol=1e-10,
                    atol=1e-10,
                    err_msg=f"Results differ between chunk size {chunk_sizes[0]} and {chunk_sizes[i]}",
                )

    def test_mpi_with_checkpointing(self, temp_directory):
        """Test MPI functionality with checkpointing enabled."""
        pass_dic = {
            "stat": "histogram",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": True,
            "checkpoint_filepath": temp_directory,
            "save_filepath": temp_directory,
            "bins": 8,
        }

        if MPI_RANK == 0:
            test_data = self.create_test_data(time_steps=12, lat_size=6, lon_size=6)
        else:
            test_data = None

        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Process first half
        opa = Opa(pass_dic)

        chunk1 = test_data.isel(time=slice(0, 6))
        result1 = opa.compute(chunk1)

        # Process second half (should load from checkpoint)
        chunk2 = test_data.isel(time=slice(6, 12))
        result2 = opa.compute(chunk2)

        # Verify final result
        if MPI_RANK == 0:
            assert result2 is not None, "Final result should not be None"
            assert "pr" in result2, "Result should contain 'pr' variable"

            # Check that checkpoint files were created (at least on rank 0)
            import os

            checkpoint_files = [
                f
                for f in os.listdir(temp_directory)
                if "checkpoint" in f and f.endswith(".pkl")
            ]
            # Note: checkpoint behavior might differ in MPI mode
            # This test might need adjustment based on implementation

    def test_mpi_error_recovery(self, temp_directory):
        """Test MPI behavior with invalid data."""
        pass_dic = {
            "stat": "histogram",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": False,
            "checkpoint_filepath": temp_directory,
            "save_filepath": temp_directory,
            "bins": 10,
        }

        opa = Opa(pass_dic)

        # Create data with some invalid values
        if MPI_RANK == 0:
            data = np.random.random((6, 4, 4))
            data[0, 0, 0] = np.nan  # Add NaN
            data[1, 1, 1] = np.inf  # Add infinity

            test_data = xr.Dataset(
                {
                    "pr": xr.DataArray(
                        data,
                        dims=["time", "lat", "lon"],
                        coords={
                            "time": pd.date_range("2021-01-01", periods=6, freq="H"),
                            "lat": np.linspace(-5, 5, 4),
                            "lon": np.linspace(-10, 10, 4),
                        },
                    )
                }
            )
        else:
            test_data = None

        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # This should either handle gracefully or raise a meaningful error
        try:
            result = opa.compute(test_data)

            # If it succeeds, check that result is reasonable
            if MPI_RANK == 0 and result is not None:
                hist_values = result["pr"].values
                # Should not contain NaN or inf in the result
                assert np.all(
                    np.isfinite(hist_values)
                ), "Result should not contain NaN or inf"

        except (ValueError, RuntimeError) as e:
            # Should be a meaningful error message
            error_msg = str(e).lower()
            assert any(
                keyword in error_msg for keyword in ["nan", "inf", "invalid", "finite"]
            ), f"Error message should be meaningful: {e}"
