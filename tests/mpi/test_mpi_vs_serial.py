"""Compare MPI results with serial computation."""

import pytest
import numpy as np
import xarray as xr
import pandas as pd
from tempfile import TemporaryDirectory
from unittest.mock import patch

from one_pass.opa import Opa
from one_pass.mpi import MPI_RANK


@pytest.mark.mpi(min_size=2)
class TestMPIVsSerial:
    """Compare MPI results with serial computation."""

    @pytest.fixture
    def temp_directory(self):
        with TemporaryDirectory() as tmpdir:
            yield tmpdir

    def create_deterministic_data(self, time_steps=24, lat_size=8, lon_size=8, seed=42):
        """Create deterministic test data for reproducible comparisons."""
        np.random.seed(seed)

        # Create realistic precipitation-like data (exponential distribution)
        data = np.random.exponential(scale=1.5, size=(time_steps, lat_size, lon_size))

        # Add some spatial and temporal patterns
        for t in range(time_steps):
            for i in range(lat_size):
                for j in range(lon_size):
                    # Add spatial gradient
                    data[t, i, j] *= 1.0 + 0.1 * i / lat_size
                    # Add temporal variation
                    data[t, i, j] *= 1.0 + 0.05 * np.sin(2 * np.pi * t / 24)

        time_coords = pd.date_range("2021-01-01", periods=time_steps, freq="H")
        lat_coords = np.linspace(-20, 20, lat_size)
        lon_coords = np.linspace(-40, 40, lon_size)

        dataset = xr.Dataset(
            {
                "pr": xr.DataArray(
                    data,
                    dims=["time", "lat", "lon"],
                    coords={"time": time_coords, "lat": lat_coords, "lon": lon_coords},
                )
            }
        )

        return dataset

    def run_serial_computation(self, pass_dic, test_data, chunk_size=6):
        """Run computation in serial mode."""
        with patch("one_pass.mpi.MPI_LAUNCHED", False):
            opa_serial = Opa(pass_dic)

            result = None
            time_steps = test_data.dims["time"]
            for i in range(0, time_steps, chunk_size):
                end_idx = min(i + chunk_size, time_steps)
                chunk = test_data.isel(time=slice(i, end_idx))
                result = opa_serial.compute(chunk)

            return result

    def run_mpi_computation(self, pass_dic, test_data, chunk_size=6):
        """Run computation in MPI mode."""
        opa_mpi = Opa(pass_dic)

        result = None
        time_steps = test_data.dims["time"]
        for i in range(0, time_steps, chunk_size):
            end_idx = min(i + chunk_size, time_steps)
            chunk = test_data.isel(time=slice(i, end_idx))
            result = opa_mpi.compute(chunk)

        return result

    def test_histogram_mpi_vs_serial_accuracy(self, temp_directory):
        """Compare MPI and serial histogram results."""
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
            "range": [0.0, 5.0],  # Fixed range for consistent comparison
        }

        # Create test data
        if MPI_RANK == 0:
            test_data = self.create_deterministic_data(
                time_steps=24, lat_size=6, lon_size=6
            )

            # Run serial computation
            result_serial = self.run_serial_computation(pass_dic, test_data)
        else:
            test_data = None
            result_serial = None

        # Broadcast test data to all ranks
        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Run MPI computation
        result_mpi = self.run_mpi_computation(pass_dic, test_data)

        # Compare results on rank 0
        if MPI_RANK == 0:
            assert result_serial is not None, "Serial result should not be None"
            assert result_mpi is not None, "MPI result should not be None"

            # Compare histogram counts
            serial_values = result_serial["pr"].values
            mpi_values = result_mpi["pr"].values

            np.testing.assert_allclose(
                serial_values,
                mpi_values,
                rtol=1e-12,
                atol=1e-12,
                err_msg="MPI and serial histogram results differ",
            )

            # Also check histogram edges if available
            if hasattr(result_serial, "bin_edges") and hasattr(result_mpi, "bin_edges"):
                np.testing.assert_allclose(
                    result_serial.bin_edges.values,
                    result_mpi.bin_edges.values,
                    rtol=1e-12,
                    atol=1e-12,
                    err_msg="MPI and serial histogram bin edges differ",
                )

    def test_percentile_mpi_vs_serial_accuracy(self, temp_directory):
        """Compare MPI and serial percentile results."""
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
            "percentile_list": [10, 25, 50, 75, 90, 95, 99],
        }

        # Create test data
        if MPI_RANK == 0:
            test_data = self.create_deterministic_data(
                time_steps=24, lat_size=6, lon_size=6
            )

            # Run serial computation
            result_serial = self.run_serial_computation(pass_dic, test_data)
        else:
            test_data = None
            result_serial = None

        # Broadcast test data
        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Run MPI computation
        result_mpi = self.run_mpi_computation(pass_dic, test_data)

        # Compare results on rank 0
        if MPI_RANK == 0:
            assert result_serial is not None, "Serial result should not be None"
            assert result_mpi is not None, "MPI result should not be None"

            # Compare percentile values
            serial_values = result_serial["pr"].values
            mpi_values = result_mpi["pr"].values

            # For percentiles, allow slightly larger tolerance due to TDigest approximation
            np.testing.assert_allclose(
                serial_values,
                mpi_values,
                rtol=1e-10,
                atol=1e-10,
                err_msg="MPI and serial percentile results differ",
            )

    def test_different_chunk_sizes_consistency(self, temp_directory):
        """Test that different chunk sizes give same results in both MPI and serial."""
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
            "bins": 8,
        }

        if MPI_RANK == 0:
            test_data = self.create_deterministic_data(
                time_steps=24, lat_size=4, lon_size=4
            )
        else:
            test_data = None

        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        chunk_sizes = [3, 6, 12]
        mpi_results = []
        serial_results = []

        for chunk_size in chunk_sizes:
            # MPI computation
            result_mpi = self.run_mpi_computation(
                pass_dic.copy(), test_data, chunk_size
            )
            if MPI_RANK == 0 and result_mpi is not None:
                mpi_results.append(result_mpi["pr"].values.copy())

            # Serial computation (only on rank 0)
            if MPI_RANK == 0:
                result_serial = self.run_serial_computation(
                    pass_dic.copy(), test_data, chunk_size
                )
                if result_serial is not None:
                    serial_results.append(result_serial["pr"].values.copy())

        # Compare results on rank 0
        if MPI_RANK == 0:
            # All MPI results should be identical
            for i in range(1, len(mpi_results)):
                np.testing.assert_allclose(
                    mpi_results[0],
                    mpi_results[i],
                    rtol=1e-12,
                    atol=1e-12,
                    err_msg=f"MPI results differ between chunk sizes {chunk_sizes[0]} and {chunk_sizes[i]}",
                )

            # All serial results should be identical
            for i in range(1, len(serial_results)):
                np.testing.assert_allclose(
                    serial_results[0],
                    serial_results[i],
                    rtol=1e-12,
                    atol=1e-12,
                    err_msg=f"Serial results differ between chunk sizes {chunk_sizes[0]} and {chunk_sizes[i]}",
                )

            # MPI and serial should match
            if len(mpi_results) > 0 and len(serial_results) > 0:
                np.testing.assert_allclose(
                    mpi_results[0],
                    serial_results[0],
                    rtol=1e-12,
                    atol=1e-12,
                    err_msg="MPI and serial results differ",
                )

    def test_large_data_mpi_vs_serial(self, temp_directory):
        """Test MPI vs serial with larger data to ensure scalability."""
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
            "bins": 15,
        }

        # Use larger spatial dimensions to test MPI distribution
        if MPI_RANK == 0:
            # Create data that's larger than typical test cases
            test_data = self.create_deterministic_data(
                time_steps=12, lat_size=20, lon_size=20, seed=789  # 400 spatial points
            )

            # Run serial computation
            result_serial = self.run_serial_computation(
                pass_dic, test_data, chunk_size=4
            )
        else:
            test_data = None
            result_serial = None

        # Broadcast test data
        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Run MPI computation
        result_mpi = self.run_mpi_computation(pass_dic, test_data, chunk_size=4)

        # Compare results
        if MPI_RANK == 0:
            assert result_serial is not None, "Serial result should not be None"
            assert result_mpi is not None, "MPI result should not be None"

            serial_values = result_serial["pr"].values
            mpi_values = result_mpi["pr"].values

            # Check shapes match
            assert (
                serial_values.shape == mpi_values.shape
            ), f"Shape mismatch: serial {serial_values.shape} vs MPI {mpi_values.shape}"

            # Check values match
            np.testing.assert_allclose(
                serial_values,
                mpi_values,
                rtol=1e-12,
                atol=1e-12,
                err_msg="Large data MPI and serial results differ",
            )

            # Additional check: ensure we're actually testing substantial data
            assert serial_values.size > 100, "Test data should be substantial"
            assert np.sum(serial_values) > 0, "Should have non-zero histogram counts"

    def test_edge_case_data_distribution(self, temp_directory):
        """Test with data distributions that stress the MPI implementation."""
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

        if MPI_RANK == 0:
            # Create data with challenging distribution
            np.random.seed(999)

            # Mix of different distributions to test edge cases
            data1 = np.random.exponential(scale=0.1, size=(6, 3, 3))  # Small values
            data2 = np.random.exponential(scale=10.0, size=(6, 3, 3))  # Large values
            data3 = np.zeros((6, 3, 3))  # Zeros
            data4 = np.full((6, 3, 3), 1.0)  # Constant values

            combined_data = np.concatenate([data1, data2, data3, data4], axis=0)

            test_data = xr.Dataset(
                {
                    "pr": xr.DataArray(
                        combined_data,
                        dims=["time", "lat", "lon"],
                        coords={
                            "time": pd.date_range("2021-01-01", periods=24, freq="H"),
                            "lat": np.linspace(-5, 5, 3),
                            "lon": np.linspace(-10, 10, 3),
                        },
                    )
                }
            )

            # Run serial computation
            result_serial = self.run_serial_computation(pass_dic, test_data)
        else:
            test_data = None
            result_serial = None

        # Broadcast and run MPI computation
        from one_pass.mpi import MPI_COMM

        test_data = MPI_COMM.bcast(test_data, root=0)
        result_mpi = self.run_mpi_computation(pass_dic, test_data)

        # Compare results
        if MPI_RANK == 0:
            assert result_serial is not None, "Serial result should not be None"
            assert result_mpi is not None, "MPI result should not be None"

            np.testing.assert_allclose(
                result_serial["pr"].values,
                result_mpi["pr"].values,
                rtol=1e-12,
                atol=1e-12,
                err_msg="Edge case data: MPI and serial results differ",
            )
