"""Test MPI TDigest functionality."""

import pytest
import numpy as np
import xarray as xr
import pandas as pd
from unittest.mock import patch, MagicMock

from one_pass.opa import Opa
from one_pass.statistics.library.tdigest import initialise, update
from one_pass.mpi import MPI_LAUNCHED, MPI_RANK, MPI_SIZE, MPI_get_indices


class TestTDigestSerial:
    """Test TDigest functionality in serial (non-MPI) mode."""

    def setup_opa_instance(self):
        """Create a test OPA instance for serial testing."""
        pass_dic = {
            "stat": "histogram",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": False,
            "bins": 10,
        }
        return Opa(pass_dic)

    def test_serial_tdigest_initialization(self):
        """Test TDigest initialization in serial mode."""
        with patch("one_pass.mpi.MPI_LAUNCHED", False):
            opa = self.setup_opa_instance()
            opa.data_set_info.size_data_source_tail = 50

            tdigests = initialise(opa)

            # Check that we have the right number of TDigests
            assert len(tdigests) == 50

            # Check that all are TDigest objects (not None)
            for tdigest in tdigests:
                assert tdigest is not None
                assert hasattr(tdigest, "update")
                assert hasattr(tdigest, "quantile")

    def test_serial_tdigest_update(self):
        """Test TDigest update in serial mode."""
        with patch("one_pass.mpi.MPI_LAUNCHED", False):
            opa = self.setup_opa_instance()
            opa.data_set_info.size_data_source_tail = 12  # 3x4 grid
            opa.data_set_info.size_data_source_tail = 12
            opa.statistics.cumulative["tdigest"] = initialise(opa)

            # Create test data
            data = np.random.random((2, 3, 4))  # 2 time steps, 3x4 spatial
            data_array = xr.DataArray(
                data,
                dims=["time", "lat", "lon"],
                coords={"time": pd.date_range("2021-01-01", periods=2, freq="H")},
            )

            # Update should not raise errors
            update(opa, data_array, weight=2)

            # Verify all tdigests were updated
            tdigests = opa.statistics.cumulative["tdigest"]
            for tdigest in tdigests:
                assert tdigest.n > 0, "TDigest should have received data"


@pytest.mark.mpi(min_size=2)
class TestMPITDigest:
    """Test MPI TDigest functionality."""

    def setup_opa_instance(self):
        """Create a test OPA instance for MPI testing."""
        pass_dic = {
            "stat": "histogram",
            "stat_freq": "daily",
            "output_freq": "daily",
            "time_step": 60,
            "variable": "pr",
            "save": False,
            "checkpoint": False,
            "bins": 10,
        }
        return Opa(pass_dic)

    def test_mpi_tdigest_initialization(self):
        """Test TDigest initialization in MPI environment."""
        opa = self.setup_opa_instance()

        # Mock data set info
        opa.data_set_info.size_data_source_tail = 100

        tdigests = initialise(opa)

        # Check that we have the right structure
        assert len(tdigests) == 100

        # Check that only local indices have TDigest objects
        start_idx, end_idx = MPI_get_indices(100)

        if start_idx is not None and end_idx is not None:
            for i in range(100):
                if start_idx <= i < end_idx:
                    assert tdigests[i] is not None
                    assert hasattr(tdigests[i], "update")
                else:
                    assert tdigests[i] is None
        else:
            # This rank has no work - all should be None
            for tdigest in tdigests:
                assert tdigest is None

    def test_mpi_tdigest_initialization_small_data(self):
        """Test TDigest initialization when data size < MPI_SIZE."""
        opa = self.setup_opa_instance()

        # Use smaller data than number of processes
        small_size = max(1, MPI_SIZE - 1)
        opa.data_set_info.size_data_source_tail = small_size

        tdigests = initialise(opa)

        assert len(tdigests) == small_size

        # Check distribution
        start_idx, end_idx = MPI_get_indices(small_size)

        if start_idx is not None and end_idx is not None:
            # This rank has work
            for i in range(small_size):
                if start_idx <= i < end_idx:
                    assert tdigests[i] is not None
                else:
                    assert tdigests[i] is None
        else:
            # This rank has no work
            for tdigest in tdigests:
                assert tdigest is None

    def test_mpi_tdigest_update(self):
        """Test TDigest update in MPI environment."""
        opa = self.setup_opa_instance()
        opa.data_set_info.size_data_source_tail = 20  # 4x5 grid
        opa.data_set_info.size_data_source_tail = 20
        opa.statistics.cumulative["tdigest"] = initialise(opa)

        # Create test data
        data = np.random.random((1, 4, 5))  # 1 time step, 4x5 spatial
        data_array = xr.DataArray(
            data,
            dims=["time", "lat", "lon"],
            coords={"time": pd.date_range("2021-01-01", periods=1, freq="H")},
        )

        # Update should not raise errors
        update(opa, data_array, weight=1)

        # Verify local tdigests were updated
        start_idx, end_idx = MPI_get_indices(20)
        tdigests = opa.statistics.cumulative["tdigest"]

        if start_idx is not None and end_idx is not None:
            for i in range(start_idx, end_idx):
                assert tdigests[i] is not None
                # TDigest should have received data
                assert tdigests[i].n > 0

    def test_mpi_tdigest_update_multiple_timesteps(self):
        """Test TDigest update with multiple timesteps in MPI environment."""
        opa = self.setup_opa_instance()
        opa.data_set_info.size_data_source_tail = 16  # 4x4 grid
        opa.data_set_info.size_data_source_tail = 16
        opa.statistics.cumulative["tdigest"] = initialise(opa)

        # Create test data with multiple timesteps
        data = np.random.random((3, 4, 4))  # 3 time steps, 4x4 spatial
        data_array = xr.DataArray(
            data,
            dims=["time", "lat", "lon"],
            coords={"time": pd.date_range("2021-01-01", periods=3, freq="H")},
        )

        # Update with weight > 1
        update(opa, data_array, weight=3)

        # Verify local tdigests were updated
        start_idx, end_idx = MPI_get_indices(16)
        tdigests = opa.statistics.cumulative["tdigest"]

        if start_idx is not None and end_idx is not None:
            for i in range(start_idx, end_idx):
                assert tdigests[i] is not None
                # TDigest should have received 3 data points
                assert tdigests[i].n == 3

    def test_mpi_tdigest_data_consistency(self):
        """Test that MPI TDigest update produces consistent results."""
        opa = self.setup_opa_instance()
        opa.data_set_info.size_data_source_tail = (
            MPI_SIZE * 4
        )  # Ensure even distribution
        opa.data_set_info.size_data_source_tail = MPI_SIZE * 4
        opa.statistics.cumulative["tdigest"] = initialise(opa)

        # Create deterministic test data
        np.random.seed(42)  # Fixed seed for reproducibility
        data = np.random.random(
            (1, 2, MPI_SIZE * 2)
        )  # Reshape to match size_data_source_tail
        data_array = xr.DataArray(
            data,
            dims=["time", "lat", "lon"],
            coords={"time": pd.date_range("2021-01-01", periods=1, freq="H")},
        )

        update(opa, data_array, weight=1)

        # Gather TDigest statistics from all ranks
        start_idx, end_idx = MPI_get_indices(MPI_SIZE * 4)
        local_stats = []

        if start_idx is not None and end_idx is not None:
            tdigests = opa.statistics.cumulative["tdigest"]
            for i in range(start_idx, end_idx):
                if tdigests[i] is not None:
                    local_stats.append(
                        (i, tdigests[i].n, float(tdigests[i].quantile(0.5)))
                    )

        from one_pass.mpi import MPI_COMM

        all_stats = MPI_COMM.allgather(local_stats)

        if MPI_RANK == 0:
            # Flatten the list of lists
            flat_stats = [item for sublist in all_stats for item in sublist]

            # Check that all indices are covered and have data
            indices_covered = {stat[0] for stat in flat_stats}
            expected_indices = set(range(MPI_SIZE * 4))
            assert indices_covered == expected_indices

            # Check that all TDigests received exactly one data point
            for idx, n, median in flat_stats:
                assert n == 1, f"TDigest at index {idx} should have n=1, got {n}"
                assert 0.0 <= median <= 1.0, f"Median {median} out of expected range"
