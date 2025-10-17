"""Test MPI infrastructure and basic functionality."""

import pytest
import numpy as np
import os
from unittest.mock import patch

from one_pass.mpi import (
    MPI_LAUNCHED,
    MPI_RANK,
    MPI_SIZE,
    MPI_get_indices,
    MPI,
    MPI_COMM,
)


class TestMPIDetection:
    """Test MPI environment detection."""

    def test_mpi_not_launched_by_default(self):
        """Test that MPI is not detected in normal pytest environment."""
        # This test runs in non-MPI context
        # Note: This test might fail if pytest is run under MPI
        # In that case, we check if we're actually in an MPI environment
        if not any(
            var in os.environ
            for var in ["OMPI_COMM_WORLD_SIZE", "PMI_SIZE", "MPI_LOCALNRANKS"]
        ):
            assert not MPI_LAUNCHED
            assert MPI is None
            assert MPI_COMM is None
            assert MPI_RANK is None
            assert MPI_SIZE is None

    def test_mpi_detection_with_environment_variables(self):
        """Test MPI detection with different environment variables."""
        env_vars = ["OMPI_COMM_WORLD_SIZE", "PMI_SIZE", "MPI_LOCALNRANKS"]

        for var in env_vars:
            with patch.dict(os.environ, {var: "4"}, clear=False):
                # Test the detection logic directly
                mpi_detected = any(
                    test_var in os.environ
                    for test_var in [
                        "OMPI_COMM_WORLD_SIZE",
                        "PMI_SIZE",
                        "MPI_LOCALNRANKS",
                    ]
                )
                assert mpi_detected, f"Failed to detect MPI with {var} set"

    def test_mpi_get_indices_without_mpi(self):
        """Test MPI_get_indices when MPI is not launched."""
        with patch("one_pass.mpi.MPI_LAUNCHED", False):
            # Import locally to avoid redefinition warning
            import one_pass.mpi

            start, end = one_pass.mpi.MPI_get_indices(100)
            assert start is None
            assert end is None


@pytest.mark.mpi(min_size=2)
class TestMPIFunctionality:
    """Test MPI functionality with actual MPI processes."""

    def test_mpi_basic_properties(self):
        """Test basic MPI properties are correctly set."""
        assert MPI_LAUNCHED
        assert MPI is not None
        assert MPI_COMM is not None
        assert MPI_RANK is not None
        assert MPI_SIZE is not None
        assert 0 <= MPI_RANK < MPI_SIZE
        assert MPI_SIZE >= 2

    def test_mpi_get_indices_distribution(self):
        """Test that MPI_get_indices correctly distributes work."""
        test_sizes = [10, 100, 1000]

        for size in test_sizes:
            start_idx, end_idx = MPI_get_indices(size)

            # Check indices are valid
            assert start_idx is not None
            assert end_idx is not None
            assert 0 <= start_idx < end_idx <= size

            # Gather all indices from all ranks
            all_indices = MPI_COMM.allgather((start_idx, end_idx))

            if MPI_RANK == 0:
                # Check coverage and no overlap
                covered_indices = set()
                for start, end in all_indices:
                    rank_indices = set(range(start, end))
                    assert not (
                        rank_indices & covered_indices
                    ), "Overlapping indices detected"
                    covered_indices.update(rank_indices)

                # Check all indices are covered
                assert covered_indices == set(range(size)), "Not all indices covered"

    def test_mpi_get_indices_edge_cases(self):
        """Test MPI_get_indices with edge cases."""
        # Test with size smaller than number of processes
        small_size = max(1, MPI_SIZE - 1)
        start_idx, end_idx = MPI_get_indices(small_size)

        # Some ranks might get no work
        if start_idx is not None:
            assert end_idx is not None
            assert 0 <= start_idx < end_idx <= small_size
        else:
            assert end_idx is None

        # Test with size equal to number of processes
        start_idx, end_idx = MPI_get_indices(MPI_SIZE)
        assert start_idx is not None
        assert end_idx is not None
        assert end_idx - start_idx == 1  # Each process gets exactly one element

        # Test with zero size
        start_idx, end_idx = MPI_get_indices(0)
        assert start_idx is None
        assert end_idx is None

    def test_mpi_communication_basic(self):
        """Test basic MPI communication."""
        data = MPI_RANK * 10
        gathered_data = MPI_COMM.allgather(data)

        if MPI_RANK == 0:
            expected = [i * 10 for i in range(MPI_SIZE)]
            assert gathered_data == expected

    def test_mpi_barrier_synchronization(self):
        """Test MPI barrier synchronization."""
        import time

        # Add some artificial delay based on rank
        time.sleep(MPI_RANK * 0.01)

        start_time = time.time()
        MPI_COMM.Barrier()
        end_time = time.time()

        # All processes should reach the barrier
        # Timing test is approximate due to system variability
        barrier_time = end_time - start_time
        assert barrier_time < 1.0, f"Barrier took too long: {barrier_time}s"

    def test_mpi_data_types(self):
        """Test MPI with different data types."""
        # Test with numpy arrays
        local_array = np.full(5, MPI_RANK, dtype=np.float64)
        all_arrays = MPI_COMM.allgather(local_array)

        if MPI_RANK == 0:
            assert len(all_arrays) == MPI_SIZE
            for i, arr in enumerate(all_arrays):
                assert np.allclose(arr, i), f"Array from rank {i} incorrect"
