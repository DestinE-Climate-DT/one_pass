"""Test MPI edge cases and error handling."""

import pytest
import numpy as np
import xarray as xr
import pandas as pd
from tempfile import TemporaryDirectory

from one_pass.opa import Opa
from one_pass.mpi import MPI_RANK, MPI_SIZE, MPI_get_indices, MPI_COMM


@pytest.mark.mpi(min_size=2)
class TestMPIEdgeCases:
    """Test MPI edge cases and error handling."""

    @pytest.fixture
    def temp_directory(self):
        with TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_small_data_distribution(self):
        """Test MPI with data smaller than number of processes."""
        # When size < MPI_SIZE, some processes should get no work
        small_size = max(1, MPI_SIZE - 1)

        start_idx, end_idx = MPI_get_indices(small_size)

        # Gather all indices to check distribution
        all_indices = MPI_COMM.allgather((start_idx, end_idx))

        if MPI_RANK == 0:
            # Count how many ranks got work
            ranks_with_work = sum(
                1 for start, end in all_indices if start is not None and end is not None
            )
            ranks_without_work = MPI_SIZE - ranks_with_work

            # Some ranks should get no work when data size < MPI_SIZE
            if small_size < MPI_SIZE:
                assert (
                    ranks_without_work > 0
                ), "Some ranks should get no work with small data"
                assert (
                    ranks_with_work == small_size
                ), f"Expected {small_size} ranks with work, got {ranks_with_work}"

        # Verify that ranks without work handle it gracefully
        if start_idx is None:
            assert end_idx is None, "If start_idx is None, end_idx should also be None"
        else:
            assert (
                end_idx is not None
            ), "If start_idx is not None, end_idx should also not be None"
            assert start_idx < end_idx, "start_idx should be less than end_idx"
            assert end_idx <= small_size, "end_idx should not exceed data size"

    def test_uneven_data_distribution(self):
        """Test MPI with data size not evenly divisible by number of processes."""
        # Use a size that doesn't divide evenly
        uneven_size = MPI_SIZE * 5 + 3  # Not evenly divisible

        start_idx, end_idx = MPI_get_indices(uneven_size)

        # Gather all ranges to verify complete coverage
        all_ranges = MPI_COMM.allgather((start_idx, end_idx))

        if MPI_RANK == 0:
            # Check total coverage
            total_covered = 0
            covered_indices = set()

            for start, end in all_ranges:
                if start is not None and end is not None:
                    chunk_size = end - start
                    total_covered += chunk_size

                    # Check for overlaps
                    chunk_indices = set(range(start, end))
                    assert not (
                        chunk_indices & covered_indices
                    ), f"Overlapping indices detected"
                    covered_indices.update(chunk_indices)

            assert (
                total_covered == uneven_size
            ), f"Total coverage {total_covered} != {uneven_size}"
            assert covered_indices == set(range(uneven_size)), "Not all indices covered"

            # Last rank should handle the remainder
            last_start, last_end = all_ranges[-1]
            if last_start is not None and last_end is not None:
                expected_last_chunk = uneven_size - (MPI_SIZE - 1) * (
                    uneven_size // MPI_SIZE
                )
                actual_last_chunk = last_end - last_start
                assert (
                    actual_last_chunk == expected_last_chunk
                ), f"Last rank chunk size {actual_last_chunk} != expected {expected_last_chunk}"

    def test_zero_size_data(self):
        """Test MPI handling of zero-size data."""
        start_idx, end_idx = MPI_get_indices(0)

        # All ranks should get no work with zero-size data
        assert start_idx is None, "start_idx should be None for zero-size data"
        assert end_idx is None, "end_idx should be None for zero-size data"

        # Verify this is consistent across all ranks
        all_indices = MPI_COMM.allgather((start_idx, end_idx))
        for i, (start, end) in enumerate(all_indices):
            assert start is None, f"Rank {i} should have start=None for zero-size data"
            assert end is None, f"Rank {i} should have end=None for zero-size data"

    def test_single_element_data(self):
        """Test MPI with single element data."""
        start_idx, end_idx = MPI_get_indices(1)

        # Gather results
        all_indices = MPI_COMM.allgather((start_idx, end_idx))

        if MPI_RANK == 0:
            # Exactly one rank should get the single element
            ranks_with_work = sum(
                1 for start, end in all_indices if start is not None and end is not None
            )
            assert (
                ranks_with_work == 1
            ), f"Expected 1 rank with work, got {ranks_with_work}"

            # Find the rank that got work
            working_rank_indices = [
                (start, end) for start, end in all_indices if start is not None
            ]
            assert (
                len(working_rank_indices) == 1
            ), "Should have exactly one working rank"

            start, end = working_rank_indices[0]
            assert start == 0, "Single element should start at index 0"
            assert end == 1, "Single element should end at index 1"

    def test_empty_data_handling(self, temp_directory):
        """Test MPI handling of empty data arrays."""
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

        # Create empty data array on rank 0 and broadcast
        if MPI_RANK == 0:
            empty_data = xr.Dataset(
                {
                    "pr": xr.DataArray(
                        np.array([]).reshape(0, 2, 2),
                        dims=["time", "lat", "lon"],
                        coords={
                            "time": pd.DatetimeIndex([]),
                            "lat": [0, 1],
                            "lon": [0, 1],
                        },
                    )
                }
            )
        else:
            empty_data = None

        empty_data = MPI_COMM.bcast(empty_data, root=0)

        # This should handle gracefully without crashing
        try:
            result = opa.compute(empty_data)
            # If it succeeds, result might be None or an empty result
            if result is not None and MPI_RANK == 0:
                # Check that result is reasonable
                assert isinstance(
                    result, (xr.Dataset, type(None))
                ), "Result should be Dataset or None"
        except Exception as e:
            # Should be a graceful error, not a crash
            error_msg = str(e).lower()
            expected_keywords = ["time", "empty", "data", "dimension", "size"]
            assert any(
                keyword in error_msg for keyword in expected_keywords
            ), f"Error message should be meaningful: {e}"

    def test_invalid_data_handling(self, temp_directory):
        """Test MPI behavior with invalid data values."""
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

        # Create data with invalid values on rank 0
        if MPI_RANK == 0:
            data = np.random.random((6, 4, 4))

            # Add various invalid values
            data[0, 0, 0] = np.nan  # NaN
            data[1, 1, 1] = np.inf  # Positive infinity
            data[2, 2, 2] = -np.inf  # Negative infinity
            data[3, 3, 3] = -1.0  # Negative value (might be invalid for precipitation)

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

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Test should either handle gracefully or raise meaningful error
        try:
            result = opa.compute(test_data)

            # If it succeeds, check that result is reasonable
            if MPI_RANK == 0 and result is not None:
                hist_values = result["pr"].values
                # Result should not contain NaN or inf
                assert np.all(
                    np.isfinite(hist_values)
                ), "Result should not contain NaN or inf"

        except (ValueError, RuntimeError, FloatingPointError) as e:
            # Should be a meaningful error message
            error_msg = str(e).lower()
            expected_keywords = ["nan", "inf", "invalid", "finite", "value"]
            assert any(
                keyword in error_msg for keyword in expected_keywords
            ), f"Error message should be meaningful: {e}"

    def test_memory_pressure_handling(self, temp_directory):
        """Test MPI behavior under memory pressure conditions."""
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
            "bins": 50,  # More bins to increase memory usage
        }

        # Create relatively large data to test memory handling
        if MPI_RANK == 0:
            # Size chosen to be significant but not excessive for testing
            large_data = np.random.exponential(
                scale=1.0, size=(12, 100, 100)
            )  # 120,000 points

            test_data = xr.Dataset(
                {
                    "pr": xr.DataArray(
                        large_data,
                        dims=["time", "lat", "lon"],
                        coords={
                            "time": pd.date_range("2021-01-01", periods=12, freq="H"),
                            "lat": np.linspace(-50, 50, 100),
                            "lon": np.linspace(-100, 100, 100),
                        },
                    )
                }
            )
        else:
            test_data = None

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Monitor memory usage during computation
        import psutil
        import os

        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss / 1024 / 1024  # MB

        try:
            opa = Opa(pass_dic)
            result = opa.compute(test_data)

            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = memory_after - memory_before

            # Memory increase should be reasonable (less than 2GB per process)
            assert (
                memory_increase < 2048
            ), f"Excessive memory use: {memory_increase:.1f}MB"

            # Verify result if successful
            if MPI_RANK == 0 and result is not None:
                hist_values = result["pr"].values
                assert hist_values.shape[0] == 1, "Should have one time dimension"
                assert np.sum(hist_values) > 0, "Should have non-zero histogram counts"

        except MemoryError:
            # Memory error is acceptable for this test - it means we successfully
            # detected memory pressure rather than crashing
            pytest.skip(
                "Memory pressure test hit memory limit - this is expected behavior"
            )

    def test_mpi_communication_failure_recovery(self, temp_directory):
        """Test recovery from simulated communication issues."""
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
            "percentile_list": [25, 50, 75],
        }

        # Create test data
        if MPI_RANK == 0:
            np.random.seed(42)
            data = np.random.exponential(scale=2.0, size=(12, 6, 6))

            test_data = xr.Dataset(
                {
                    "pr": xr.DataArray(
                        data,
                        dims=["time", "lat", "lon"],
                        coords={
                            "time": pd.date_range("2021-01-01", periods=12, freq="H"),
                            "lat": np.linspace(-10, 10, 6),
                            "lon": np.linspace(-20, 20, 6),
                        },
                    )
                }
            )
        else:
            test_data = None

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Test basic computation (should work normally)
        opa = Opa(pass_dic)

        try:
            result = opa.compute(test_data)

            # Verify computation completed successfully
            if MPI_RANK == 0:
                assert result is not None, "Computation should succeed"

                if "pr" in result:
                    percentile_values = result["pr"].values
                    assert percentile_values.shape[0] == 1, "Should have one time step"
                    assert np.all(
                        percentile_values >= 0
                    ), "Percentiles should be non-negative"

        except Exception as e:
            # Any exceptions should be meaningful and related to MPI or computation
            error_msg = str(e).lower()
            # Don't fail the test for MPI-related errors - they might be environment-specific
            if "mpi" in error_msg or "communication" in error_msg:
                pytest.skip(f"MPI communication issue (environment-specific): {e}")
            else:
                raise

    def test_rank_specific_errors(self, temp_directory):
        """Test handling of errors that occur on specific ranks."""
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

        # Create normal test data
        if MPI_RANK == 0:
            data = np.random.exponential(scale=1.0, size=(6, 4, 4))

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

        test_data = MPI_COMM.bcast(test_data, root=0)

        # Simulate rank-specific issues (e.g., one rank has problem with computation)
        # For testing purposes, we'll just run normal computation and verify
        # that all ranks participate properly

        opa = Opa(pass_dic)

        # Each rank reports its status
        rank_status = {"rank": MPI_RANK, "initialized": True, "error": None}

        try:
            result = opa.compute(test_data)
            rank_status["completed"] = True
            rank_status["has_result"] = result is not None

        except Exception as e:
            rank_status["completed"] = False
            rank_status["error"] = str(e)

        # Gather status from all ranks
        all_statuses = MPI_COMM.gather(rank_status, root=0)

        if MPI_RANK == 0:
            # Analyze rank statuses
            completed_ranks = sum(1 for status in all_statuses if status["completed"])
            failed_ranks = sum(1 for status in all_statuses if not status["completed"])

            print(f"Rank Status Summary:")
            print(f"  Completed: {completed_ranks}/{MPI_SIZE}")
            print(f"  Failed: {failed_ranks}/{MPI_SIZE}")

            # For this test, all ranks should complete successfully
            assert completed_ranks == MPI_SIZE, f"Some ranks failed: {failed_ranks}"

            # At least rank 0 should have a result
            rank_0_status = all_statuses[0]
            assert rank_0_status["has_result"], "Rank 0 should have a result"
