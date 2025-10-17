"""Test MPI performance and scaling characteristics."""

import pytest
import time
import numpy as np
import xarray as xr
import pandas as pd
from tempfile import TemporaryDirectory

from one_pass.opa import Opa
from one_pass.mpi import MPI_RANK, MPI_SIZE, MPI_COMM


@pytest.mark.mpi(min_size=2)
class TestMPIPerformance:
    """Test MPI performance and scaling."""

    @pytest.fixture
    def temp_directory(self):
        with TemporaryDirectory() as tmpdir:
            yield tmpdir

    def create_performance_test_data(self, time_steps, lat_size, lon_size):
        """Create larger datasets for performance testing."""
        # Create data on rank 0 and broadcast
        if MPI_RANK == 0:
            np.random.seed(42)  # Reproducible data
            data = np.random.exponential(
                scale=2.0, size=(time_steps, lat_size, lon_size)
            )

            dataset = xr.Dataset(
                {
                    "pr": xr.DataArray(
                        data,
                        dims=["time", "lat", "lon"],
                        coords={
                            "time": pd.date_range(
                                "2021-01-01", periods=time_steps, freq="H"
                            ),
                            "lat": np.linspace(-45, 45, lat_size),
                            "lon": np.linspace(-90, 90, lon_size),
                        },
                    )
                }
            )
        else:
            dataset = None

        return MPI_COMM.bcast(dataset, root=0)

    def measure_computation_time(self, pass_dic, test_data, chunk_size=6):
        """Measure the time taken for a complete computation."""
        opa = Opa(pass_dic)

        # Synchronize all processes before starting
        MPI_COMM.Barrier()
        start_time = time.time()

        result = None
        time_steps = test_data.dims["time"]
        for i in range(0, time_steps, chunk_size):
            end_idx = min(i + chunk_size, time_steps)
            chunk = test_data.isel(time=slice(i, end_idx))
            result = opa.compute(chunk)

        # Synchronize all processes before ending
        MPI_COMM.Barrier()
        end_time = time.time()

        computation_time = end_time - start_time
        return computation_time, result

    def test_mpi_scaling_efficiency(self, temp_directory):
        """Test that MPI provides reasonable scaling with more processes."""
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
            "bins": 20,
        }

        # Create moderately large dataset
        test_data = self.create_performance_test_data(
            time_steps=24, lat_size=50, lon_size=50  # 2500 spatial points
        )

        # Measure computation time
        computation_time, result = self.measure_computation_time(pass_dic, test_data)

        # Gather timing results from all ranks
        all_times = MPI_COMM.gather(computation_time, root=0)

        if MPI_RANK == 0:
            max_time = max(all_times)
            min_time = min(all_times)
            avg_time = sum(all_times) / len(all_times)

            # Basic performance checks
            assert max_time < 120.0, f"Computation took too long: {max_time}s"

            # Check that computation was reasonably balanced
            time_imbalance = (max_time - min_time) / max_time if max_time > 0 else 0
            assert time_imbalance < 0.5, f"High time imbalance: {time_imbalance:.2f}"

            # Verify we got a valid result
            assert result is not None, "Should have a valid result"
            assert "pr" in result, "Result should contain pr variable"

            print(f"MPI Performance - Processes: {MPI_SIZE}")
            print(f"  Max time: {max_time:.3f}s")
            print(f"  Min time: {min_time:.3f}s")
            print(f"  Avg time: {avg_time:.3f}s")
            print(f"  Time imbalance: {time_imbalance:.3f}")

    def test_mpi_memory_efficiency(self, temp_directory):
        """Test memory usage patterns with MPI."""
        import psutil
        import os

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
            "percentile_list": [5, 25, 50, 75, 95],
        }

        # Measure memory before computation
        process = psutil.Process(os.getpid())
        memory_before = process.memory_info().rss / 1024 / 1024  # MB

        # Create test data
        test_data = self.create_performance_test_data(
            time_steps=36, lat_size=30, lon_size=30
        )

        # Run computation
        computation_time, result = self.measure_computation_time(pass_dic, test_data)

        # Measure memory after computation
        memory_after = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = memory_after - memory_before

        # Gather memory stats
        all_memory_increases = MPI_COMM.gather(memory_increase, root=0)

        if MPI_RANK == 0:
            max_memory_increase = max(all_memory_increases)
            avg_memory_increase = sum(all_memory_increases) / len(all_memory_increases)

            # Memory increase should be reasonable (less than 1GB per process)
            assert (
                max_memory_increase < 1024
            ), f"Excessive memory use: {max_memory_increase:.1f}MB"

            print(f"Memory Usage - Processes: {MPI_SIZE}")
            print(f"  Max memory increase: {max_memory_increase:.1f}MB")
            print(f"  Avg memory increase: {avg_memory_increase:.1f}MB")

    def test_mpi_communication_overhead(self, temp_directory):
        """Test and measure MPI communication overhead."""
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

        # Test with different data sizes to measure communication scaling
        data_sizes = [
            (12, 10, 10),  # Small: 1200 points
            (24, 20, 20),  # Medium: 9600 points
            (24, 30, 30),  # Large: 21600 points
        ]

        timing_results = []

        for time_steps, lat_size, lon_size in data_sizes:
            test_data = self.create_performance_test_data(
                time_steps, lat_size, lon_size
            )

            # Measure time for this data size
            computation_time, result = self.measure_computation_time(
                pass_dic, test_data, chunk_size=6
            )

            # Calculate data size
            total_points = time_steps * lat_size * lon_size

            timing_results.append((total_points, computation_time))

            if MPI_RANK == 0 and result is not None:
                # Verify we got reasonable results
                hist_values = result["pr"].values
                assert (
                    np.sum(hist_values) > 0
                ), f"No data in histogram for size {total_points}"

        # Analyze scaling on rank 0
        if MPI_RANK == 0:
            print(f"Communication Overhead Analysis - {MPI_SIZE} processes:")
            for i, (points, time_taken) in enumerate(timing_results):
                throughput = points / time_taken if time_taken > 0 else 0
                print(
                    f"  Size {points:6d} points: {time_taken:.3f}s ({throughput:.0f} points/s)"
                )

            # Basic scaling check: larger problems shouldn't be disproportionately slower
            if len(timing_results) >= 2:
                small_throughput = timing_results[0][0] / timing_results[0][1]
                large_throughput = timing_results[-1][0] / timing_results[-1][1]

                # Throughput shouldn't drop too much with larger data
                throughput_ratio = (
                    large_throughput / small_throughput if small_throughput > 0 else 1
                )
                assert (
                    throughput_ratio > 0.1
                ), f"Poor scaling: throughput ratio {throughput_ratio:.3f}"

    def test_mpi_load_balancing(self, temp_directory):
        """Test load balancing across MPI processes."""
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
            "bins": 12,
        }

        # Create data that might challenge load balancing
        test_data = self.create_performance_test_data(
            time_steps=18,
            lat_size=37,  # Prime number to challenge even distribution
            lon_size=41,  # Another prime number
        )

        # Measure detailed timing for each rank
        rank_start_time = time.time()

        opa = Opa(pass_dic)
        result = None

        # Track processing time per chunk
        chunk_times = []

        time_steps = test_data.dims["time"]
        chunk_size = 6

        for i in range(0, time_steps, chunk_size):
            chunk_start = time.time()

            end_idx = min(i + chunk_size, time_steps)
            chunk = test_data.isel(time=slice(i, end_idx))
            result = opa.compute(chunk)

            chunk_end = time.time()
            chunk_times.append(chunk_end - chunk_start)

        rank_end_time = time.time()
        total_rank_time = rank_end_time - rank_start_time

        # Gather load balancing stats
        load_stats = {
            "rank": MPI_RANK,
            "total_time": total_rank_time,
            "chunk_times": chunk_times,
            "mean_chunk_time": np.mean(chunk_times),
            "std_chunk_time": np.std(chunk_times),
        }

        all_load_stats = MPI_COMM.gather(load_stats, root=0)

        if MPI_RANK == 0:
            # Analyze load balancing
            total_times = [stats["total_time"] for stats in all_load_stats]
            mean_chunk_times = [stats["mean_chunk_time"] for stats in all_load_stats]

            max_total_time = max(total_times)
            min_total_time = min(total_times)
            avg_total_time = sum(total_times) / len(total_times)

            # Load balance metrics
            load_imbalance = (
                (max_total_time - min_total_time) / max_total_time
                if max_total_time > 0
                else 0
            )

            print(f"Load Balancing Analysis - {MPI_SIZE} processes:")
            print(f"  Max total time: {max_total_time:.3f}s")
            print(f"  Min total time: {min_total_time:.3f}s")
            print(f"  Avg total time: {avg_total_time:.3f}s")
            print(f"  Load imbalance: {load_imbalance:.3f}")

            # Check load balancing quality
            assert load_imbalance < 0.3, f"Poor load balancing: {load_imbalance:.3f}"

            # Verify all ranks completed successfully
            for i, stats in enumerate(all_load_stats):
                assert len(stats["chunk_times"]) > 0, f"Rank {i} processed no chunks"
                assert stats["total_time"] > 0, f"Rank {i} reported zero time"

    def test_mpi_weak_scaling(self, temp_directory):
        """Test weak scaling: constant work per process."""
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

        # Scale data size with number of processes (weak scaling test)
        # Each process should handle roughly the same amount of work
        work_per_process = 500  # spatial points per process
        total_spatial_points = MPI_SIZE * work_per_process

        # Choose dimensions that give approximately the right number of points
        lat_size = int(np.sqrt(total_spatial_points))
        lon_size = total_spatial_points // lat_size

        test_data = self.create_performance_test_data(
            time_steps=12, lat_size=lat_size, lon_size=lon_size
        )

        # Measure computation time
        computation_time, result = self.measure_computation_time(pass_dic, test_data)

        # Gather results
        all_times = MPI_COMM.gather(computation_time, root=0)
        work_info = MPI_COMM.gather(
            {
                "spatial_points": lat_size * lon_size,
                "work_per_process": (lat_size * lon_size) // MPI_SIZE,
            },
            root=0,
        )

        if MPI_RANK == 0:
            max_time = max(all_times)

            print(f"Weak Scaling Test - {MPI_SIZE} processes:")
            print(f"  Total spatial points: {lat_size * lon_size}")
            print(f"  Points per process: ~{work_per_process}")
            print(f"  Max computation time: {max_time:.3f}s")

            # In ideal weak scaling, time should remain roughly constant
            # as we add more processes with proportionally more work
            # For this test, we just verify reasonable performance
            assert max_time < 60.0, f"Weak scaling test too slow: {max_time:.3f}s"

            # Verify result quality
            assert result is not None, "Should have valid result"
            if "pr" in result:
                hist_values = result["pr"].values
                assert np.sum(hist_values) > 0, "Should have non-zero histogram"
