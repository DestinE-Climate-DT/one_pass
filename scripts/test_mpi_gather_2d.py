#!/usr/bin/env python3
"""MPI test script demonstrating 2D Gatherv (like histogram data) and NetCDF writing.

This script creates a 2D array on each MPI rank (mimicking histogram bins x spatial points),
gathers all arrays on rank 0, and writes the result to a NetCDF file.

Usage:
    mpirun -np 4 python test_mpi_gather_2d.py
"""

import numpy as np
from mpi4py import MPI
import netCDF4 as nc

# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

print(f"Rank {rank}/{size}: Starting")

# Simulate a scenario similar to histogram processing
# Total spatial points to process
total_spatial_points = 20
n_bins = 10  # e.g., histogram bins

# Each rank gets a portion of spatial points
points_per_rank = total_spatial_points // size
remainder = total_spatial_points % size

# Distribute points (give remainder to last rank)
if rank < size - 1:
    local_width = points_per_rank
else:
    local_width = points_per_rank + remainder

# Create local 2D array (bins x local_spatial_points)
# Fill with values that identify the rank
local_array = np.ones((n_bins, local_width), dtype=np.int32) * (rank + 1) * 100
# Add bin index to make it more interesting
local_array += np.arange(n_bins)[:, np.newaxis]

print(f"Rank {rank}: Processing {local_width} spatial points")
print(f"Rank {rank}: Local array shape: {local_array.shape}")
print(f"Rank {rank}: Local array sample:\n{local_array[:3, :min(3, local_width)]}")

# Gather the widths from all ranks
local_widths = comm.allgather(local_width)
print(f"Rank {rank}: Local widths from all ranks: {local_widths}")

# Prepare for Gatherv
if rank == 0:
    # Create receive buffer with final shape
    global_array = np.empty((n_bins, total_spatial_points), dtype=np.int32)

    # Calculate receive counts (number of elements each rank sends)
    # Each rank sends n_bins * local_width elements
    recvcounts = [n_bins * w for w in local_widths]

    # Compute displacements (start index of each chunk in flattened array)
    displs = np.insert(np.cumsum(recvcounts[:-1]), 0, 0)

    print(f"\nRank 0: Total spatial points: {total_spatial_points}")
    print(f"Rank 0: Receive counts: {recvcounts}")
    print(f"Rank 0: Displacements: {displs}")
else:
    global_array = None
    recvcounts = None
    displs = None

# Flatten local arrays for sending
local_flat = local_array.flatten()

# Perform Gatherv
comm.Gatherv(
    sendbuf=local_flat,
    recvbuf=(
        global_array.flatten() if rank == 0 else None,
        recvcounts,
        displs,
        MPI.INT,
    ),
    root=0,
)

print(f"Rank {rank}: Gatherv completed")

# Rank 0 writes the result to NetCDF
if rank == 0:
    print(f"\nRank 0: Gathered array shape: {global_array.shape}")
    print(f"Rank 0: Gathered array sample:\n{global_array[:3, :6]}")

    # Create NetCDF file
    output_file = "mpitest_2d.nc"
    with nc.Dataset(output_file, "w", format="NETCDF4") as dataset:
        # Create dimensions
        dataset.createDimension("bins", n_bins)
        dataset.createDimension("space", total_spatial_points)

        # Create variable
        test_var = dataset.createVariable("test", "i4", ("bins", "space"))

        # Add some attributes
        test_var.units = "counts"
        test_var.long_name = "Test 2D array gathered from MPI ranks"
        test_var.description = f"2D array gathered from {size} MPI ranks"

        # Write data
        test_var[:, :] = global_array

        # Add global attributes
        dataset.title = "MPI 2D Gatherv Test (Histogram-like)"
        dataset.n_ranks = size
        dataset.n_bins = n_bins
        dataset.total_spatial_points = total_spatial_points

    print(f"\nRank 0: Successfully wrote NetCDF file: {output_file}")
    print(f"Rank 0: Variable 'test' has shape: {global_array.shape}")

    # Verify the data is correct
    expected_sum = sum((r + 1) * 100 * n_bins * w for r, w in enumerate(local_widths))
    expected_sum += (
        n_bins * (n_bins - 1) // 2 * total_spatial_points
    )  # contribution from bin indices
    actual_sum = global_array.sum()
    print(
        f"Rank 0: Data verification - Expected sum: {expected_sum}, Actual sum: {actual_sum}"
    )
    if expected_sum == actual_sum:
        print("Rank 0: ✓ Data integrity check passed!")
    else:
        print("Rank 0: ✗ Data integrity check failed!")

# Synchronize all ranks before finishing
comm.Barrier()
print(f"Rank {rank}: Finished")
