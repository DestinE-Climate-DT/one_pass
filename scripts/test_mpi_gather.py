#!/usr/bin/env python3
"""Simple MPI test script demonstrating Gatherv and NetCDF writing.

This script creates a small array on each MPI rank, gathers all arrays
on rank 0, and writes the result to a NetCDF file.

Usage:
    mpirun -np 4 python test_mpi_gather.py
"""

import numpy as np
from mpi4py import MPI
import netCDF4 as nc

# Initialize MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

print(f"Rank {rank}/{size}: Starting")

# Each rank creates a small array with values based on its rank
# For simplicity, each rank creates an array of size 5
local_size = 5
local_array = np.arange(local_size, dtype=np.float64) + (rank * 10)

print(f"Rank {rank}: Created local array: {local_array}")

# Gather the sizes from all ranks (in this case, all the same)
local_sizes = comm.allgather(local_size)
print(f"Rank {rank}: Local sizes from all ranks: {local_sizes}")

# Prepare for Gatherv
if rank == 0:
    # Calculate total size and create receive buffer
    total_size = sum(local_sizes)
    global_array = np.empty(total_size, dtype=np.float64)

    # Calculate receive counts and displacements
    recvcounts = local_sizes
    displs = np.insert(np.cumsum(recvcounts[:-1]), 0, 0)

    print(f"Rank 0: Total size: {total_size}")
    print(f"Rank 0: Receive counts: {recvcounts}")
    print(f"Rank 0: Displacements: {displs}")
else:
    global_array = None
    recvcounts = None
    displs = None

# Perform Gatherv
comm.Gatherv(
    sendbuf=local_array,
    recvbuf=(global_array, recvcounts, displs, MPI.DOUBLE) if rank == 0 else None,
    root=0,
)

print(f"Rank {rank}: Gatherv completed")

# Rank 0 writes the result to NetCDF
if rank == 0:
    print(f"Rank 0: Gathered array: {global_array}")

    # Create NetCDF file
    output_file = "mpitest.nc"
    with nc.Dataset(output_file, "w", format="NETCDF4") as dataset:
        # Create dimension
        dataset.createDimension("x", len(global_array))

        # Create variable
        test_var = dataset.createVariable("test", "f8", ("x",))

        # Add some attributes
        test_var.units = "arbitrary"
        test_var.long_name = "Test array gathered from MPI ranks"
        test_var.description = f"Array gathered from {size} MPI ranks"

        # Write data
        test_var[:] = global_array

        # Add global attributes
        dataset.title = "MPI Gatherv Test"
        dataset.n_ranks = size

    print(f"Rank 0: Successfully wrote NetCDF file: {output_file}")
    print(f"Rank 0: Variable 'test' contains: {global_array}")

# Synchronize all ranks before finishing
comm.Barrier()
print(f"Rank {rank}: Finished")
