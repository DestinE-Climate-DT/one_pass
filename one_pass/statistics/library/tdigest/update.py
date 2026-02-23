"""Module to handle updating of the TDigest object, based on the current strategy.

Currenty the supported strategies are:
    - Serial
    - Multiprocessing (MP) with shared memory (via temporary NetCDF files)
    - MPI (Message Parsing Interface)
"""

import os
import pickle
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

from crick import TDigest
from joblib import Parallel, delayed
import numpy as np
import xarray as xr

from one_pass.mpi import MPI_get_indices
from one_pass.mp import MP_get_indices, MP_set_context

if TYPE_CHECKING:
    from one_pass.opa import Opa


def update_serial(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Computes one pass tdigest with weight corresponding to the number
    of timesteps being added.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    tdigests : list of TDigest
        Updated cumulative tdigest objects stored in opa.statistics.cumulative["tdigest"]
    """

    size = opa.data_set_info.size_data_source_tail
    assert size is not None

    tdigests = opa.statistics.cumulative["tdigest"]

    if weight == 1:
        # TODO: Use np.flatten instead
        data_source_values = np.reshape(data_source.values, size)
        for j, tdigest in enumerate(tdigests):
            tdigest.update(data_source_values[j])

    # TODO: Check whether np.reshape((-1, weight)) instead would make code reusable,
    #       as data_source_values would be a scalar (for weight == 1),
    #       or a 1D array (otherwise)
    else:
        data_source_values = np.reshape(data_source.values, (weight, -1))
        assert size is not None
        for j, tdigest in enumerate(tdigests):
            tdigest.update(data_source_values[:, j])

    opa.statistics.cumulative["tdigest"] = tdigests

    return tdigests


def _get_shm_data_path(opa: "Opa") -> Path:
    """Get path for shared memory data file."""
    filename = (
        f"opa_shm_{opa.request.variable}_{opa.request.stat.value}_{os.getpid()}.npy"
    )

    # Try /dev/shm first (Linux), fall back to temp directory
    shm_dir = Path("/dev/shm")
    if shm_dir.exists() and shm_dir.is_dir():
        return shm_dir / filename
    return Path(tempfile.gettempdir()) / filename


def _write_data_to_shm(opa: "Opa", data_source: xr.DataArray, weight: int) -> Path:
    """Write raw NumPy data to a shared-memory-backed file.

    Stores the underlying NumPy array with shape ``(weight, size)`` directly,
    avoiding unnecessary copying or reshaping. Workers will memory-map and slice
    this array to read only their portion.
    """
    size = opa.data_set_info.size_data_source_tail
    assert size is not None

    shm_path = _get_shm_data_path(opa)

    # Reshape incoming data to (weight, size) - this is a view, no copy
    data_2d = np.reshape(data_source.values, (weight, size))

    # Write NumPy array directly to shared memory
    # Use np.save for simplicity - workers will memory-map this file
    with open(shm_path, "wb") as f:
        np.save(f, data_2d, allow_pickle=False)

    return shm_path


def _worker_update_from_shm(
    worker_id: int,
    n_workers: int,
    size: int,
    weight: int,
    shm_path: str,
    checkpoint_file: str,
    compression: int,
    use_checkpoints: bool,
) -> Tuple[int, int, int]:
    """Worker function that updates TDigests from shared memory.

    Runs in a separate process. Loads checkpoint (if enabled and exists),
    reads data from the shared NumPy array, updates TDigests, and saves
    the checkpoint.
    """
    # Set MP context for this worker
    MP_set_context(n_workers, worker_id)

    # Get local indices
    start_index, end_index = MP_get_indices(size, n_workers, worker_id)
    local_size = end_index - start_index

    if local_size == 0:
        return worker_id, start_index, end_index

    # Load checkpoint if it exists and checkpointing is enabled
    if use_checkpoints and os.path.exists(checkpoint_file):
        with open(checkpoint_file, "rb") as f:
            local_tdigests = pickle.load(f)
        # Verify loaded tdigests have the correct size
        if len(local_tdigests) != local_size:
            raise RuntimeError(
                f"Loaded checkpoint has wrong size: expected {local_size}, got {len(local_tdigests)}. "
                f"Checkpoint file: {checkpoint_file}"
            )
    else:
        # Initialize new TDigest objects for this worker's range
        local_tdigests = [TDigest(compression=compression) for _ in range(local_size)]

    # Read data from shared memory using memory-mapping to avoid loading entire array
    # Array shape: (weight, size) where size is total number of grid points
    # We use mmap_mode='r' so the array is not loaded into memory; only the slice
    # we access will be paged in by the OS.
    data_mmap = np.load(shm_path, mmap_mode="r", allow_pickle=False)

    # Extract this worker's slice: data[:, start_index:end_index]
    # This creates a view that will be paged in on demand, not copied into memory
    # worker_data shape: (weight, local_size)
    worker_data = data_mmap[:, start_index:end_index]

    # Update TDigests with this worker's data
    for j in range(local_size):
        if weight == 1:
            local_tdigests[j].update(worker_data[0, j])
        else:
            local_tdigests[j].update(worker_data[:, j])

    # Save checkpoint if enabled
    if use_checkpoints:
        with open(checkpoint_file, "wb") as f:
            pickle.dump(local_tdigests, f, protocol=pickle.HIGHEST_PROTOCOL)

    return worker_id, start_index, end_index


def update_mp(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Parallel version: Updates the digest for each grid point using multiprocessing.

    Uses shared memory (NetCDF file in /dev/shm) to avoid serialization bottleneck.
    Each worker maintains its own TDigest objects, saved to separate checkpoint files
    (one per worker, similar to MPI).

    Note: When workers > 1, checkpoint files are ALWAYS created (even if checkpoint=False)
    to allow gathering tdigests at finalization. If checkpoint_filepath is not set,
    it will be set to a temporary directory (/dev/shm or system temp directory).
    """
    size = opa.data_set_info.size_data_source_tail
    n_workers = opa.workers
    compression = opa.request.compression if opa.request.compression is not None else 1

    # When workers > 1, we MUST always create checkpoint files for finalization
    # even if user set checkpoint=False. This is because tdigests exist only in worker processes.
    # Set checkpoint_filepath to temporary directory if not already set
    if opa.request.checkpoint_filepath is None:
        # Use same logic as _get_shm_data_path: try /dev/shm first, then temp directory
        shm_dir = Path("/dev/shm")
        if shm_dir.exists() and shm_dir.is_dir():
            opa.request.checkpoint_filepath = str(shm_dir)
        else:
            opa.request.checkpoint_filepath = tempfile.gettempdir()

    # Always use checkpoints when workers > 1 (needed for finalization)
    use_checkpoints = True

    # Write data to shared memory
    shm_path = _write_data_to_shm(opa, data_source, weight)

    try:
        # Prepare worker arguments
        worker_args = []
        for worker_id in range(n_workers):
            checkpoint_file = opa.request.checkpoint_file_for_worker(
                worker_id, n_workers
            )

            worker_args.append(
                (
                    worker_id,
                    n_workers,
                    size,
                    weight,
                    str(shm_path),
                    checkpoint_file,
                    compression,
                    use_checkpoints,
                )
            )

        # Execute workers in parallel
        results = Parallel(n_jobs=n_workers)(
            delayed(_worker_update_from_shm)(*args) for args in worker_args
        )

        opa.logger.debug(f"MP update completed. {len(results)} workers processed data.")

        # Note: We don't update opa.statistics.cumulative["tdigest"] here
        # because the data is distributed across workers (and optionally checkpoint files).
        # It will be gathered during finalization.

    finally:
        # Clean up shared memory file
        if shm_path.exists():
            shm_path.unlink()


def update_mpi_no_weight(opa: "Opa", data_source: xr.DataArray):
    """Parallel version: Updates the digest for each grid point. Uses MPI."""

    size = opa.data_set_info.size_data_source_tail
    data_source_values = np.reshape(data_source.values, size)
    tdigests = opa.statistics.cumulative["tdigest"]

    start_index, end_index = MPI_get_indices(size)

    for j in range(start_index, end_index):
        tdigests[j].update(data_source_values[j])

    opa.statistics.cumulative["tdigest"] = tdigests
    return tdigests


# TODO: Review reshape to merge with function above (as outlined in update_serial)
def update_mpi_large_weight(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Parallel version: Updates the digest for each grid point. Uses MPI."""

    size = opa.data_set_info.size_data_source_tail
    data_source_values = np.reshape(data_source.values, (weight, -1))
    tdigests = opa.statistics.cumulative["tdigest"]

    start_index, end_index = MPI_get_indices(size)

    for j in range(start_index, end_index):
        tdigests[j].update(data_source_values[:, j])

    opa.statistics.cumulative["tdigest"] = tdigests
    return tdigests


def update_mpi(opa: "Opa", data_source: xr.DataArray, weight: int = 1):
    """MPI version: Updates the digest for each grid point in parallel.
    If the statistic is not bias correction, it will also update the
    count with the weight. For bias correction, this is done in the
    daily means calculation.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Note
    ----
    Updates opa.statistics.cumulative["tdigest"] with the new data.
    Each MPI rank processes its local portion of the tdigests.
    """

    if weight == 1:
        update_mpi_no_weight(opa, data_source)
    else:
        update_mpi_large_weight(opa, data_source, weight)


def update_no_mpi(opa: "Opa", data_source: xr.DataArray, weight: int = 1):
    """Non-MPI version: Updates the digest for each grid point.
    If the statistic is not bias correction, it will also update the
    count with the weight. For bias correction, this is done in the
    daily means calculation.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Note
    ----
    Updates opa.statistics.cumulative["tdigest"] with the new data.
    Uses serial or multiprocessing with shared memory depending on configuration.

    MP mode (opa.workers > 1) always uses shared memory (temporary NetCDF files)
    to avoid serialization bottlenecks. Checkpointing is optional and controlled
    by opa.request.checkpoint.
    """

    if opa.workers > 1:
        update_mp(opa, data_source, weight)
    else:
        update_serial(opa, data_source, weight)

    # Time counting is handled centrally in update_statistics
    # to avoid double-counting here.
