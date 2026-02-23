"""One-Pass percentile library."""

# pylint: disable=W0613

import os
import pickle
import tempfile
from pathlib import Path
from typing import List, Tuple, TYPE_CHECKING

from joblib import Parallel, delayed
import numpy as np
from numpy.typing import NDArray

from one_pass.mp import MP_get_indices
from one_pass.mpi import MPI, MPI_COMM, MPI_RANK, MPI_get_indices


if TYPE_CHECKING:
    from one_pass.opa import Opa


def _get_memmap_path(label: str) -> Path:
    """Return a path on /dev/shm (Linux) or the system temp directory."""
    filename = f"opa_mmap_{label}_{os.getpid()}.dat"
    shm_dir = Path("/dev/shm")
    if shm_dir.exists() and shm_dir.is_dir():
        return shm_dir / filename
    return Path(tempfile.gettempdir()) / filename


def _worker_compute_percentiles_mmap(
    checkpoint_file: str,
    percentile_list: List[float],
    mmap_path: str,
    mmap_shape: Tuple[int, ...],
    start_index: int,
) -> None:
    """Worker function that computes percentiles and writes to a memory-mapped file.

    Runs in a separate process. Loads TDigests from the checkpoint file,
    computes percentiles, and writes results directly into the memory-mapped
    array at the correct column offset.

    Parameters
    ----------
    checkpoint_file : str
        Path to the worker's checkpoint file containing TDigests.
    percentile_list : list[float]
        List of percentiles to compute.
    mmap_path : str
        File path of the memory-mapped array for percentiles.
    mmap_shape : tuple[int, ...]
        Shape of the full percentiles array (n_percentiles, size_data_source).
    start_index : int
        Column offset in the shared array where this worker's results begin.
    """
    percentiles = np.memmap(mmap_path, dtype=np.float64, mode="r+", shape=mmap_shape)

    with open(checkpoint_file, "rb") as f:
        local_tdigests = pickle.load(f)

    for j, tdigest in enumerate(local_tdigests):
        percentiles[:, start_index + j] = tdigest.quantile(percentile_list)

    percentiles.flush()


def _finalize_percentile_mp(
    opa: "Opa", percentile_list: list, n_percentiles: int, size_data_source: int
) -> NDArray:
    """Compute percentiles from worker checkpoint files in MP mode.

    Creates a memory-mapped array for percentiles, then spawns parallel
    workers that load their TDigests and write quantile results directly into
    the mapped file — no per-worker arrays are returned or assembled.

    Parameters
    ----------
    opa : Opa
        OPA instance
    percentile_list : list
        List of percentiles to compute
    n_percentiles : int
        Number of percentiles
    size_data_source : int
        Total size of the data source

    Returns
    -------
    NDArray
        Percentiles array with shape (n_percentiles, size_data_source)
    """
    checkpoint_dir = opa.request.checkpoint_filepath
    if checkpoint_dir is None:
        raise RuntimeError(
            "checkpoint_filepath is None. This should have been set by update_mp "
            "when workers > 1. Please ensure update_mp was called before finalization."
        )

    n_workers = opa.workers

    mmap_shape = (n_percentiles, size_data_source)

    mmap_path = _get_memmap_path("percentiles")

    # Create memory-mapped file (w+ creates and zero-fills)
    mmap_arr = np.memmap(mmap_path, dtype=np.float64, mode="w+", shape=mmap_shape)
    mmap_arr.flush()
    del mmap_arr  # Release main-process handle so workers can open independently

    try:
        # Build worker arguments, skipping workers with no work
        worker_args = []
        for worker_id in range(n_workers):
            start_index, end_index = MP_get_indices(
                size_data_source, n_workers, worker_id
            )
            if end_index - start_index == 0:
                continue

            checkpoint_file = opa.request.checkpoint_file_for_worker(
                worker_id, n_workers
            )
            if not os.path.exists(checkpoint_file):
                raise FileNotFoundError(
                    f"Missing worker file for worker {worker_id}. "
                    f"Expected at: {checkpoint_file}"
                )

            worker_args.append(
                (
                    checkpoint_file,
                    percentile_list,
                    str(mmap_path),
                    mmap_shape,
                    start_index,
                )
            )

        # Execute percentile computation in parallel
        Parallel(n_jobs=n_workers)(
            delayed(_worker_compute_percentiles_mmap)(*args) for args in worker_args
        )

        # Read results back from memory-mapped file into a regular numpy array
        percentiles = np.array(
            np.memmap(mmap_path, dtype=np.float64, mode="r", shape=mmap_shape)
        )
    finally:
        mmap_path.unlink(missing_ok=True)

    return percentiles


def finalize_percentile(opa: "Opa") -> NDArray:
    """Converts digest functions into percentiles and reshapes
    the attribute percentile_cum back into the shape of the original
    grid. It will use the attribute percentile_list if specified,
    to return the requested percentiles, if not it will default to
    percentiles 0-99

    In MP mode (opa.workers > 1), this function loads TDigests from worker
    checkpoint files, computes percentiles per worker as NDArrays, and then
    efficiently concatenates them, avoiding TDigest serialization.

    Arguments
    ---------
    opa : Opa class

    Returns
    ---------
    np.ndarray
        array with same shape as original data but with an extra
        dimension corresponding to the number of percentiles requested.
    """
    percentile_list = opa.request.percentile_list
    size_data_source = opa.data_set_info.size_data_source_tail
    shape_data_source = opa.data_set_info.shape_data_source_tail

    assert percentile_list is not None
    assert size_data_source is not None
    assert shape_data_source is not None

    n_percentiles = len(percentile_list)

    # Handle MP mode (workers > 1) - compute percentiles per worker as NDArrays
    if opa.workers > 1:
        percentiles = _finalize_percentile_mp(
            opa, percentile_list, n_percentiles, size_data_source
        )
    else:
        # Serial mode: use existing logic with tdigests from cumulative
        tdigests = opa.statistics.cumulative["tdigest"]
        if tdigests is None:
            raise RuntimeError(
                "tdigests is None. This should have been populated during initialization. "
                f"checkpoint_filepath={opa.request.checkpoint_filepath}, workers={opa.workers}"
            )
        size_digests = len(tdigests)

        percentiles = np.empty((n_percentiles, size_digests))

        for j, tdigest in enumerate(tdigests):
            percentiles[:, j] = tdigest.quantile(percentile_list)

    final_size = [n_percentiles, *shape_data_source[1:]]

    # With the percentiles we add another dimension for the percentiles
    percentiles = np.reshape(percentiles, final_size)

    # Add axis for time
    return np.expand_dims(percentiles, axis=0)


def finalize_percentile_mpi(opa: "Opa") -> NDArray:  # pylint: disable=R0914
    """MPI version of finalize_percentile. Distributes tdigest processing across ranks
    and gathers results on rank 0.

    Converts digest functions into percentiles and reshapes
    the attribute percentile_cum back into the shape of the original
    grid. It will use the attribute percentile_list if specified,
    to return the requested percentiles, if not it will default to
    percentiles 0-99

    Arguments
    ---------
    opa : Opa class

    Returns
    ---------
    np.ndarray or None
        array with same shape as original data but with an extra
        dimension corresponding to the number of percentiles requested.
        Returns None on non-root ranks.
    """

    percentile_list = opa.request.percentile_list
    size_data_source = opa.data_set_info.size_data_source_tail
    shape_data_source = opa.data_set_info.shape_data_source_tail

    if percentile_list is None:
        raise ValueError("percentile_list must be set before MPI finalization")
    if size_data_source is None or shape_data_source is None:
        raise ValueError(
            "Data source size and shape must be set before MPI finalization"
        )

    # Get MPI indices for this rank
    start_index, end_index = MPI_get_indices(size_data_source)

    n_percentiles = len(percentile_list)

    # Handle case where this rank has no work
    if start_index is None or end_index is None:
        local_width = 0
        local_percentiles = np.empty((n_percentiles, 0))
    else:
        local_width = end_index - start_index

        # Get local tdigests for this rank
        tdigests = opa.statistics.cumulative["tdigest"]
        local_tdigests = tdigests[start_index:end_index]

        # Local percentiles array
        local_percentiles = np.empty((n_percentiles, local_width))

        # Process local tdigests
        for j, tdigest in enumerate(local_tdigests):
            if tdigest is None:
                raise RuntimeError(
                    f"TDigest at index {start_index + j} is None - initialization failed"
                )
            local_percentiles[:, j] = tdigest.quantile(percentile_list)

    # Gather sizes from all ranks
    local_widths = MPI_COMM.allgather(local_width)

    # Prepare receive buffers on rank 0
    if MPI_RANK == 0:
        # Final shape (without time dimension)
        final_size = [n_percentiles, *shape_data_source[1:]]
        global_percentiles = np.empty(final_size)

        # Calculate number of elements each rank will send
        recvcounts = [n_percentiles * w for w in local_widths]

        # Compute displacements (start index of each chunk in the flattened array)
        displs = np.insert(np.cumsum(recvcounts[:-1]), 0, 0)

    else:
        global_percentiles = None
        recvcounts = None
        displs = None

    # Gather percentiles
    # NOTE: Must use ravel() not flatten() to get a view, not a copy
    MPI_COMM.Gatherv(
        sendbuf=local_percentiles.ravel(),
        recvbuf=(
            global_percentiles.ravel() if MPI_RANK == 0 else None,
            recvcounts,
            displs,
            MPI.DOUBLE,
        ),
        root=0,
    )

    if MPI_RANK == 0:
        # The MPI Gatherv operation reconstructs data in rank order
        # Since MPI_get_indices distributes work sequentially across the flattened spatial grid,
        # the gathered data is already in the correct spatial order
        # We can use reshape directly like the serial version

        # Reshape to final dimensions (no copy - just changes view)
        final_size = [n_percentiles, *shape_data_source[1:]]
        percentiles = np.reshape(global_percentiles, final_size)

        # Add time dimension
        return np.expand_dims(percentiles, axis=0)

    return None
