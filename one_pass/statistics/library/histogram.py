"""One-Pass histogram library."""

# pylint: disable=W0613

import logging
import os
import pickle
import tempfile
from pathlib import Path
from typing import Tuple, TYPE_CHECKING

from joblib import Parallel, delayed
import numpy as np
from numpy.typing import NDArray
from tqdm import tqdm

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


def _worker_compute_histograms_mmap(
    checkpoint_file: str,
    bins: int,
    hist_range: tuple,
    counts_path: str,
    edges_path: str,
    counts_shape: Tuple[int, ...],
    edges_shape: Tuple[int, ...],
    start_index: int,
) -> None:
    """Worker function that computes histograms and writes to memory-mapped files.

    Runs in a separate process. Loads TDigests from the checkpoint file,
    computes histograms, and writes results directly into the memory-mapped
    arrays at the correct column offset.

    Parameters
    ----------
    checkpoint_file : str
        Path to the worker's checkpoint file containing TDigests.
    bins : int
        Number of bins for histogram.
    hist_range : tuple
        Range for histogram bins.
    counts_path : str
        File path of the memory-mapped array for histogram counts.
    edges_path : str
        File path of the memory-mapped array for histogram edges.
    counts_shape : tuple[int, ...]
        Shape of the full counts array (bins, size_data_source).
    edges_shape : tuple[int, ...]
        Shape of the full edges array (bins + 1, size_data_source).
    start_index : int
        Column offset in the shared arrays where this worker's results begin.
    """
    hist_counts = np.memmap(counts_path, dtype=np.int32, mode="r+", shape=counts_shape)
    hist_edges = np.memmap(edges_path, dtype=np.float64, mode="r+", shape=edges_shape)

    with open(checkpoint_file, "rb") as f:
        local_tdigests = pickle.load(f)

    for j, tdigest in enumerate(local_tdigests):
        histogram, bin_edges = tdigest.histogram(bins=bins, range=hist_range)
        hist_counts[:, start_index + j] = histogram
        hist_edges[:, start_index + j] = bin_edges

    hist_counts.flush()
    hist_edges.flush()


def _finalize_histogram_mp(
    opa: "Opa", bins: int, size_data_source: int
) -> Tuple[NDArray, NDArray]:
    """Compute histograms from worker checkpoint files in MP mode.

    Creates memory-mapped arrays for counts and edges, then spawns parallel
    workers that load their TDigests and write histogram results directly into
    the mapped files — no per-worker arrays are returned or assembled.

    Parameters
    ----------
    opa : Opa
        OPA instance
    bins : int
        Number of bins for histogram
    size_data_source : int
        Total size of the data source

    Returns
    -------
    tuple[NDArray, NDArray]
        (hist_counts, hist_edges) arrays with shape (bins, size_data_source)
        and (bins + 1, size_data_source) respectively
    """
    from one_pass.mp import MP_get_indices

    checkpoint_dir = opa.request.checkpoint_filepath
    if checkpoint_dir is None:
        raise RuntimeError(
            "checkpoint_filepath is None. This should have been set by update_mp "
            "when workers > 1. Please ensure update_mp was called before finalization."
        )

    n_workers = opa.workers
    hist_range = opa.request.range

    counts_shape = (bins, size_data_source)
    edges_shape = (bins + 1, size_data_source)

    counts_path = _get_memmap_path("hist_counts")
    edges_path = _get_memmap_path("hist_edges")

    # Create memory-mapped files (w+ creates and zero-fills)
    mmap_counts = np.memmap(counts_path, dtype=np.int32, mode="w+", shape=counts_shape)
    mmap_edges = np.memmap(edges_path, dtype=np.float64, mode="w+", shape=edges_shape)
    mmap_counts.flush()
    mmap_edges.flush()
    # Release main-process handles so workers can open independently
    del mmap_counts, mmap_edges

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
                    bins,
                    hist_range,
                    str(counts_path),
                    str(edges_path),
                    counts_shape,
                    edges_shape,
                    start_index,
                )
            )

        # Execute histogram computation in parallel
        Parallel(n_jobs=n_workers)(
            delayed(_worker_compute_histograms_mmap)(*args) for args in worker_args
        )

        # Read results back from memory-mapped files into regular numpy arrays
        hist_counts = np.array(
            np.memmap(counts_path, dtype=np.int32, mode="r", shape=counts_shape)
        )
        hist_edges = np.array(
            np.memmap(edges_path, dtype=np.float64, mode="r", shape=edges_shape)
        )
    finally:
        counts_path.unlink(missing_ok=True)
        edges_path.unlink(missing_ok=True)

    return hist_counts, hist_edges


# NOTE: Could be returned to the user as-is (no tdigest)
def finalize_histogram(opa: "Opa") -> Tuple[NDArray, NDArray]:
    """Converts tdigests into histograms. It first creates the
    two attributes, bin_edges and bin_counts which it then fills
    with the output of the data from the t-digest histogram func.
    It then re-shapes these attributes back into the original grid.
    (https://github.com/dask/crick/blob/main/crick/tdigest.pyx)

    In MP mode (opa.workers > 1), this function loads TDigests from worker
    checkpoint files, computes histograms per worker as NDArrays, and then
    efficiently concatenates them, avoiding TDigest serialization.

    Arguments
    ----------
    opa : Opa class
    opa.request.bins : int or array_like, optional
            If ``bins`` is an int, it defines the number of equal
            width bins in the given range. If ``bins`` is an array_like,
            the values define the edges of the bins (rightmost edge
            inclusive), allowing for non-uniform bin widths. The default
            is 10.

    Returns
    ---------
    opa.statistics.histogram_cum: numpy array with the same shape as the
            original grid (+ one extra dimension of length opa.request.bins)
            that contains the cumulative count for every bin.
    opa.statistics.histogram_bin_edges_cum: numpy array with the same shape
            as theoriginal grid (+ one extra dimension of length opa.request
            .bins + 1)that contains the edges for all the bins. This needs to be
            one longer in the extra dimension.
    """
    bins = opa.request.bins
    size_data_source = opa.data_set_info.size_data_source_tail
    shape_data_source = opa.data_set_info.shape_data_source_tail
    assert size_data_source is not None
    assert shape_data_source is not None
    # TODO: Set default when parsing request
    if bins is None:
        bins = 10

    # Handle MP mode (workers > 1) - compute histograms per worker as NDArrays
    if opa.workers > 1:
        hist_counts, hist_edges = _finalize_histogram_mp(opa, bins, size_data_source)
    else:
        # Serial mode: use existing logic with tdigests from cumulative
        tdigests = opa.statistics.cumulative["tdigest"]
        if tdigests is None:
            raise RuntimeError(
                "tdigests is None. This should have been populated during initialization. "
                f"checkpoint_filepath={opa.request.checkpoint_filepath}, workers={opa.workers}"
            )
        size_digests = len(tdigests)

        hist_counts = np.empty((bins, size_digests), dtype=np.int32)
        hist_edges = np.empty(((bins + 1), size_digests), dtype=np.float64)
        with_pbar = opa.logger.isEnabledFor(logging.DEBUG)
        for j, tdigest in tqdm(
            enumerate(tdigests),
            desc="Extracting histogram from digests",
            disable=not with_pbar,
        ):
            histogram, bin_edges = tdigest.histogram(
                bins=bins,
                range=opa.request.range,
            )
            hist_counts[:, j] = histogram
            hist_edges[:, j] = bin_edges

    # Add axis for time
    final_size_counts = [bins, *shape_data_source[1:]]
    final_size_edges = [bins + 1, *shape_data_source[1:]]

    hist_counts = np.reshape(hist_counts, final_size_counts)
    hist_edges = np.reshape(hist_edges, final_size_edges)

    hist_counts = np.expand_dims(hist_counts, axis=0)
    hist_edges = np.expand_dims(hist_edges, axis=0)
    return (hist_counts, hist_edges)


def finalize_histogram_mpi(opa: "Opa") -> Tuple[NDArray, NDArray]:
    """MPI version of finalize_histogram. Distributes tdigest processing across ranks
    and gathers results on rank 0.

    Returns
    -------
    tuple or (None, None)
        (histogram_counts, bin_edges) on rank 0, (None, None) on other ranks
    """
    bins = opa.request.bins
    size_data_source = opa.data_set_info.size_data_source_tail
    shape_data_source = opa.data_set_info.shape_data_source_tail

    if size_data_source is None or shape_data_source is None:
        raise ValueError(
            "Data source size and shape must be set before MPI finalization"
        )

    if bins is None:
        bins = 10  # Fallback

    # Get MPI indices for this rank
    start_index, end_index = MPI_get_indices(size_data_source)

    # Build local histograms
    if start_index is None or end_index is None:
        local_width = 0
        hist_counts = np.empty((bins, 0), dtype=np.int32)
        hist_edges = np.empty((bins + 1, 0), dtype=np.float64)
    else:
        local_width = end_index - start_index
        hist_counts = np.empty((bins, local_width), dtype=np.int32)
        hist_edges = np.empty((bins + 1, local_width), dtype=np.float64)

        # Process local tdigests
        tdigests = opa.statistics.cumulative["tdigest"]
        local_tdigests = tdigests[start_index:end_index]
        for j, tdigest in enumerate(local_tdigests):
            if tdigest is None:
                raise RuntimeError(
                    f"TDigest at index {start_index + j} is None "
                    "- initialization failed"
                )
            histogram, bin_edges = tdigest.histogram(
                bins=bins,
                range=opa.request.range,
            )
            hist_counts[:, j] = histogram
            hist_edges[:, j] = bin_edges

    # Gather widths and index ranges across all ranks
    local_widths = MPI_COMM.allgather(local_width)
    start_end = (start_index or 0, end_index or 0)
    all_start_end = MPI_COMM.allgather(start_end)

    # Prepare receive buffers and metadata on root
    if MPI_RANK == 0:
        total_spatial_points = size_data_source
        # Flattened receive buffers
        global_hist_counts = np.empty(bins * total_spatial_points, dtype=np.int32)
        global_hist_edges = np.empty(
            (bins + 1) * total_spatial_points, dtype=np.float64
        )

        recvcounts_hist = [bins * w for w in local_widths]
        recvcounts_edges = [(bins + 1) * w for w in local_widths]
        displs_hist = np.insert(np.cumsum(recvcounts_hist[:-1]), 0, 0)
        displs_edges = np.insert(np.cumsum(recvcounts_edges[:-1]), 0, 0)
    else:
        global_hist_counts = None
        global_hist_edges = None
        recvcounts_hist = None
        recvcounts_edges = None
        displs_hist = None
        displs_edges = None

    # Use explicit MPI datatypes for safety
    mpi_dtype_counts = MPI.INT32_T
    mpi_dtype_edges = MPI.DOUBLE

    # Gatherv histogram counts
    MPI_COMM.Gatherv(
        sendbuf=hist_counts.ravel(),
        recvbuf=(
            global_hist_counts if MPI_RANK == 0 else None,
            recvcounts_hist,
            displs_hist,
            mpi_dtype_counts,
        ),
        root=0,
    )

    # Gatherv histogram edges
    MPI_COMM.Gatherv(
        sendbuf=hist_edges.ravel(),
        recvbuf=(
            global_hist_edges if MPI_RANK == 0 else None,
            recvcounts_edges,
            displs_edges,
            mpi_dtype_edges,
        ),
        root=0,
    )

    if MPI_RANK == 0:
        # Reconstruct flattened spatial layout
        flat_counts = np.empty((bins, size_data_source), dtype=np.int32)
        flat_edges = np.empty((bins + 1, size_data_source), dtype=np.float64)

        offset_counts = 0
        offset_edges = 0
        for (start, end), w in zip(all_start_end, local_widths):
            if w == 0:
                continue
            flat_counts[:, start:end] = global_hist_counts[
                offset_counts : offset_counts + bins * w
            ].reshape(bins, w)
            flat_edges[:, start:end] = global_hist_edges[
                offset_edges : offset_edges + (bins + 1) * w
            ].reshape(bins + 1, w)
            offset_counts += bins * w
            offset_edges += (bins + 1) * w

        # Final shape check & reshape
        final_shape_counts = [bins, *shape_data_source[1:]]
        final_shape_edges = [bins + 1, *shape_data_source[1:]]
        assert flat_counts.size == np.prod(final_shape_counts)
        assert flat_edges.size == np.prod(final_shape_edges)

        hist_counts = flat_counts.reshape(final_shape_counts)
        hist_edges = flat_edges.reshape(final_shape_edges)

        # Add time dimension
        hist_counts = np.expand_dims(hist_counts, axis=0)
        hist_edges = np.expand_dims(hist_edges, axis=0)
        return hist_counts, hist_edges

    return None, None
