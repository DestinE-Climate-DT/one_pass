"""One-Pass histogram library."""

# pylint: disable=W0613

import logging
from typing import Tuple, TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray
from tqdm import tqdm
from one_pass.mpi import MPI, MPI_COMM, MPI_RANK, MPI_get_indices

if TYPE_CHECKING:
    from one_pass.opa import Opa


# NOTE: Could be returned to the user as-is (no tdigest)
def finalize_histogram(opa: "Opa") -> Tuple[NDArray, NDArray]:
    """Converts tdigests into histograms. It first creates the
    two attributes, bin_edges and bin_counts which it then fills
    with the output of the data from the t-digest histogram func.
    It then re-shapes these attributes back into the original grid.
    (https://github.com/dask/crick/blob/main/crick/tdigest.pyx)

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

    tdigests = opa.statistics.cumulative["tdigest"]
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
