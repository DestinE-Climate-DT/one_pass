"""One-Pass percentile library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray
from one_pass.mpi import MPI, MPI_COMM, MPI_RANK, MPI_get_indices


if TYPE_CHECKING:
    from one_pass.opa import Opa


def finalize_percentile(opa: "Opa") -> NDArray:
    """Converts digest functions into percentiles and reshapes
    the attribute percentile_cum back into the shape of the original
    grid. It will use the attribute percentile_list if specified,
    to return the requested percentiles, if not it will default to
    percentiles 0-99

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

    tdigests = opa.statistics.cumulative["tdigest"]
    size_digests = len(tdigests)

    n_percentiles = len(percentile_list)
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
