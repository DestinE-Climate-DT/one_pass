"""Module to handle initialization and update of the TDigest object,
based on the current strategy used.

Currenty the supported strategies are:
    - Serial
    - Multiprocessing (MP)
    - MPI (Message Parsing Interface)
"""

from typing import TYPE_CHECKING

from crick import TDigest
from joblib import Parallel, delayed
import numpy as np
import xarray as xr

from one_pass.mpi import MPI_LAUNCHED, MPI_get_indices


if TYPE_CHECKING:
    from one_pass.opa import Opa


def initialise(opa: "Opa"):
    """Initalise a list of empty TDigest objects or, if using MPI, a list
    with the local TDigest objects and None everywhere else.

    The number of elements matches the number of grid points for the incoming
    data array, and is taken from opa.data_set_info

    Arguments
    ----------
    opa : Opa
        The OPA object which holds the DataSetInfo with the spacial size

    Returns
    -------
    list[TDigest]
        A sequence of empty TDigest objects with compression=1

    Note
    ----
    This function is deprecated. Use the library.tdigest.initialise instead,
    which properly initializes cumulative["tdigest"].
    """

    size = opa.data_set_info.size_data_source_tail
    compression = opa.request.compression

    if compression is None:
        compression = 1

    if not MPI_LAUNCHED:
        opa.logger.info("Initialising digests")
        return [TDigest(compression=compression) for _ in range(size)]

    start_index, end_index = MPI_get_indices(size)

    # Local sequence contains TDigest objects only in the relevant indices
    # for the MPI worker, and None everywhere else. None is immutable, so can be copied
    # with '*' operation. However, TDigest needs to be initialized separately
    local_sequence = (
        [None] * start_index
        + [TDigest(compression=compression) for _ in range(start_index, end_index)]
        + [None] * (size - end_index)
    )
    return local_sequence


def update_mp(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Parallel version: Updates the digest for each grid point. Uses multiprocessing.

    Note
    ----
    This function is deprecated. Use library.tdigest.update instead,
    which properly updates cumulative["tdigest"].
    """

    def get_input_and_output_structure(
        opa: "Opa", data_source: xr.DataArray, weight: int
    ):
        size = opa.data_set_info.size_data_source_tail
        digests = opa.statistics.cumulative["tdigest"]

        # Prepare input for parallel processing
        if weight == 1:
            data_source_values = np.reshape(data_source.values, size)
            inputs = [(j, data_source_values[j], digests[j]) for j in range(size)]
            outputs = [None] * size

        else:
            data_source_values = data_source.values.reshape((weight, -1))
            inputs = [(j, data_source_values[:, j], digests[j]) for j in range(size)]
            outputs = [None] * size

        return inputs, outputs

    def update_single_digest(j, value, digest):
        """Helper function to update a single digest."""
        digest.update(value)
        return j, digest

    inputs, outputs = get_input_and_output_structure(opa, data_source, weight)

    results = Parallel(n_jobs=opa.workers)(
        delayed(update_single_digest)(j, value, digest) for j, value, digest in inputs
    )

    for j, updated_digest in results:
        outputs[j] = updated_digest

    # Replace old digests with updated ones
    opa.statistics.cumulative["tdigest"] = outputs


def update_mpi(opa: "Opa", data_source: xr.DataArray):
    """Parallel version: Updates the digest for each grid point. Uses MPI.

    Note
    ----
    This function is deprecated. Use library.tdigest.update instead,
    which properly updates cumulative["tdigest"].
    """

    size = opa.data_set_info.size_data_source_tail
    data_source_values = np.reshape(data_source.values, size)
    tdigests = opa.statistics.cumulative["tdigest"]
    start_index, end_index = MPI_get_indices(size)

    for j in range(start_index, end_index):
        tdigests[j].update(data_source_values[j])

    opa.statistics.cumulative["tdigest"] = tdigests


# TODO: Review reshape to merge with function above (as outlined in update_serial)
def update_mpi_large_weight(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Parallel version: Updates the digest for each grid point. Uses MPI.

    Note
    ----
    This function is deprecated. Use library.tdigest.update instead,
    which properly updates cumulative["tdigest"].
    """

    size = opa.data_set_info.size_data_source_tail
    data_source_values = np.reshape(data_source.values, (weight, -1))
    tdigests = opa.statistics.cumulative["tdigest"]
    start_index, end_index = MPI_get_indices(size)

    for j in range(start_index, end_index):
        tdigests[j].update(data_source_values[:, j])

    opa.statistics.cumulative["tdigest"] = tdigests


def update_serial(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Update tdigests in a serial fashion. Equivalent to original `update` function.

    Note
    ----
    This function is deprecated. Use library.tdigest.update instead,
    which properly updates cumulative["tdigest"].
    """
    size = opa.data_set_info.size_data_source_tail
    assert size is not None
    tdigests = opa.statistics.cumulative["tdigest"]

    if weight == 1:
        # TODO: Use np.flatten instead
        data_source_values = np.reshape(data_source.values, size)
        for j in range(size):
            tdigests[j].update(data_source_values[j])

    # TODO: Check whether np.reshape((-1, weight)) instead would make code reusable,
    #       as data_source_values would be a scalar (for weight == 1),
    #       or a 1D array (otherwise)
    else:
        data_source_values = np.reshape(data_source.values, (weight, -1))
        assert size is not None
        for j in range(size):
            tdigests[j].update(data_source_values[:, j])

    opa.statistics.cumulative["tdigest"] = tdigests


def update(opa: "Opa", data_source: xr.DataArray, weight: int = 1):
    """Sequential loop that updates the digest for each grid point.
    If the statistic is not bias correction, it will also update the
    count with the weight. For bias correction, this is done in the
    daily means calculation.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa.statistics.cumulative["tdigest"] : each digest for each grid cell is updated with
            the new data
    opa.time.count : updated with weight unless the statistic is bias-corr
            in which case that is updated in daily means

    Note
    ----
    This function is deprecated. Use library.tdigest.update instead,
    which is the preferred implementation.
    """

    if MPI_LAUNCHED:
        if weight == 1:
            update_mpi(opa, data_source)
        else:
            update_mpi_large_weight(opa, data_source, weight)

    elif opa.workers > 1:
        update_mp(opa, data_source, weight)

    else:
        update_serial(opa, data_source, weight)

    # Time counting is handled centrally in update_statistics
    # to avoid double-counting here.
