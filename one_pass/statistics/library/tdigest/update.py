"""Module to handle updating of the TDigest object,
based on the current strategy used.

Currenty the supported strategies are:
    - Serial
    - Multiprocessing (MP)
    - MPI (Message Parsing Interface)
"""

from concurrent.futures import as_completed, ProcessPoolExecutor
import logging
from typing import TYPE_CHECKING

import numpy as np
import tqdm
import xarray as xr

from one_pass.options import FrequencyKey
from one_pass.mpi import MPI_get_indices

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


def update_mp(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Parallel version: Updates the digest for each grid point. Uses multiprocessing."""

    def get_input_and_output_structure(
        opa: "Opa", data_source: xr.DataArray, weight: int
    ):
        size = opa.data_set_info.size_data_source_tail
        tdigests = opa.statistics.cumulative["tdigest"]

        # Prepare input for parallel processing
        if weight == 1:
            data_source_values = np.reshape(data_source.values, size)
            inputs = [(j, data_source_values[j], tdigests[j]) for j in range(size)]
            outputs = [None] * size

        else:
            data_source_values = data_source.values.reshape((weight, -1))
            inputs = [(j, data_source_values[:, j], tdigests[j]) for j in size]
            outputs = [None] * size

        return inputs, outputs

    def update_single_digest(j, value, digest):
        """Helper function to update a single digest."""
        digest.update(value)
        return j, digest

    inputs, outputs = get_input_and_output_structure(opa, data_source, weight)
    with_pbar = opa.logger.isEnabledFor(logging.DEBUG)

    with ProcessPoolExecutor(max_workers=opa.workers) as executor:
        futures = {
            executor.submit(update_single_digest, *args): args[0] for args in inputs
        }
        for future in tqdm.tqdm(
            as_completed(futures),
            total=len(outputs),
            desc="Updating digests",
            disable=not with_pbar,
        ):
            j, updated_digest = future.result()
            outputs[j] = updated_digest

    # Replace old digests with updated ones
    opa.statistics.cumulative["tdigest"] = outputs


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
    Uses either serial or multiprocessing depending on opa.workers.
    """

    if opa.workers > 1:
        update_mp(opa, data_source, weight)

    else:
        update_serial(opa, data_source, weight)

    # Time counting is handled centrally in update_statistics
    # to avoid double-counting here.
