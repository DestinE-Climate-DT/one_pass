"""Module to handle initialization of the TDigest object,
based on the current strategy used.

Currently the supported strategies are:
    - Serial
    - MPI (Message Parsing Interface)
"""

from typing import TYPE_CHECKING

from crick import TDigest
from one_pass.mpi import MPI_LAUNCHED, MPI_get_indices


if TYPE_CHECKING:
    from one_pass.opa import Opa

def initialise_mpi(opa: "Opa"):
    """Initialize TDigest objects for MPI execution.

    Arguments
    ----------
    opa : Opa
        The OPA object which holds the DataSetInfo with the spacial size
    size : int
        Total number of spatial points to distribute

    Returns
    -------
    list[TDigest or None]
        A sequence where local indices contain TDigest objects and
        remote indices contain None
    """
    size = opa.data_set_info.size_data_source_tail
    start_index, end_index = MPI_get_indices(size)

    # Read the compression from request. If it wasn't passed, set to 1
    compression = opa.request.compression
    if compression is None:
        compression = 1

    # Handle case where this rank gets no work
    if start_index is None or end_index is None:
        return [None] * size

    # Local sequence contains TDigest objects only in the relevant indices
    # for the MPI worker, and None everywhere else. None is immutable, so can be copied
    # with '*' operation. However, TDigest needs to be initialized separately
    local_sequence = (
        [None] * start_index
        + [TDigest(compression=compression) for _ in range(start_index, end_index)]
        + [None] * (size - end_index)
    )
    return local_sequence


def initialise_no_mpi(opa: "Opa"):
    """Initialize a list of empty TDigest objects for non-MPI execution.

    The number of elements matches the number of grid points for the incoming
    data array, and is taken from opa.data_set_info

    Arguments
    ----------
    opa : Opa
        The OPA object which holds the DataSetInfo with the spacial size

    Returns
    -------
    list[TDigest]
        A sequence of empty TDigest objects with specified compression
    """

    size = opa.data_set_info.size_data_source_tail

    # Read the compression from request. If it wasn't passed, set to 1
    compression = opa.request.compression
    if compression is None:
        compression = 1

    opa.logger.info("Initialising digests")
    return [TDigest(compression=compression) for _ in range(size)]


def initialise(opa: "Opa"):
    """Initialize a list of empty TDigest objects or, if using MPI, a list
    with the local TDigest objects and None everywhere else.

    The number of elements matches the number of grid points for the incoming
    data array, and is taken from opa.data_set_info

    Arguments
    ----------
    opa : Opa
        The OPA object which holds the DataSetInfo with the spacial size

    Returns
    -------
    list[TDigest or None]
        A sequence of TDigest objects. In MPI mode, only local indices
        contain TDigest objects, others are None.
    """
    size = opa.data_set_info.spacial_size

    if MPI_LAUNCHED:
        return initialise_mpi(opa)
    return initialise_no_mpi(opa)
