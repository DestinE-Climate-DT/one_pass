"""Support module to figure out whether Message Parsing Interface (MPI) is used,
as well as the RANK/SIZE of the MPI launch
"""

import os
from typing import Tuple, Union

__all__ = [
    "MPI",
    "MPI_COMM",
    "MPI_LAUNCHED",
    "MPI_RANK",
    "MPI_SIZE",
    "MPI_get_indices",
    "check_mp_mpi_conflict",
]

# Check if we are under an MPI environment
MPI_LAUNCHED = any(
    var in os.environ
    for var in [
        "OMPI_COMM_WORLD_SIZE",  # OpenMPI
        "PMI_SIZE",  # MPICH / IntelMPI
        "MPI_LOCALNRANKS",  # Some others
    ]
)


if MPI_LAUNCHED:
    try:
        from mpi4py import MPI
    except ImportError as exc:
        raise ImportError(
            "Optional dependency mpi4py must be installed if running with mpirun/mpiexec."
        ) from exc
    MPI_COMM = MPI.COMM_WORLD
    MPI_RANK = MPI_COMM.Get_rank()
    MPI_SIZE = MPI_COMM.Get_size()


else:
    MPI = None
    MPI_COMM = None
    MPI_RANK = None
    MPI_SIZE = None


def MPI_get_indices(  # pylint: disable=C0103
    size: int,
) -> Union[Tuple[int, int], Tuple[None, None]]:
    """In a given MPI context, return rank local indices for a given global array size.

    Parameters
    ----------
    size : int
        Total size of the array to be distributed

    Returns
    -------
    tuple
        (start_index, end_index) for this rank, or (None, None) if not MPI
        or if this rank gets no work

    Raises
    ------
    ValueError
        If size is negative
    RuntimeError
        If MPI is launched but rank/size not available
    """
    if size < 0:
        raise ValueError(f"Size must be non-negative, got {size}")

    if not MPI_LAUNCHED:
        return None, None

    if MPI_SIZE is None or MPI_RANK is None:
        raise RuntimeError("MPI appears to be launched but rank/size not available")

    if size == 0:
        return None, None

    chunk_size = size // MPI_SIZE
    start_index = MPI_RANK * chunk_size

    # Handle case where some ranks get no work (size < MPI_SIZE)
    if start_index >= size:
        return None, None

    if MPI_RANK == MPI_SIZE - 1:
        # Last process handles the remainder
        end_index = size
    else:
        # Ensure end_index doesn't exceed size
        end_index = min((MPI_RANK + 1) * chunk_size, size)

    return start_index, end_index


def check_mp_mpi_conflict(n_workers: int):
    """Check for conflicts between MP and MPI modes.

    Parameters
    ----------
    n_workers : int
        Number of MP workers requested

    Raises
    ------
    RuntimeError
        If both MPI and MP (workers > 1) are enabled simultaneously
    """
    if MPI_LAUNCHED and n_workers > 1:
        raise RuntimeError(
            "Cannot use both MPI and multiprocessing (workers > 1) simultaneously. "
            f"MPI is active with {MPI_SIZE} ranks, but {n_workers} MP workers were requested. "
            "Please use either MPI (mpirun/mpiexec) OR multiprocessing (workers > 1), not both."
        )
