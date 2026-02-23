"""Support module for Multiprocessing (MP) utilities.

This module provides utilities for managing multiprocessing workers,
similar to the MPI module but for shared-memory parallelism.

Module-level variables track MP state:
- MP_WORKERS: Number of workers (set by Opa initialization)
- MP_WORKER_ID: Current worker ID (set within worker processes)
"""

# pylint: disable=C0103,W0603

from typing import Tuple

__all__ = [
    "MP_WORKERS",
    "MP_WORKER_ID",
    "MP_set_context",
    "MP_get_indices",
]

# Package-wide MP state (similar to MPI_LAUNCHED, MPI_RANK, MPI_SIZE)
MP_LAUNCHED: bool = False
MP_WORKERS: int = 1  # Default to serial mode
MP_WORKER_ID: int = 0  # Worker ID within worker processes


def MP_set_context(n_workers: int, worker_id: int = 0):
    """Set the MP context for the current process.

    This is called:
    - In the main process with n_workers from Opa.workers
    - In each worker process with its specific worker_id

    Parameters
    ----------
    n_workers : int
        Total number of workers
    worker_id : int, default 0
        Worker ID for this process (0 in main process)
    """
    global MP_LAUNCHED, MP_WORKERS, MP_WORKER_ID
    MP_LAUNCHED = n_workers > 1
    MP_WORKERS = n_workers
    MP_WORKER_ID = worker_id


def MP_get_indices(
    size: int, n_workers: int = None, worker_id: int = None
) -> Tuple[int, int]:
    """Calculate local indices for a given worker in a multiprocessing context.

    Similar to MPI_get_indices, but for multiprocessing workers.
    If n_workers/worker_id are not provided, uses the package-wide MP_WORKERS/MP_WORKER_ID.

    Parameters
    ----------
    size : int
        Total size of the array to be distributed
    n_workers : int, optional
        Total number of workers (uses MP_WORKERS if None)
    worker_id : int, optional
        ID of this worker (uses MP_WORKER_ID if None)

    Returns
    -------
    tuple
        (start_index, end_index) for this worker

    Raises
    ------
    ValueError
        If size is negative or worker_id is invalid
    """
    if n_workers is None:
        n_workers = MP_WORKERS
    if worker_id is None:
        worker_id = MP_WORKER_ID

    if size < 0:
        raise ValueError(f"Size must be non-negative, got {size}")

    if worker_id < 0 or worker_id >= n_workers:
        raise ValueError(f"Worker ID must be in [0, {n_workers-1}], got {worker_id}")

    if size == 0:
        return 0, 0

    chunk_size = size // n_workers
    start_index = worker_id * chunk_size

    # Handle case where worker ID exceeds available work
    if start_index >= size:
        return size, size

    if worker_id == n_workers - 1:
        # Last worker handles the remainder
        end_index = size
    else:
        # Ensure end_index doesn't exceed size
        end_index = min((worker_id + 1) * chunk_size, size)

    return start_index, end_index
