"""Module for removing checkpoint files after computation completes."""

import os
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from one_pass.opa import Opa


def _remove_mp_checkpoints(opa: "Opa"):
    # Remove all worker checkpoint files
    opa.logger.debug("removing MP worker checkpoints")
    n_workers = opa.workers
    for worker_id in range(n_workers):
        checkpoint_file = opa.request.checkpoint_file_for_worker(worker_id, n_workers)
        if os.path.isfile(checkpoint_file):
            os.remove(checkpoint_file)


def remove_checkpoints(opa: "Opa"):
    """Remove checkpoint files after successful computation.

    This function cleans up checkpoint files when computation is complete,
    unless explicitly configured to keep them.

    Parameters
    ----------
    opa : Opa
        OPA instance containing checkpoint configuration and file path.

    Notes
    -----
    The checkpoint file is only removed if:
    - opa.keep_checkpoints is False
    - Checkpointing was enabled in the request
    - The checkpoint file exists on disk

    For MP mode (workers > 1), removes all worker checkpoint files and main checkpoint file.
    For MPI mode, each rank removes its own checkpoint file.
    For serial mode, removes the single checkpoint file.
    """
    if opa.keep_checkpoints:
        return
    if not opa.request.checkpoint:
        return

    # Check if we're in MP mode (workers > 1, not MPI)
    mp_launched = opa.workers > 1
    if mp_launched:
        _remove_mp_checkpoints(opa)

    # Remove single checkpoint file (serial, MP and MPI)
    if os.path.isfile(opa.request.checkpoint_file):
        opa.logger.debug("removing checkpoint")
        os.remove(opa.request.checkpoint_file)
