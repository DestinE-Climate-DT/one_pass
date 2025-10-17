"""Module for removing checkpoint files after computation completes."""

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from one_pass.opa import Opa


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
    """
    if opa.keep_checkpoints:
        return
    if not opa.request.checkpoint:
        return
    if os.path.isfile(opa.request.checkpoint_file):
        opa.logger.debug("removing checkpoint")
        os.remove(opa.request.checkpoint_file)
