"""Statistics computation and management for OPA.

This module provides the core statistical computation functionality including
update algorithms, finalization routines, and raw data handling.
"""

from .raw_data import check_raw
from .finalize import create_data_set, update_attributes
from .update import update_statistics
from .opa_statistics import OpaStatistics
from .tdigest import initialise as initialise_tdigest, update as update_tdigest

__all__ = [
    "check_raw",
    "create_data_set",
    "update_attributes",
    "update_statistics",
    "OpaStatistics",
    "initialise_tdigest",
    "update_tdigest",
]
