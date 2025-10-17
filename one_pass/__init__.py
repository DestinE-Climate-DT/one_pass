"""
OnePass Algorithm (OPA) - One-pass streaming statistics for large climate datasets.

This package provides efficient computation of various statistics over temporal
dimensions of xarray data without requiring the entire dataset in memory.
"""

import importlib.metadata

from one_pass.opa import Opa
from one_pass.request import Request, RequestError, load_yaml
from one_pass.options import StatKey, FrequencyKey
from one_pass.mpi import MPI_LAUNCHED, MPI_RANK, MPI_SIZE

try:
    # Try to get the version using importlib.metadata
    __version__ = importlib.metadata.version("one_pass")
except importlib.metadata.PackageNotFoundError:
    # If the package is not installed, set a default version
    __version__ = "unknown"

__all__ = [
    "Opa",
    "Request",
    "RequestError",
    "load_yaml",
    "StatKey",
    "FrequencyKey",
    "MPI_LAUNCHED",
    "MPI_RANK",
    "MPI_SIZE",
]
