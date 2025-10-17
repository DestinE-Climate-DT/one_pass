"""Time management utilities for OPA computations.

This module provides time tracking, timestamp validation, and data splitting
functionality for managing temporal aspects of streaming statistics.
"""

from .opa_time import OpaTime
from .timestamps import validate, split

__all__ = [
    "OpaTime",
    "split",
    "validate",
]
