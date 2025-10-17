"""Utilities for saving OPA computation results to NetCDF files.

This module provides functions for filename generation, attribute management,
and NetCDF output of computed statistics.
"""

from .create_file_names import create_file_name
from .remove_attributes import remove_attributes_continuous
from .save_final import save_output_nc
