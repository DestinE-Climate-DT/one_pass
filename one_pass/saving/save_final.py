"""Functions for saving finalized statistics to NetCDF files."""

import os
from typing import TYPE_CHECKING, Optional
from pathlib import Path

import numpy as np

from one_pass.options import StatKey
from one_pass.mpi import MPI_LAUNCHED, MPI_RANK

if TYPE_CHECKING:
    from one_pass.opa import Opa


def save_output_nc(opa: "Opa", directory: Optional[str | Path] = None):
    """Save computed statistics to NetCDF file.

    Saves the finalized statistics Dataset to a NetCDF file with appropriate
    naming conventions. Handles both single and dual outputs (for histograms).

    Parameters
    ----------
    opa : Opa
        OPA instance containing the computed statistics and save configuration.
    directory : str or Path, optional
        Directory to save the NetCDF file(s). If None, uses the directory
        specified in `opa.request.save_filepath`.

    Notes
    -----
    For histogram statistics, saves two files:
    - One for bin counts (*_bin_counts.nc)
    - One for bin edges (*_bin_edges.nc)

    For other statistics, saves a single file with the statistic name.

    In MPI mode, only rank 0 performs the actual file writing to avoid
    conflicts and duplicate files.
    """

    if MPI_LAUNCHED and MPI_RANK != 0:
        return

    stat = opa.request.stat
    stat_freq = opa.request.stat_freq
    convert_to_float32 = opa.as_float32
    base_filename = opa.data_set_info.filename

    filename_stem = (
        f"{base_filename}_{opa.request.variable}_"
        f"timestep_{opa.request.time_step}_"
        f"{stat_freq.value}_{stat.value}"
    )

    if stat == StatKey.HISTOGRAM:
        suffixes = ("_bin_counts.nc", "_bin_edges.nc")
        for dm, suffix in zip(opa.statistics.get_final(), suffixes):
            if convert_to_float32:
                for var_name in dm.data_vars:
                    if dm[var_name].dtype == np.float64:
                        dm[var_name] = dm[var_name].astype(np.float32)
            path = os.path.join(directory, filename_stem + suffix)
            dm.to_netcdf(path=path, mode="w")

    else:
        path = os.path.join(directory, filename_stem + ".nc")
        dm = opa.statistics.get_final()
        if convert_to_float32:
            for var_name in dm.data_vars:
                if dm[var_name].dtype == np.float64:
                    dm[var_name] = dm[var_name].astype(np.float32)
        dm.to_netcdf(path=path, mode="w")
        dm.close()
