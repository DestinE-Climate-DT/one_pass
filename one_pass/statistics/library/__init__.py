"""
One-Pass statistics library

This submodule contains functionality to update and compute different
statistics in a One-Pass fashion, with the aid of the Opa class.

Each of the modules in one_pass.statistics.library corresponds to either
a supported statistic (mean.py, std.py) or an intermediate process (tdigest.py)
in order to achiveve a statistic.

The function names and signatures follow a specific pattern, which
seeks to provide clear meaning and structure throughout this library.


update_<stat>
=============

    These functions provide the update functionality of one step.
    They take in the Opa instance and the chunked data, and perform an update.
    Note that they may update the opa.statistics.cumulative object as a side effect,
    which is typically dependent on the concrete statistic.
    However, the time update, common to all streamed statistics happens outside
    of these functions.

    Arguments
    ---------
        opa : Opa
            Opa instance
        data_source : xr.DataArray
            chunked data. It consists of a xr.DataArray with the
            time, longitude, latitude
            Calling data_source.values necessarily returns a 3-D np.ndarray
        weight: int
            Size of the temporal dimension of the incoming data_source.
    Returns
    -------
        np.ndarray
            aggregated data


finalize_<stat>
===============

    These functions provide the functionality to compute arrays after the statistic is
    wrapped-up, i.e. after the aggregation period has finalized. This turns intermediate
    representations in opa.stastics.cumulative into final data arrays.

    Arguments
    ---------
        opa : Opa
            Opa instance
    Returns
    -------
        np.ndarray or tuple of np.ndarrays
            Finalized data. It is a tuple in the case of histogram, which has bins and edges.


transform_<stat>
==============

    These functions perform transformation on finalized arrays. They serve as helpers
    while converting the finalized numpy arrays into the xr.DataSet objects that the
    OPA outputs.
"""

from .histogram import finalize_histogram, finalize_histogram_mpi
from .iams import finalize_iams, update_iams
from .mean import update_mean
from .min import update_min
from .max import update_max
from .percentile import (
    finalize_percentile,
    finalize_percentile_mpi,
)
from .std import finalize_std
from .sum import update_sum
from .tdigest import initialise as initialise_tdigest, update as update_tdigest
from .threshold import update_threshold
from .variance import update_var

__all__ = [
    "initialise_tdigest",
    "finalize_histogram",
    "finalize_histogram_mpi",
    "finalize_iams",
    "finalize_percentile",
    "finalize_percentile_mpi",
    "finalize_std",
    "update_iams",
    "update_mean",
    "update_min",
    "update_max",
    "update_sum",
    "update_tdigest",
    "update_threshold",
    "update_var",
]
