"""Module for finalizing statistics and creating output datasets.

This module contains functions for converting rolling cumulative statistics
into final output datasets, including metadata handling and dimension management.
"""

from datetime import datetime
from typing import Union, Sequence, Tuple, TYPE_CHECKING
import importlib.metadata

import dask.array as da
import numpy as np
import xarray as xr

from one_pass.options import StatKey, FrequencyKey
from one_pass.mpi import MPI_LAUNCHED, MPI_RANK

from .library import (
    finalize_histogram,
    finalize_histogram_mpi,
    finalize_iams,
    finalize_percentile,
    finalize_percentile_mpi,
    finalize_std,
)

if TYPE_CHECKING:
    from one_pass.opa import Opa

try:
    __version__ = importlib.metadata.version("one_pass")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"


def get_now_str():
    """Get current timestamp as formatted string.

    Returns
    -------
    str
        Current timestamp formatted as "YYYY-MM-DD HH:MM:SS"
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def make_attr(value: str = ""):
    """Create attribute string with timestamp and OPA information.

    Parameters
    ----------
    value : str, optional
        Description of the values being assigned, by default ""

    Returns
    -------
    str
        Formatted attribute string with timestamp and OPA information
    """
    return (
        f"{get_now_str()} OPA : {value} values assigned with the one_pass algorithm"
        "\n"
    )


def _convert_thresh_exceed(
    opa: "Opa", wireframe: xr.DataArray
) -> Tuple[xr.DataArray, None]:
    """Add threshold dimension to wireframe for threshold exceedance statistics."""
    # TODO, In-place operations?
    wireframe = wireframe.expand_dims(
        dim={"thresholds": len(opa.request.thresh_exceed)}, axis=1
    )
    new_coord_attrs = {"history": make_attr(StatKey.THRESH_EXCEED.value)}
    wireframe = wireframe.assign_coords(
        thresholds=("thresholds", opa.request.thresh_exceed, new_coord_attrs)
    )
    return wireframe


def _convert_percentile(
    opa: "Opa", wireframe: xr.DataArray
) -> Tuple[xr.DataArray, None]:
    """Add percentile dimension to wireframe for percentile statistics."""
    wireframe = wireframe.expand_dims(
        dim={StatKey.PERCENTILE: np.size(opa.request.percentile_list)}, axis=1
    )
    new_coord_attrs = {"history": make_attr(StatKey.PERCENTILE.value)}
    wireframe = wireframe.assign_coords(
        percentile=(
            "percentile",
            np.array(opa.request.percentile_list),
            new_coord_attrs,
        )
    )
    return wireframe


def _convert_iams(
    opa: "Opa", wireframe: xr.DataArray
) -> Tuple[xr.DataArray, np.ndarray]:
    """Add duration dimension to wireframe for IAMS statistics."""
    # adding time dimension in final stat
    iams = opa.statistics.cumulative["iams"]
    durations = iams.durations

    # adding durations dimension in ds
    wireframe = wireframe.expand_dims(dim={"durations": np.size(durations)}, axis=1)
    new_coord_attrs = {"history": make_attr("duration")}
    # name the durations co-ordinate
    wireframe = wireframe.assign_coords(
        durations=("durations", np.array(durations), new_coord_attrs)
    )
    return wireframe


# pylint: disable=W0613
def _convert_histogram(
    opa: "Opa", wireframe: xr.DataArray
) -> Tuple[xr.DataArray, xr.DataArray]:
    """Create wireframes for histogram bin counts and edges."""
    dim_0 = np.shape(wireframe)[0]
    return (
        wireframe.expand_dims(dim={"bin_count": dim_0}, axis=1),
        wireframe.expand_dims(
            dim={"bin_edges": dim_0 + 1},
            axis=1,
        ),
    )


# The following dict contains a series of functions that may convert
# Keys:
#     StatKey options
# Values:
#     Functions with signature:
#
#     Argument
#     --------
#          opa : Opa
#              Opa instance with finalized aggregation
#          wireframe : xr.DataArray
#              "wireframe" xr.DataArray, which serves as blueprint for final DataSet creation
#     Output
#     ------
#          wireframe : xr.DataArray
#              Modified "wireframe" xr.DataArray
#          final_stat : np.ndarray or None
#              Modified final statistic from OpaStatistics.get_final(), if needs to be modified.
#              Else None
#
# TODO: Implement, at least modification of final_stat at OpaStatistic level,
# potentially in OpaStatistics.get_final()

_SHAPE_CONVERSION = {
    StatKey.HISTOGRAM: _convert_histogram,
    StatKey.IAMS: _convert_iams,
    StatKey.PERCENTILE: _convert_percentile,
    StatKey.THRESH_EXCEED: _convert_thresh_exceed,
}

_FINALIZE_FUNCTIONS = {
    StatKey.HISTOGRAM: finalize_histogram,
    StatKey.IAMS: finalize_iams,
    StatKey.PERCENTILE: finalize_percentile,
    StatKey.STD: finalize_std,
}


def _get_finalize_function(stat: StatKey, opa: "Opa"):
    """Get the appropriate finalize function based on runtime MP/MPI state.

    This checks opa.workers and MPI_LAUNCHED at runtime (not import time)
    to select the correct finalize function.

    Note: For MP mode (workers > 1), finalize_histogram and finalize_percentile
    handle MP internally, so we always use them (not MP-specific versions).
    """

    # MPI mode: use MPI-specific functions
    if MPI_LAUNCHED and stat == StatKey.HISTOGRAM:
        return finalize_histogram_mpi
    if MPI_LAUNCHED and stat == StatKey.PERCENTILE:
        return finalize_percentile_mpi

    # MP mode (workers > 1) and serial mode: use regular functions
    # (they handle MP internally)
    if stat in _FINALIZE_FUNCTIONS:
        return _FINALIZE_FUNCTIONS[stat]
    return None


def _normalize_ndarray(
    value: Union[
        np.ndarray,
        xr.DataArray,
        Tuple[np.ndarray, np.ndarray],
        Tuple[xr.DataArray, xr.DataArray],
    ]
) -> Union[np.ndarray, Tuple[np.ndarray, np.ndarray]]:
    if isinstance(value, tuple):
        assert len(value) == 2, "Malformed final_stat"
        if all(isinstance(element, np.ndarray) for element in value):
            return value
        if all(isinstance(element, xr.DataArray) for element in value):
            return (value[0].data, value[1].data)
        if all(isinstance(element, da.Array) for element in value):
            return (value[0], value[1])
        raise TypeError(
            "Bad types for final_stat value elements: "
            + ", ".join(type(el).__name__ for el in value)
        )
    if isinstance(value, np.ndarray):
        return value
    if isinstance(value, xr.DataArray):
        return value.data
    if isinstance(value, da.Array):
        return value
    raise TypeError(f"Bad type for final_stat: {type(value).__name__}.")


def _to_dataset(
    opa: "Opa",
    final_stat: np.ndarray,
    wireframe: np.ndarray,
):
    # NOTE: Data is used as a "wireframe" xr.DataArray to
    # copy names, coordinates, attributes, dimensions, and so on.
    attributes = opa.data_set_info.data_var_attr

    ds = xr.Dataset(
        data_vars=dict(
            # need to add variable attributes
            [
                (
                    str(wireframe.name),
                    xr.Variable(wireframe.dims, final_stat, attributes),
                )
            ],
        ),
        coords=dict(wireframe.coords),
        attrs=opa.data_set_info.data_set_attr,
    )

    if "timings" in opa.statistics.cumulative:
        # TODO: Clean way to get history str.
        new_attr_str = str(
            get_now_str()
            + f" timestamp of {opa.request.stat_freq.value} "
            + f"{opa.request.stat.value} calculated using one_pass algorithm\n"
        )
        timing_attrs = {"history": new_attr_str}
        timings_array = opa.statistics.cumulative["timings"]

        if timings_array.ndim == 2:
            # From the update functions, np.where checking against the mask automatically
            # casted to (1, M, N). However, if the update was called a single time with
            # default arguments, it assigned directly the underlying time array,
            # resulting in (M, N), therefore need to cast here manually.
            # TODO: Have this ndim normalized when assigning in the update functions.
            timings_array = np.expand_dims(timings_array, axis=0)

        # Check that timings array has the same number of dimensions as wireframe
        expected_ndim = len(wireframe.dims)
        assert timings_array.ndim == expected_ndim, (
            "Wrong number of dimensions for timings. "
            f"Expected {expected_ndim}, got {timings_array.ndim}."
        )

        ds = ds.assign(
            timings=(
                list(wireframe.dims),
                timings_array,
                timing_attrs,
            )
        )
    return ds


def _get_wireframe_data_array(opa: "Opa", data_source: xr.DataArray):
    # "wireframe" is a data array with temporal dimension 1, which gets assigned
    # all the attributes and variables passed down from the original data source
    # TODO: Does copy exist?

    data_source_tail = data_source.tail(time=1)
    time_attrs = data_source_tail.time.attrs

    wireframe = data_source_tail.copy()

    # re-label the time coordinate
    wireframe = wireframe.assign_coords(
        time=(["time"], [opa.time.init_timestamp], time_attrs)
    )

    if opa.request.stat in _SHAPE_CONVERSION:
        return _SHAPE_CONVERSION[opa.request.stat](opa, wireframe)

    return wireframe


def _get_final_statistic(opa: "Opa"):
    """Extract and finalize the statistic from cumulative state.

    Converts rolling statistics into their final form, applying any necessary
    transformations (e.g., computing std from variance, extracting percentiles
    from TDigests).

    Parameters
    ----------
    opa : Opa
        OPA instance with finalized cumulative statistics.

    Returns
    -------
    np.ndarray or tuple of np.ndarray
        Finalized statistic array(s). Returns a tuple for histogram statistics
        (bin counts and edges).
    """

    # TODO: Skip this step in favor of creating final datasets.

    stat = opa.request.stat

    finalize_fun = _get_finalize_function(stat, opa)
    if finalize_fun is not None:
        opa.logger.debug("Finalize function: %s" % finalize_fun.__name__)
        final_stat = finalize_fun(opa)
    else:
        final_stat = opa.statistics.cumulative[stat]

    if MPI_LAUNCHED and MPI_RANK != 0:
        return None
    return _normalize_ndarray(final_stat)


# TODO: Drop second argument (and create xr.DataArray from scratch?)
def create_data_set(
    opa: "Opa", data_source: xr.DataArray
) -> Union[xr.Dataset, Sequence[xr.Dataset], None]:
    """Creates xarray dataSet object with final data depending
    on the statistic and updated metadata

    Arguments
    ----------
    opa_stat : Opa class
    data_source : xr.DataArray. original data used for sizing

    Returns
    ---------
    opa.statistics.final_cum : the final dm (returned from
            create_final_dataset) given as an attribute to the OPA
            class. This is given the name 'final_cum' so that it
            can be removed easily
    """

    # TODO: Remove dependency with data_source and whole "wireframe" concept.
    # Use DataSetInfo instead.
    wireframe = _get_wireframe_data_array(opa, data_source)

    final_stat = _get_final_statistic(opa)

    if MPI_LAUNCHED and final_stat is None:
        return None

    if isinstance(wireframe, tuple):
        assert isinstance(
            final_stat, tuple
        ), "wireframe and final_stat have different types during dataset creation."
        assert all(isinstance(element, xr.DataArray) for element in wireframe)
        return tuple(
            _to_dataset(opa, final_stat_el, wireframe_el)
            for final_stat_el, wireframe_el in zip(final_stat, wireframe)
        )

    return _to_dataset(opa, final_stat, wireframe)


def _get_history_attribute(opa: "Opa"):
    """Gathers final data and meta data for the final xarray Dataset
    and writes data attributes of the Dataset depending on the statistic
    requested.

    Creates an appropriate history string documenting the OPA computation,
    including timestamps and statistic details.

    Parameters
    ----------
    opa : Opa
        OPA instance containing computation details.

    Returns
    -------
    str
        Formatted history attribute string for dataset metadata.
    """
    # TODO: Tidy up
    if opa.request.stat_freq == FrequencyKey.CONTINUOUS:
        return (
            f"{get_now_str()} OPA : {opa.request.stat_freq.value} "
            f"{opa.request.stat.value} calculated using one_pass algorithm \n"
            "This xr.Dataset shows the rolling summary at "
            f"{opa.time.init_timestamp}. The statistic started at "
            f"{opa.time.init_count_timestamp}"
        )
    return (
        f"{get_now_str()} {opa.request.stat_freq.value} {opa.request.stat.value} "
        "calculated using one_pass algorithm"
    )


def _update_history(dct: dict, value: str):
    """Append a value to the history attribute in a metadata dictionary."""
    history = dct.get("history", "")
    history = history.strip("\n") + "\n" + value
    dct["history"] = history


def _update_opa_attrs(opa: "Opa", dct: dict):

    # Create the lists in the attributes dictionary if they don't exist
    if "OPA_last_timestamp" not in dct:
        dct["OPA_last_timestamp"] = []
    if "OPA_num_timestamps" not in dct:
        dct["OPA_num_timestamps"] = []

    # Opa.time last timestamp
    last_timestamp = opa.time.last_timestamp

    # Convert last timestamp to string
    if last_timestamp is not None:
        last_timestamp_str = last_timestamp.isoformat()
    else:
        last_timestamp_str = "unknown"

    # Update the attributes
    dct["OPA_timestep"] = opa.request.time_step
    dct["OPA_last_timestamp"].append(last_timestamp_str)
    dct["OPA_num_timestamps"].append(opa.time.count)
    dct["OPA_version"] = __version__


# TODO: Clean and fix (in principle only one attribute needed.)
def update_attributes(opa: "Opa"):
    """Update Opa.DataSetInfo data_set_attr and data_var_attr histories."""

    history = _get_history_attribute(opa)
    _update_history(opa.data_set_info.data_set_attr, history)
    _update_history(opa.data_set_info.data_var_attr, history)
    _update_opa_attrs(opa, opa.data_set_info.data_set_attr)
    _update_opa_attrs(opa, opa.data_set_info.data_var_attr)
