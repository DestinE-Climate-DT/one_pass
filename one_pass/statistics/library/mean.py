"""One-Pass mean library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING

import numpy as np
import xarray as xr

if TYPE_CHECKING:
    from one_pass.opa import Opa


def two_pass_mean(data_source: xr.DataArray):
    """Compute mean using NumPy's two-pass algorithm.

    Uses NumPy's mean function to compute the mean over the time dimension
    for data chunks with multiple time steps.

    Parameters
    ----------
    data_source : xr.DataArray
        Input data chunk with time dimension greater than 1.

    Returns
    -------
    np.ndarray
        Mean values over the time dimension with shape (1, ...).
    """
    ax_num = data_source.get_axis_num("time")
    temp = np.mean(data_source, axis=ax_num, dtype=np.float64, keepdims=True)

    return temp


def preprocess_mean(opa: "Opa", data_source: xr.DataArray, weight: int) -> xr.DataArray:
    """Preprocess data for one-pass mean calculation.

    For multi-timestep data, computes a two-pass mean first to reduce
    to a single timestep before applying one-pass algorithm.

    Parameters
    ----------
    opa : Opa
        OPA instance (unused but kept for consistency with other preprocessors).
    data_source : xr.DataArray
        Input data chunk.
    weight : int
        Number of time steps in the data chunk.

    Returns
    -------
    xr.DataArray
        Preprocessed data, reduced to single time step if weight > 1.
    """
    if weight > 1:
        # compute two pass mean first
        data_source = two_pass_mean(data_source)
    return data_source


def calculate_mean(opa: "Opa", data_source: xr.DataArray, weight: int) -> float:
    """Apply one-pass mean update formula.

    Implements the online/streaming mean update algorithm:
    new_mean = old_mean + weight * (new_value - old_mean) / new_count

    Parameters
    ----------
    opa : Opa
        OPA instance containing cumulative statistics and count.
    data_source : xr.DataArray
        Preprocessed input data chunk (single time step).
    weight : int
        Number of time steps being added.

    Returns
    -------
    np.ndarray
        Updated mean values.
    """
    cumulative = opa.statistics.cumulative
    cumulative_mean = cumulative["mean"]
    # opa.time.count will be updated after `the main update function`
    count = opa.time.count + weight

    return cumulative_mean + weight * (data_source.data - cumulative_mean) / count


def postprocess_mean(opa: "Opa", result: float, weight: int) -> float:
    """Store updated mean in cumulative statistics.

    Parameters
    ----------
    opa : Opa
        OPA instance to update.
    result : np.ndarray
        Calculated mean values.
    weight : int
        Number of time steps added (unused but kept for consistency).

    Returns
    -------
    np.ndarray
        The stored mean values (same as result).
    """
    cumulative = opa.statistics.cumulative
    cumulative["mean"] = result
    return result


def update_mean(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Update rolling mean with new data chunk using one-pass algorithm.

    Main entry point for mean statistic updates. Implements the streaming
    mean algorithm in three stages: preprocessing, calculation, and storage.

    Parameters
    ----------
    opa : Opa
        OPA instance containing cumulative statistics.
    data_source : xr.DataArray
        Input data chunk with time dimension.
    weight : int
        Number of time steps in the chunk (length of time dimension).

    Returns
    -------
    np.ndarray
        Updated cumulative mean values.

    Notes
    -----
    The time count (opa.time.count) is updated by the caller, not within
    this function.
    """
    # 1. Pre-processing
    preprocessed_data = preprocess_mean(opa, data_source, weight)

    # 2. Main calculation
    result = calculate_mean(opa, preprocessed_data, weight)

    # 3. Post-processing
    final_result = postprocess_mean(opa, result, weight)

    return final_result
