"""One-Pass min library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING, Tuple

import numpy as np
import xarray as xr

from one_pass.options import FrequencyKey

if TYPE_CHECKING:
    from one_pass.opa import Opa


def preprocess_min(
    opa: "Opa", data_source: xr.DataArray, weight: int
) -> Tuple[np.ndarray, np.ndarray]:
    """Extract chunk-level minima and corresponding timestamps.

    Finds the minimum value and its timestamp for each spatial location
    within the time dimension of the input chunk.

    Parameters
    ----------
    opa : Opa
        OPA instance (unused but kept for consistency).
    data_source : xr.DataArray
        Input data chunk with time dimension.
    weight : int
        Number of time steps (unused but kept for consistency).

    Returns
    -------
    tuple of (np.ndarray, np.ndarray)
        Minimum values and their corresponding timestamps, both with shape (1, ...).
    """

    # Axis index for "time" dim
    ax_num = data_source.get_axis_num("time")

    # Convert to NumPy array for fast reductions
    arr = data_source.values
    times = data_source.time.values

    # Argmin along time axis
    min_index = np.argmin(arr, axis=ax_num)

    # Min values
    min_values = np.min(arr, axis=ax_num, keepdims=True)

    # Corresponding timestamps - keep time dimension to match values
    min_times = times[min_index][np.newaxis, ...]

    return min_values, min_times


def calculate_min(
    opa: "Opa", preprocessed_data: Tuple[np.ndarray, np.ndarray], weight: int
) -> Tuple[np.ndarray, np.ndarray]:
    """Merge chunk minima with cumulative state using NumPy ops."""

    data_source, data_source_time = preprocessed_data

    has_previous = (
        opa.request.stat_freq != FrequencyKey.CONTINUOUS and opa.time.count > 0
    ) or (
        opa.request.stat_freq == FrequencyKey.CONTINUOUS
        and opa.time.count_continuous > 0
    )

    if has_previous:
        cumulative = opa.statistics.cumulative
        cumulative_min = cumulative["min"]
        cumulative_timings = cumulative["timings"]
        mask = data_source < cumulative_min
        values = np.where(mask, data_source, cumulative_min)
        times = np.where(mask, data_source_time, cumulative_timings)
    else:
        values = data_source
        times = data_source_time

    return values, times


def postprocess_min(
    opa: "Opa", result: Tuple[np.ndarray, np.ndarray], weight: int
) -> Tuple[np.ndarray, np.ndarray]:
    """Update cumulative values with results from this chunk."""

    min_values, min_times = result
    cumulative = opa.statistics.cumulative
    cumulative["min"] = min_values
    cumulative["timings"] = min_times

    return min_values, min_times


def update_min(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Update rolling minimum with new data chunk.

    Maintains the minimum value and its timestamp across all data seen so far.

    Parameters
    ----------
    opa : Opa
        OPA instance containing cumulative statistics.
    data_source : xr.DataArray
        Input data chunk with time dimension.
    weight : int
        Number of time steps in the chunk.

    Returns
    -------
    tuple of (np.ndarray, np.ndarray)
        Updated minimum values and their timestamps.
    """
    preprocessed = preprocess_min(opa, data_source, weight)
    result = calculate_min(opa, preprocessed, weight)
    final_result = postprocess_min(opa, result, weight)
    return final_result
