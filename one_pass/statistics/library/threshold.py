"""One-Pass threshold library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING

import numpy as np
import xarray as xr

from one_pass.options import StatKey

if TYPE_CHECKING:
    from one_pass.opa import Opa


def preprocess_threshold(
    opa: "Opa", data_source: xr.DataArray, weight: int
) -> xr.DataArray:
    """
    Preprocess data for multi-threshold calculation.

    Arguments
    ---------
    opa : Opa class
    data_source : xr.DataArray of shape (time, lat, lon)
    weight : number of timesteps in the incoming data chunk

    Returns
    ---------
    xr.DataArray of shape (1, num_thresholds, lat, lon)
    """
    thresholds = np.array(opa.request.thresh_exceed)  # shape (num_thresholds,)
    num_thresholds = thresholds.size

    # Expand data for broadcasting: (time, 1, lat, lon)
    data_exp = data_source.expand_dims({"threshold": num_thresholds}, axis=1)
    data_exp = data_exp.assign_coords({"threshold": thresholds})

    exceedances = data_exp > thresholds[:, np.newaxis, np.newaxis]  # broadcasts

    if weight > 1:
        # Sum over time to get counts
        exceedances = exceedances.sum(dim="time", keepdims=True)
    else:
        exceedances = exceedances.astype(int).sum(dim="time", keepdims=True)

    # Ensure threshold coordinate order is preserved
    # exceedances = exceedances.transpose("time", "threshold", "lat", "lon")

    return exceedances


def calculate_threshold(opa: "Opa", data_source: xr.DataArray, weight: int) -> float:
    """Main threshold calculation logic.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Calculated threshold value
    """
    cumulative = opa.statistics.cumulative
    cumulative_threshold = cumulative[StatKey.THRESH_EXCEED]
    return cumulative_threshold + data_source


def postprocess_threshold(opa: "Opa", result: float, weight: int) -> float:
    """Update cumulative values with calculated threshold.

    Arguments
    ---------
    opa : Opa class
    result : Calculated threshold value
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Final threshold value
    """
    cumulative = opa.statistics.cumulative
    cumulative[StatKey.THRESH_EXCEED] = result
    return result


def update_threshold(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Computes one pass threshold with weight corresponding to the number
    of timesteps being added. Also updates count.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa.time.count : updated with weight
    opa.statistics.threshold_cum : updated cumulative threshold
    """

    # 1. Pre-processing
    preprocessed_data = preprocess_threshold(opa, data_source, weight)

    # 2. Main calculation
    result = calculate_threshold(opa, preprocessed_data, weight)

    # 3. Post-processing
    final_result = postprocess_threshold(opa, result, weight)

    return final_result
