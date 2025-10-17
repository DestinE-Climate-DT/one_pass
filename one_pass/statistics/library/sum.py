"""One-Pass sum library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING

import numpy as np
import xarray as xr

if TYPE_CHECKING:
    from one_pass.opa import Opa


def preprocess_sum(opa: "Opa", data_source: xr.DataArray, weight: int) -> xr.DataArray:
    """Preprocess data for sum calculation.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Processed data source
    """
    if weight > 1:
        ax_num = data_source.get_axis_num("time")
        data_source = np.sum(data_source, axis=ax_num, keepdims=True)
    return data_source


def calculate_sum(opa: "Opa", data_source: xr.DataArray, weight: int) -> float:
    """Main sum calculation logic.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Calculated sum value
    """
    cumulative = opa.statistics.cumulative
    cumulative_sum = cumulative["sum"]
    return cumulative_sum + data_source.data


def postprocess_sum(opa: "Opa", result: float, weight: int) -> float:
    """Update cumulative values with calculated sum.

    Arguments
    ---------
    opa : Opa class
    result : Calculated sum value
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Final sum value
    """
    cumulative = opa.statistics.cumulative
    cumulative["sum"] = result
    return result


def update_sum(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Computes one pass sum with weight corresponding to the number
    of timesteps being added. Also updates count.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa.time.count : updated with weight
    opa.statistics.sum_cum : updated cumulative sum
    """
    # 1. Pre-processing
    preprocessed_data = preprocess_sum(opa, data_source, weight)

    # 2. Main calculation
    result = calculate_sum(opa, preprocessed_data, weight)

    # 3. Post-processing
    final_result = postprocess_sum(opa, result, weight)

    return final_result
