"""One-Pass variance library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING

import numpy as np
import xarray as xr

from one_pass.options import FrequencyKey
from .mean import two_pass_mean, update_mean

if TYPE_CHECKING:
    from one_pass.opa import Opa


def remultiply_varience(opa: "Opa"):
    """Restore variance to M2 form for continuous statistics.

    For continuous statistics, variance is divided by (n-1) when output.
    Before continuing computation with new data, this function restores
    the variance to its M2 (sum of squared deviations) form.

    Parameters
    ----------
    opa : Opa
        OPA instance with continuous variance statistic.

    Notes
    -----
    This function modifies opa.statistics.cumulative["var"] in place,
    multiplying by (count_continuous - 1) when starting a new statistic
    period after output.
    """
    cumulative = opa.statistics.cumulative
    cumulative_var = cumulative["var"]
    if opa.request.stat_freq == FrequencyKey.CONTINUOUS:
        if opa.time.count_continuous > 0:
            if opa.time.count == 0:
                cumulative["var"] = cumulative_var * (opa.time.count_continuous - 1)


def two_pass_var(data_source: xr.DataArray):
    """Compute sample variance using NumPy's two-pass algorithm.

    Uses NumPy's var function with ddof=1 (sample variance) to compute
    variance over the time dimension for data chunks with multiple time steps.

    Parameters
    ----------
    data_source : xr.DataArray
        Input data chunk with time dimension greater than 1.

    Returns
    -------
    np.ndarray
        Sample variance values over the time dimension with shape (1, ...).
    """
    ax_num = data_source.get_axis_num("time")
    temp = np.var(data_source, axis=ax_num, dtype=np.float64, keepdims=True, ddof=1)

    return temp


def preprocess_var(opa: "Opa", data_source: xr.DataArray, weight: int) -> xr.DataArray:
    """Handle remultiplication for continuous frequency.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Processed data source
    """
    remultiply_varience(opa)
    return data_source


def calculate_var(opa: "Opa", data_source: xr.DataArray, weight: int) -> float:
    """Main variance calculation logic.

    Arguments
    ---------
    opa : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Calculated variance value
    """
    cumulative = opa.statistics.cumulative
    cumulative_mean = cumulative["mean"]
    cumulative_var = cumulative["var"]
    old_mean = cumulative_mean.copy()

    if weight == 1:
        update_mean(opa, data_source, weight)
        return cumulative_var + weight * (data_source.data - old_mean) * (
            data_source.data - cumulative_mean
        )
    temp_mean = two_pass_mean(data_source)
    temp_var = (two_pass_var(data_source)) * (weight - 1)

    if opa.request.stat_freq != FrequencyKey.CONTINUOUS:
        return (
            cumulative_var
            + temp_var.data
            + np.square(old_mean - temp_mean.data)
            * ((opa.time.count * weight) / (opa.time.count + weight))
        )
    return (
        cumulative_var
        + temp_var.data
        + np.square(old_mean - temp_mean.data)
        * ((opa.time.count_continuous * weight) / (opa.time.count_continuous + weight))
    )


def postprocess_var(opa: "Opa", result: float, weight: int) -> float:
    """Handle final variance calculation and update cumulative values.

    Arguments
    ---------
    opa : Opa class
    result : Calculated variance value
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    Final variance value
    """

    # TODO: Clean-up/refactor to a clearer place after this.

    # NOTE: Attributes are updated after this
    count_current = opa.time.count + weight
    if opa.time.continuous:
        count_total = opa.time.count_continuous + weight
    else:
        count_total = count_current
    if count_current == opa.time.n_data:
        result = result / (count_total - 1)

    cumulative = opa.statistics.cumulative
    cumulative["var"] = result
    return result


def update_var(opa: "Opa", data_source: xr.DataArray, weight: int):
    """Update rolling variance with new data chunk using one-pass algorithm.

    Implements Welford's online variance algorithm, maintaining M2 (sum of
    squared deviations) during accumulation and converting to variance when
    the statistic is complete.

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
    np.ndarray
        Updated cumulative variance or M2 value. When the statistic is
        complete (n == n_data), returns actual variance (M2 / (n-1)).
        Otherwise returns M2 for continued accumulation.

    Notes
    -----
    This function calls update_mean internally, which updates opa.time.count.
    The variance is stored as M2 until finalization, then divided by (n-1).
    """
    # 1. Pre-processing
    preprocessed_data = preprocess_var(opa, data_source, weight)

    # 2. Main calculation
    result = calculate_var(opa, preprocessed_data, weight)

    # 3. Post-processing
    final_result = postprocess_var(opa, result, weight)

    return final_result
