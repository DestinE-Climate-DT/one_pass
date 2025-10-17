"""Main update function for rolling statistics computation.

This module dispatches incoming data to the appropriate statistic-specific
update function based on the requested statistic type.
"""

from typing import TYPE_CHECKING

import pandas as pd
import xarray as xr

from one_pass.options import StatKey

from .library import (
    update_iams,
    update_mean,
    update_max,
    update_min,
    update_sum,
    update_tdigest,
    update_threshold,
    update_var,
)

if TYPE_CHECKING:
    from one_pass.opa import Opa


_UPDATE_FUN = {
    StatKey.MEAN: update_mean,
    StatKey.VAR: update_var,
    StatKey.STD: update_var,
    StatKey.MIN: update_min,
    StatKey.MAX: update_max,
    StatKey.SUM: update_sum,
    StatKey.THRESH_EXCEED: update_threshold,
    StatKey.PERCENTILE: update_tdigest,
    StatKey.HISTOGRAM: update_tdigest,
    StatKey.IAMS: update_iams,
}


def update_statistics(
    opa: object,
    data_source: xr.DataArray,
):
    """Update rolling statistics with new data.

    Main function for updating statistics. Calculates the remaining capacity
    for the current statistic and updates with the provided data. Returns
    whether the statistic period is now complete.

    Parameters
    ----------
    opa : Opa
        OPA instance containing statistics and time tracking.
    data_source : xr.DataArray
        Incoming data chunk to incorporate into statistics.

    Returns
    -------
    bool
        True if the statistic period is now complete (finalized),
        False if more data is needed.

    Notes
    -----
    This function:
    - Extracts timestamps from the data
    - Updates the appropriate statistic using registered update functions
    - Updates time tracking information
    - Logs progress information
    """

    # TODO: Move checkpointing to some level above the function stack.

    timestamp_list = [pd.to_datetime(t) for t in data_source.time.data]
    weight = len(timestamp_list)
    timesteps_left = opa.time.n_data - opa.time.count

    assert timesteps_left > 0, "Attempting to update statistic with no timesteps left."
    assert (
        weight <= timesteps_left
    ), "Update function has too much data on temporal domain."

    # update rolling statistic with weight
    _UPDATE_FUN[opa.request.stat](opa, data_source, weight)

    opa.logger.debug(
        "Statistic has just been updated with %s time stamps. \n"
        "It has completed %s out of the %s required to fill "
        "the statistic.",
        weight,
        opa.time.count,
        opa.time.n_data,
    )

    # moving the time stamp to the last of the set
    opa.time.update(weight, timestamp_list[-1])

    return weight == timesteps_left
