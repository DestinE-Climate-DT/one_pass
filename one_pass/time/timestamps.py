"""Timestamp validation and data splitting utilities.

This module provides functions for validating incoming timestamps, discarding
already-seen data, and splitting data chunks that span multiple aggregation periods.
"""

from bisect import bisect_right
from functools import partial
from logging import Logger
import typing as T
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import xarray as xr


from .conversion import get_minutes_into_word

if TYPE_CHECKING:
    from .opa_time import OpaTime


def _validate_time_gap(
    opatime: "OpaTime", first_timestamp: pd.Timestamp, logger: Logger
):
    """Validate the time gap between the last seen and first new timestamp."""

    time_gap = (first_timestamp - opatime.last_timestamp).total_seconds() / 60
    assert time_gap > 0, "This should have been larger than zero."

    if time_gap == opatime.timestep:
        return
    if time_gap < 2 * opatime.timestep:
        logger.warning(
            f"Time gap ({time_gap}) between last seen time stamp ({opatime.last_timestamp}) "
            f"and first unseen time stamp ({first_timestamp}) does not match the "
            f"expected difference ({opatime.timestep}) but within a tolerance "
            f"({2 * opatime.timestep}). Has the time step changed slightly? "
            f"The OPA will carry on."
        )
        return
    raise ValueError(
        f"Time gap ({time_gap}) between last seen time stamp ({opatime.last_timestamp}) "
        f"and first unseen time stamp ({first_timestamp}) is larger than the "
        f"expected difference ({opatime.timestep}). "
        "It seems that a few time steps have been missed."
    )


def _validate_timestamps(
    opatime: "OpaTime", timestamps: T.Sequence[pd.Timestamp], logger: Logger
):
    """Validate that incoming timestamps are monotonic, evenly spaced, and match request."""
    if len(timestamps) == 1:
        return

    # Time step differences (in minutes)
    timesteps = [
        (ts2 - ts1).total_seconds() / 60.0
        for ts1, ts2 in zip(timestamps[:-1], timestamps[1:])
    ]

    # Check if the timestamps are monotonically increasing
    if any(timestep <= 0.0 for timestep in timesteps):
        raise ValueError(
            "Incoming timestamps are not monotonically increasing. This is not a valid time series."
        )

    # Check if all timesteps have the same value
    if not all(difference == timesteps[0] for difference in timesteps):
        raise ValueError(
            "Incoming timestamps are unevenly spaced. At the moment this is not supported. "
            "Please, pass the data individually."
        )

    # All timesteps are the same, take the first one
    timestep = timesteps[0]

    # Check if the timestep matches the time step of the request
    if timestep != opatime.timestep:
        opatime.opa.request.time_step = timesteps[0]
        logger.warning(
            f"The timestep of the incoming data ({timesteps[0]}) "
            f"does not coincide with the time step of the request ({opatime.timestep}). "
            "The request has been modified to accomodate this data."
        )


def _discard_seen_data(
    opatime: "OpaTime",
    data_source: xr.DataArray,
    timestamps: T.Sequence[pd.Timestamp],
    logger: Logger,
):
    """Remove timestamps that have already been processed."""
    index = bisect_right(timestamps, opatime.last_timestamp)
    unseen_timestamps = timestamps[index:]

    if len(unseen_timestamps) == 0:
        return None, False

    if index == 0:
        _validate_time_gap(opatime, unseen_timestamps[0], logger)
        return data_source, True

    logger.warning(
        "Timestamps discarded due to being seen: "
        f"{', '.join(str(t) for t in timestamps[:index])}"
    )
    _validate_time_gap(opatime, unseen_timestamps[0], logger)
    return data_source.isel(time=slice(index, None)), True


def _get_start_point(
    opatime: "OpaTime",
    timestamps: T.List[pd.Timestamp],
    first_year: T.Optional[int] = None,
) -> T.Union[int, None]:
    """Find the first timestamp that corresponds to the start of a statistic period."""

    # Request timestep
    ts_request = opatime.timestep

    if opatime.continuous:
        time_word = opatime.opa.request.output_freq
    else:
        time_word = opatime.opa.request.stat_freq

    # Obtain a callable that takes in a timestamp and outputs a number of minutes
    fun = partial(get_minutes_into_word, time_word=time_word, first_year=first_year)

    # Positions in the list of timestamps that could be start points for the statistic,
    # given the Request time step and stat_freq (output_freq for continuous)
    # E.g. for a weekly statistic, positions corresponding to Mondays at 00:00, or
    #      a few minutes past, never exceeding the time step of the request
    start_indices = np.where(list(ts < ts_request for ts in map(fun, timestamps)))[0]

    # If no start point is found, return None
    if len(start_indices) == 0:
        return None

    # Otherwise return index
    return start_indices[0]


def _crop_previous_timestamps(
    opatime: "OpaTime",
    data_source: xr.DataArray,
    timestamps: T.Sequence[pd.Timestamp],
    logger: Logger,
):
    """Remove timestamps that don't align with the current statistic period start."""

    # If initialized, the first operand will be a timestamp.
    # Useful for periods longer than a year.
    # Otherwise, take the year of the first timestamp
    first_year = opatime.first_year or timestamps[0].year
    index = _get_start_point(opatime, timestamps, first_year)

    if index is None:
        return data_source, False

    if index == 0:
        return data_source, True

    logger.warning(
        "Timestamps discarded due not corresponding to current statistic: %s",
        f"{', '.join(str(t) for t in timestamps[:index])}",
    )
    # If there is a start point, crop only after it in the time dimension
    return data_source.isel(time=slice(index, None)), True


def validate(opatime: "OpaTime", data: xr.DataArray, logger: Logger) -> T.Tuple[
    xr.DataArray,
    bool,
    bool,
]:
    """Validate and filter incoming data timestamps.

    Checks incoming timestamps for correctness, removes already-seen data,
    and discards timestamps that don't align with the statistic period.

    Parameters
    ----------
    opatime : OpaTime
        Time tracking instance.
    data : xr.DataArray
        Input data with time dimension.
    logger : Logger
        Logger for warnings and errors.

    Returns
    -------
    tuple of (xr.DataArray, bool, bool)
        - Filtered data (potentially cropped in time dimension)
        - already_seen: True if all data was previously processed
        - to_init: True if data should trigger initialization

    Notes
    -----
    This function performs multiple validation steps:
    1. Validates timestamps are monotonic and evenly spaced
    2. Removes timestamps that were already processed
    3. Crops timestamps that don't align with statistic period start

    For uninitialized computations, only retains data starting from the
    first timestamp that aligns with the statistic period (e.g., for daily
    statistics starting at midnight, discards any data before midnight).
    """
    # assuming that incoming data has a time dimension

    # NOTE: Previous loop was:
    #     while (not proceed) and (index < weight):
    #
    # THIS ENDS:
    # [(not proceed) and (index < weight)]          is False
    # [(not proceed) is False]  or  [(index < weight) is False]
    # (proceed is True)         or  (index >= weight)

    timestamps = [pd.to_datetime(x) for x in data.time.data]

    # Check correctness of timestamps, i.e. non-overlapping, equally spaced, matching the request
    _validate_timestamps(opatime, timestamps, logger)

    if opatime.initialized:
        # When initialized, discard all data that has already been processed.
        data, check = _discard_seen_data(opatime, data, timestamps, logger)

        if not check:
            logger.warning("All incoming timestamps have already been seen.")
            return data, True, False

    else:
        # If still not initialized, discard all data that doesn't
        # correspond to the beginning of the requested aggregation,
        # e.g. for a daily aggregation (stat_freq: daily) and incoming data
        #
        #         22:00  23:00  00:00  01:00  02:00  ...  22:00  23:00
        #         -----  -----  -----  -----  -----       -----  -----
        #         disc.  disc.  start  incl.  incl.  ...  incl.  incl.
        #
        #   discard the two first data points
        data, proceed = _crop_previous_timestamps(opatime, data, timestamps, logger)

        # If no valid start point was found, do not initialize yet
        if not proceed:
            return data, False, False

    return data, False, True


def split(opatime: "OpaTime", data: xr.DataArray, logger: Logger):
    """Split data into current and carryover portions based on time boundaries.

    Parameters
    ----------
    opatime : OpaTime
        Time management instance containing aggregation boundaries
    data : xr.DataArray
        Input data to be split
    logger : Logger
        Logger instance for debug messages

    Returns
    -------
    tuple
        Tuple containing (current_data, carryover_data) where current_data
        contains data within the current aggregation period and carryover_data
        contains data that extends beyond the current period
    """

    if not opatime.initialized:
        raise RuntimeError("Something went wrong during pre-processing of aggregation.")

    # The data is already "cleaned" to be all unseen
    count = opatime.count
    n_data = opatime.n_data

    difference = n_data - count
    assert (
        difference != 0
    ), "Attempting to split DataArray for an already finalized statistic"

    if difference >= len(data.time.data):
        logger.debug(
            "Not splitting statistic since current data chunk does not complete period."
        )
        return data, None

    logger.debug(f"Splitting statistic by timestamp {str(data.time.data[difference])}.")
    return (
        data.isel(time=slice(None, difference)),
        data.isel(time=slice(difference, None)),
    )
