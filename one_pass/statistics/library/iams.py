"""One-Pass IAMS library."""

# pylint: disable=W0613

from typing import TYPE_CHECKING

import numpy as np
import xarray as xr

from one_pass.statistics.iams import Iams

if TYPE_CHECKING:
    from one_pass.opa import Opa


def _compute_window_sum(iams: Iams, start: int, length: int) -> np.ndarray:
    """Compute sum over a rolling window in circular buffer with wrap-around support.

    Parameters
    ----------
    iams : Iams
        IAMS state object containing the circular buffer.
    start : int
        Starting index in the buffer.
    length : int
        Window length (number of elements to sum).

    Returns
    -------
    np.ndarray
        Sum of values in the window with shape (1, ...).
    """

    buffer = iams.buffer

    full_length = buffer.shape[0]

    if start + length <= full_length:
        return buffer[start : start + length, :].sum(axis=0, keepdims=True)

    # wrap-around case
    part1 = buffer[start:, :].sum(axis=0, keepdims=True)
    part2 = buffer[: length - (full_length - start), :].sum(axis=0, keepdims=True)
    return part1 + part2


def _update_windows(iams: Iams, count: int) -> None:
    """Update IAMS maxima for all duration windows after adding new data.

    Parameters
    ----------
    iams : Iams
        IAMS state object to update.
    count : int
        Current global count (unused but part of interface).

    Notes
    -----
    This function updates the cumulative maxima for each duration window
    once enough data has been accumulated for that window size.
    """
    full_length = iams.buffer.shape[0]

    for idx, n_data in enumerate(iams.n_data_durations):
        count = int(iams.count_durations[idx])
        count_full = int(iams.count_durations_full[idx])

        # Reset if buffer looped
        if count >= full_length:
            count = 0

        if (count_full + n_data) <= count:
            window_sum = _compute_window_sum(iams, count, n_data)
            iams.cumulative[idx, :] = np.maximum(window_sum, iams.cumulative[idx, :])
            count += 1
            count_full += 1

        iams.count_durations[idx] = count
        iams.count_durations_full[idx] = count_full


def calculate_iams(
    iams: Iams,
    data_chunk: np.ndarray,
    weight: int,
    count: int,
) -> np.ndarray:
    """
    Merge new data with rolling IAMS state:
    - Update buffer circular buffer.
    - Perform one-pass window sums and update maxima.

    Returns
    -------
    np.ndarray
        Updated cumulative maxima for all durations.
    """

    full_length = iams.n_data_durations[-1]

    position = count % full_length

    for step in range(weight):

        # Update rolling buffer with current time step
        iams.buffer[position : position + 1, :] = data_chunk[step : step + 1, :, :]

        # update windows
        _update_windows(iams, count)

        # advance counters
        count += 1
        position = count % full_length

    return iams.cumulative


def update_iams(opa: "Opa", data_source: xr.DataArray, weight: int) -> np.ndarray:
    """Update IAMS (Incremental Annual Maximum Statistic) with new data.

    Maintains rolling maximum sums over multiple duration windows,
    tracking the largest sum observed for each duration.

    Parameters
    ----------
    opa : Opa
        OPA instance containing IAMS state.
    data_source : xr.DataArray
        Input data chunk with time dimension.
    weight : int
        Number of time steps in the chunk.

    Returns
    -------
    np.ndarray
        Updated cumulative maxima for all durations with shape (n_durations, ...).
    """

    iams = opa.statistics.cumulative["iams"]
    return calculate_iams(iams, data_source.values, weight, opa.time.count)


def finalize_iams(opa: "Opa") -> np.ndarray:
    """Extract final IAMS statistics after computation completes.

    Parameters
    ----------
    opa : Opa
        OPA instance with finalized IAMS state.

    Returns
    -------
    np.ndarray
        Final IAMS maxima with time dimension added, shape (1, n_durations, ...).
    """

    # adding time dimension in final stat
    iams = opa.statistics.cumulative["iams"]
    return np.expand_dims(iams.cumulative, axis=0)
