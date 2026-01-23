"""Test IAMS buffer wraparound scenarios.

This test module verifies correct IAMS computation across various
scenarios involving circular buffer wraparound:
- Single and multiple wraparounds
- Incremental processing (one timestep at a time)
- Extended computations beyond buffer size
- Individual duration window correctness
"""

import numpy as np
import xarray as xr
import pytest

from one_pass.statistics.iams import Iams
from one_pass.statistics.library.iams import calculate_iams
from init_tests import (
    get_test_data,
    get_opa_stat,
    temp_directory,
    TOLERANCE,
    _not_close_message,
)


def compute_iams_reference(data: xr.DataArray, durations: list) -> np.ndarray:
    """
    Compute IAMS using a simple two-pass rolling window approach.

    This serves as ground truth for validating the one-pass algorithm,
    especially for buffer wraparound scenarios.

    Parameters
    ----------
    data : xr.DataArray
        Input data with time dimension.
    durations : list
        List of durations (in number of time steps).

    Returns
    -------
    np.ndarray
        Maximum rolling sums for each duration, shape (n_durations, ...).
    """
    results = []

    for duration in durations:
        # Compute rolling sum
        rolling_sum = data.rolling(time=duration, center=False).sum()
        # Get maximum (ignoring NaN from incomplete windows at start)
        rolling_max = rolling_sum.max(dim="time", skipna=True)
        results.append(rolling_max.values)

    return np.array(results)


def compute_iams_direct(data: np.ndarray, time_step: int = 60) -> np.ndarray:
    """
    Compute IAMS directly using the library function.

    This bypasses the Opa interface and tests the core algorithm directly,
    allowing us to test with any amount of data.

    Parameters
    ----------
    data : np.ndarray
        Input data with shape (time, ...).
    time_step : int
        Time step in minutes.

    Returns
    -------
    np.ndarray
        IAMS cumulative maxima for all durations.
    """
    shape = data.shape
    iams = Iams(time_step, shape)

    count = 0
    for i in range(shape[0]):
        data_chunk = data[i : i + 1, :, :]
        calculate_iams(iams, data_chunk, weight=1, count=count)
        count += 1

    return iams.cumulative


def test_iams_buffer_wraparound_single_cycle(temp_directory):
    """
    Test IAMS computation when circular buffer wraps around once.

    Processes 2 weeks of hourly data (336 hours), which exceeds the longest
    duration window (168 hours), ensuring the circular buffer wraps around.
    Verifies that maximum rolling sums are correctly computed across the
    wraparound boundary.
    """
    # Process more data than the longest duration window
    # With 60-minute timestep, longest duration is 10080 min = 168 hours = 1 week
    # So we process 2 weeks of data
    n_data = 336  # 14 days * 24 hours

    # Get test data
    TEST_DATA = get_test_data()
    data_arr = TEST_DATA["pr"][:n_data, 0:10, 0:10]

    # Compute using one-pass algorithm
    one_pass = compute_iams_direct(data_arr.values, time_step=60)

    # Verify output is not all zeros (windows should have non-zero sums)
    assert not np.allclose(
        one_pass, 0.0
    ), "IAMS output is all zeros! Expected non-zero maximum rolling sums for precipitation data."

    # Compute reference using simple rolling window
    durations = Iams.duration_pick(60, "hours")
    reference = compute_iams_reference(data_arr, durations)

    # Verify reference is also not all zeros (sanity check)
    assert not np.allclose(
        reference, 0.0
    ), "Reference implementation produced all zeros - test data issue?"

    # Compare results
    max_diff = np.max(np.abs(reference - one_pass))
    assert np.allclose(
        reference, one_pass, rtol=TOLERANCE, atol=TOLERANCE
    ), f"Buffer wraparound test failed. Max difference: {max_diff}"


def test_iams_buffer_wraparound_multiple_cycles(temp_directory):
    """
    Test IAMS computation with multiple buffer wraparounds.

    Processes 4 weeks of hourly data (672 hours), causing the circular
    buffer to wrap around approximately 4 times. Verifies that the algorithm
    maintains correct cumulative maxima across multiple complete cycles.
    """
    # Process 4 weeks of data (4x the longest duration)
    n_data = 672  # 28 days * 24 hours

    # Get test data
    TEST_DATA = get_test_data()
    data_arr = TEST_DATA["pr"][:n_data, 0:10, 0:10]

    # Compute using one-pass algorithm
    one_pass = compute_iams_direct(data_arr.values, time_step=60)

    # Verify output contains non-zero maximum values
    assert not np.allclose(
        one_pass, 0.0
    ), "IAMS output is all zeros! Expected non-zero maximum rolling sums."

    # Compute reference using simple rolling window
    durations = Iams.duration_pick(60, "hours")
    reference = compute_iams_reference(data_arr, durations)

    # Compare results
    max_diff = np.max(np.abs(reference - one_pass))
    assert np.allclose(
        reference, one_pass, rtol=TOLERANCE, atol=TOLERANCE
    ), f"Multiple buffer wraparound test failed. Max difference: {max_diff}"


def test_iams_buffer_wraparound_incremental(temp_directory):
    """
    Test IAMS computation with incremental per-timestep processing.

    Processes 10 days of data (240 hours) one hour at a time internally.
    This fine-grained processing pattern tests the correctness of per-step
    window updates and buffer position tracking.
    """
    # Process 10 days of data
    n_data = 240  # 10 days * 24 hours

    # Get test data
    TEST_DATA = get_test_data()
    data_arr = TEST_DATA["pr"][:n_data, 0:10, 0:10]

    # Compute using one-pass algorithm (processes one hour at a time internally)
    one_pass = compute_iams_direct(data_arr.values, time_step=60)

    # Verify output contains non-zero maximum values
    assert not np.allclose(
        one_pass, 0.0
    ), "IAMS output is all zeros! Expected non-zero maximum rolling sums."

    # Compute reference using simple rolling window
    durations = Iams.duration_pick(60, "hours")
    reference = compute_iams_reference(data_arr, durations)

    # Compare results
    max_diff = np.max(np.abs(reference - one_pass))
    assert np.allclose(
        reference, one_pass, rtol=TOLERANCE, atol=TOLERANCE
    ), f"Incremental wraparound test failed. Max difference: {max_diff}"


def test_iams_all_durations_updated(temp_directory):
    """
    Test that all duration windows produce correct results.

    Processes 3 weeks of data (504 hours) and verifies each individual
    duration window (1h, 6h, 12h, 24h, etc.) produces non-zero maximum
    rolling sums that match the reference implementation.
    """
    # Process enough data to ensure all durations have sufficient coverage
    n_data = 504  # 21 days * 24 hours (3 weeks)

    # Get test data
    TEST_DATA = get_test_data()
    data_arr = TEST_DATA["pr"][:n_data, 0:10, 0:10]

    # Compute using one-pass algorithm
    one_pass = compute_iams_direct(data_arr.values, time_step=60)

    # Verify overall output contains non-zero values
    assert not np.allclose(
        one_pass, 0.0
    ), "IAMS output is all zeros! Expected non-zero maximum rolling sums."

    # Compute reference using simple rolling window
    durations = Iams.duration_pick(60, "hours")
    reference = compute_iams_reference(data_arr, durations)

    # Check each duration separately to ensure all produce correct results
    for idx, duration in enumerate(durations):
        # Each duration should have non-zero maximum values
        assert not np.allclose(
            one_pass[idx], 0.0
        ), f"Duration {duration} hours produced all zeros - expected non-zero maxima!"

        # And should match the reference
        duration_match = np.allclose(
            reference[idx], one_pass[idx], rtol=TOLERANCE, atol=TOLERANCE
        )
        assert duration_match, (
            f"Duration {duration} hours failed. "
            f"Max difference: {np.max(np.abs(reference[idx] - one_pass[idx]))}"
        )


def test_iams_extended_computation(temp_directory):
    """
    Test IAMS computation stability over extended data processing.

    Processes 33+ days of data (800 hours), significantly exceeding the
    buffer size and causing many complete wraparounds. Verifies the algorithm
    maintains numerical stability and produces correct results over long runs.
    """
    # Process over a month of data to test long-term stability
    n_data = 800  # Over 33 days of hourly data

    # Get test data
    TEST_DATA = get_test_data()
    data_arr = TEST_DATA["pr"][:n_data, 0:10, 0:10]

    # Compute using one-pass algorithm
    one_pass = compute_iams_direct(data_arr.values, time_step=60)

    # Verify output contains non-zero maximum values
    assert not np.allclose(
        one_pass, 0.0
    ), "IAMS output is all zeros! Expected non-zero maximum rolling sums."

    # Compute reference
    durations = Iams.duration_pick(60, "hours")
    reference = compute_iams_reference(data_arr, durations)

    # Compare results
    max_diff = np.max(np.abs(reference - one_pass))
    assert np.allclose(
        reference, one_pass, rtol=TOLERANCE, atol=TOLERANCE
    ), f"Extended computation test failed. Max difference: {max_diff}"
