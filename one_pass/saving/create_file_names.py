"""Utilities for creating output file names for NetCDF files."""

from typing import Optional, TYPE_CHECKING

import pandas as pd
import xarray as xr

from one_pass.options import FrequencyKey

if TYPE_CHECKING:
    from one_pass.opa import Opa


def create_raw_file_name(data_source: xr.DataArray) -> str:
    """Create filename for raw data output.

    Generates a descriptive filename based on the timestamps present in the raw data.

    Parameters
    ----------
    data_source : xr.DataArray
        Input data containing timestamps.

    Returns
    -------
    str
        Filename string with format YYYY_MM_DD_THH_MM for single timestamp,
        or YYYY_MM_DD_THH_MM_to_YYYY_MM_DD_THH_MM for multiple timestamps.
    """
    timestamp_sorted = sorted(data_source.time.data)
    timestamp_list = [pd.to_datetime(x) for x in timestamp_sorted]

    if len(timestamp_list) > 1:
        return (
            timestamp_list[0].strftime("%Y_%m_%d_T%H_%M")
            + "_to_"
            + timestamp_list[-1].strftime("%Y_%m_%d_T%H_%M")
        )
    return timestamp_list[0].strftime("%Y_%m_%d_T%H_%M")


def _create_filename_span(opa: "Opa", time_word: FrequencyKey):
    """Create filename for appended statistics spanning multiple periods.

    Parameters
    ----------
    opa : Opa
        OPA instance with time and request information.
    time_word : FrequencyKey
        Temporal frequency determining the filename format.

    Returns
    -------
    str
        Filename spanning from first to last timestamp in the aggregation.

    Raises
    ------
    RuntimeError
        If an unsupported time_word is provided.
    """
    if time_word in (
        FrequencyKey.HOURLY,
        FrequencyKey.TWO_HOURLY,
        FrequencyKey.THREE_HOURLY,
        FrequencyKey.SIX_HOURLY,
        FrequencyKey.TWELVE_HOURLY,
        FrequencyKey.DAILY_NOON,
    ):
        return (
            opa.time.first_timestamp.strftime("%Y_%m_%d_T%H")
            + "_to_"
            + opa.time.init_timestamp.strftime("%Y_%m_%d_T%H")
        )

    if time_word in (FrequencyKey.DAILY, FrequencyKey.WEEKLY):
        return (
            opa.time.first_timestamp.strftime("%Y_%m_%d")
            + "_to_"
            + opa.time.init_timestamp.strftime("%Y_%m_%d")
        )

    if time_word in (
        FrequencyKey.MONTHLY,
        FrequencyKey.THREE_MONTHLY,
    ):
        return (
            opa.time.first_timestamp.strftime("%Y_%m")
            + "_to_"
            + opa.time.init_timestamp.strftime("%Y_%m")
        )

    if time_word == FrequencyKey.YEARLY or opa.request.stat == FrequencyKey.TEN_YEARLY:
        return (
            opa.time.first_timestamp.strftime("%Y")
            + "_to_"
            + opa.time.init_timestamp.strftime("%Y")
        )

    raise RuntimeError("Cannot create filename.")


def _create_filename_single_point(opa: "Opa", time_word: FrequencyKey):
    """Create filename for a single statistic (no appending).

    Parameters
    ----------
    opa : Opa
        OPA instance with time and request information.
    time_word : FrequencyKey
        Temporal frequency determining the filename format.

    Returns
    -------
    str
        Filename representing the single statistic timestamp.

    Raises
    ------
    RuntimeError
        If an unsupported time_word is provided.
    """
    if time_word in (
        FrequencyKey.HOURLY,
        FrequencyKey.TWO_HOURLY,
        FrequencyKey.THREE_HOURLY,
        FrequencyKey.SIX_HOURLY,
        FrequencyKey.TWELVE_HOURLY,
        FrequencyKey.DAILY_NOON,
    ):
        return opa.time.init_timestamp.strftime("%Y_%m_%d_T%H")

    if time_word == FrequencyKey.DAILY:
        return opa.time.init_timestamp.strftime("%Y_%m_%d")

    if time_word == FrequencyKey.WEEKLY:
        return opa.time.init_timestamp.strftime("%Y_%m_%d")

    if time_word in (
        FrequencyKey.MONTHLY,
        FrequencyKey.THREE_MONTHLY,
    ):
        return opa.time.init_timestamp.strftime("%Y_%m")

    if time_word == FrequencyKey.YEARLY or opa.request.stat == FrequencyKey.TEN_YEARLY:
        return opa.time.init_timestamp.strftime("%Y")

    raise RuntimeError("Cannot create filename.")


def create_file_name(opa: "Opa", time_word: Optional[str] = None) -> str:
    """Create the final filename for a NetCDF output file.

    Generates a descriptive filename based on the statistic period and whether
    multiple statistics are being appended into one file.

    Parameters
    ----------
    opa : Opa
        OPA instance containing time and request configuration.
    time_word : FrequencyKey or None, optional
        Frequency to use for filename generation. Used for continuous statistics
        where stat_freq is continuous but output_freq determines the output period.
        If None, uses opa.request.stat_freq.

    Returns
    -------
    str
        Timestamp-based filename string.

    Notes
    -----
    For continuous statistics (stat_freq='continuous'), the output_freq should be
    passed as time_word to generate appropriate filenames.
    """

    # used for naming continuous data
    if time_word is None:
        time_word = opa.request.stat_freq

    # Filename of a span if we have more than one statistic (_to_)
    is_span = opa.time.num_statistics > 1

    if is_span:
        return _create_filename_span(opa, time_word)

    return _create_filename_single_point(opa, time_word)
