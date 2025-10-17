"""Validation functions for OPA request configuration.

This module ensures request dictionaries contain all required fields with
valid values and appropriate combinations for the requested statistics.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import numpy as np

from one_pass.options import (
    StatKey,
    FrequencyKey,
)

if TYPE_CHECKING:
    from one_pass.request import Request


def raise_key_error_freq_mix(output_freq, stat_freq):
    """Raise ValueError for invalid stat_freq and output_freq combinations.

    Parameters
    ----------
    output_freq : FrequencyKey
        Requested output frequency.
    stat_freq : FrequencyKey
        Requested statistic frequency.

    Raises
    ------
    ValueError
        Always raised to indicate incompatible frequency combination.
    """
    raise ValueError(
        f"Cannot set output_freq equal to '{output_freq.value}' if stat_freq "
        f"is equal to '{stat_freq.value}'. Output_freq must always be greater "
        "than or the same as stat_freq."
    )


def check_output_frequency(request):
    """Check that output frequency is valid and not CONTINUOUS.

    Parameters
    ----------
    request : Request
        Request object containing output frequency configuration

    Raises
    ------
    ValueError
        If output frequency is set to CONTINUOUS
    """
    if request.output_freq == FrequencyKey.CONTINUOUS:
        raise ValueError(
            f"Output frequency cannot have the value of '{FrequencyKey.CONTINUOUS}'."
        )


def mix_of_stat_and_output_freq(output_freq, stat_freq, request, logger):
    """Validate compatibility of stat_freq and output_freq combination.

    Checks that output_freq is not smaller than stat_freq and warns about
    potentially problematic combinations.

    Parameters
    ----------
    output_freq : FrequencyKey
        Requested output frequency.
    stat_freq : FrequencyKey
        Requested statistic frequency.
    request : Request
        Full request object (may be modified in place).
    logger : Logger
        Logger for warnings.

    Raises
    ------
    ValueError
        If output_freq is smaller than stat_freq or other invalid combinations.
    """
    output_freq_weekly_options = [
        FrequencyKey.WEEKLY,
        FrequencyKey.MONTHLY,
        FrequencyKey.THREE_MONTHLY,
        FrequencyKey.ANNUALLY,
        FrequencyKey.TEN_ANNUALLY,
    ]

    # TODO: Improve logic
    stat_index = list(FrequencyKey).index(stat_freq)
    output_index = list(FrequencyKey).index(output_freq)

    if output_index > stat_index + 3:
        if stat_freq == FrequencyKey.HOURLY and output_freq == FrequencyKey.DAILY:
            pass
        else:
            logger.warning(
                f"Your set value of output_freq : {output_freq.value} "
                f"is significantly larger than your set value of stat_freq : "
                f"{stat_freq.value}. This will result in a final file size with a large "
                f"time dimension. We recommend you reduce your value of output_freq. "
            )

    if output_freq == FrequencyKey.CONTINUOUS and FrequencyKey.CONTINUOUS:
        raise_key_error_freq_mix(output_freq, stat_freq)

    elif stat_freq == FrequencyKey.WEEKLY:
        if output_freq not in output_freq_weekly_options:
            raise_key_error_freq_mix(output_freq, stat_freq)

    if stat_freq == FrequencyKey.DAILY_NOON:
        if output_freq == FrequencyKey.DAILY:
            request.output_freq = request.stat_freq
            setattr(request, output_freq, stat_freq)
            logger.warning(
                'Changed output_freq from "daily" to "daily_noon" '
                " as stat_freq is set to daily_noon",
            )

    if stat_freq != FrequencyKey.CONTINUOUS:
        for j in range(stat_index):
            # test that output_freq is always the same or greater than stat_freq
            if output_freq in list(FrequencyKey)[0:j]:
                raise_key_error_freq_mix(output_freq, stat_freq)


def check_thresh_exceed(request: Request, logger):
    """Validate threshold exceedance configuration.

    Ensures thresh_exceed is properly specified when stat='thresh_exceed'.

    Parameters
    ----------
    request : Request
        Request object to validate (may be modified in place).
    logger : Logger
        Logger for warnings.

    Raises
    ------
    AttributeError
        If thresh_exceed stat is requested but thresh_exceed values not provided.
    ValueError
        If thresh_exceed has invalid type or empty values.
    TypeError
        If thresh_exceed is not a list, int, or float.
    """

    if request.stat != StatKey.THRESH_EXCEED:
        return

    thresh_exceed = request.thresh_exceed
    if thresh_exceed is None:
        raise AttributeError(
            "For the thresh_exceed statistic you need to provide "
            "the threshold value. This is set in a seperate key : value "
            "pair, 'thresh_exceed' : [some_value_1, some_value_2], "
            "where this is a list of exceedance values. This can be a "
            "single value."
        )
    if isinstance(thresh_exceed, (int, float)):
        request.thresh_exceed = [thresh_exceed]
        logger.warning(
            "Values for threshold exceedance should be passed as a list. "
            f"The single value {thresh_exceed} has been converted "
            f"to the form [{thresh_exceed}]. To avoid this warning "
            "pass as a list."
        )
        return
    if isinstance(thresh_exceed, list):
        if not all(isinstance(element, (int, float)) for element in thresh_exceed):
            raise ValueError(
                f"thresh_exceed {thresh_exceed} contains invalid values. "
                "Expected a list of ints or floats."
            )
        if len(thresh_exceed) == 0:
            raise ValueError(
                "thresh_exceed was empty. Please, provide a list with at least a value."
            )
        return
    raise TypeError(
        f"The value for the threshold exceedance has an invalid type: "
        f"{type(thresh_exceed).__name__}."
    )


def check_histogram(request, logger):
    """Validate histogram configuration and set defaults.

    Checks histogram-specific parameters and warns about default bin count.

    Parameters
    ----------
    request : Request
        Request object to validate (may be modified in place).
    logger : Logger
        Logger for warnings.
    """
    if request.stat == StatKey.HISTOGRAM:
        if request.bins is None:
            logger.warning(
                "Optional key value 'bins' : int or array_like. "
                "If 'bins' is an int, it defines the number of equal width bins in "
                "the given range. If 'bins' is an array_like, the values define "
                "the edges of the bins (rightmost edge inclusive), allowing for "
                "non-uniform bin widths. If set to 'None', or not included at all "
                "it will default to 10."
            )
            request.bins = 10


def _default_percentile(request, logger):
    """Function that issues sets default value
    for the percentile list option, issuing a warning.
    """

    request.percentile_list = list(np.linspace(0, 99, 100) / 100)
    logger.warning(
        "percentile_list not passed or empty. This defaults to "
        "the whole distribution, i.e. [0.00, 0.02, 0.03, ..., 0.99]. "
        "For other distributions, pass as a list, e.g. "
        '"percentile_list: [0.25, 0.5, 0.75]".'
    )


def check_percentile(request, logger):
    """Validate percentile configuration and set defaults.

    Ensures percentile_list is properly specified or sets default values.

    Parameters
    ----------
    request : Request
        Request object to validate (may be modified in place).
    logger : Logger
        Logger for warnings.

    Raises
    ------
    ValueError
        If percentile_list contains invalid values (not in [0, 1] range).
    """
    if request.stat != StatKey.PERCENTILE:
        return

    # Get request's percentile_list property
    percentile_list = request.percentile_list

    # If not passed, default with warning
    if percentile_list is None:
        _default_percentile(request, logger)
        return

    if isinstance(percentile_list, list):
        if len(percentile_list) == 0:
            _default_percentile(request, logger)
            return
        if not all(
            isinstance(element, (int, float)) for element in percentile_list
        ) or any(element > 1 or element < 0 for element in percentile_list):
            raise ValueError(
                f"percentile_list {percentile_list} contains invalid "
                "values. Values must be in a list and range between 0 and 1. "
                "For the whole distribution, put []"
            )
        return
    raise ValueError(
        f"percentile_list {percentile_list} not a valid "
        "value. Values must be in a list and range between 0 and 1. "
        "For the whole distribution, put []"
    )


def check_iams(request):
    """Validate IAMS statistic configuration.

    IAMS requires both stat_freq and output_freq to be 'yearly'.

    Parameters
    ----------
    request : Request
        Request object to validate.

    Raises
    ------
    ValueError
        If stat_freq or output_freq is not 'yearly' for IAMS statistic.
    """
    if request.stat == StatKey.IAMS:
        if request.stat_freq != FrequencyKey.YEARLY:
            raise ValueError(
                "Must set stat_freq equal to yearly when requesting iams statistic"
            )
        if request.output_freq != FrequencyKey.YEARLY:
            raise ValueError(
                "Must set output_freq equal to yearly when requesting iams statistic"
            )


def check_raw(request, logger):
    """Check that if raw has been selected as the statistic then the user knows that
    the opa will not alter the time dimension and the variables "stat_freq" and
    "output_freq" will be ignored.
    """
    if request.stat == StatKey.RAW:
        logger.info(
            "You have selected raw data. The opa will save the raw data that it is "
            "passed, it will not alter the time dimension. The key value pairs "
            "'stat_freq' and 'output_freq' (if present) will be "
            "ignored in this request."
        )


def check_deprecation(request, logger):
    """Check if the input frequency is annually, will now change this to yearly
    and flag Future depcripiation warning.
    """
    if request.output_freq == FrequencyKey.ANNUALLY:
        request.output_freq = FrequencyKey.YEARLY
        logger.warning(
            "The time request of 'annually' for output_freq will be "
            "deprecated in future versions of the Opa. Please use the request "
            "'yearly' instead."
        )

    if request.stat_freq == FrequencyKey.ANNUALLY:
        request.stat_freq = FrequencyKey.YEARLY
        logger.warning(
            "The time request of 'annually' for stat_freq will be "
            "deprecated in future versions of the Opa. Please use the request "
            "'yearly' instead."
        )


def check_compression(request, logger):
    """Check whether cmpression has been passed, and defaults it to 1 in the case of
    the stat being "histogram" or "percentile".
    """
    if request.stat in ("histogram", "percentile"):
        compression = request.compression
        if compression is None:
            logger.warning(
                "For 'histogram' or 'percentile' stats, not passing a compression "
                "results in a default compression of 1."
            )
        elif not isinstance(compression, (int, float)):
            raise TypeError(
                f"In request, compression should be a float, not {type(compression).__name__}"
            )


def check_legacy_bias_adjustment(request: "Request", logger):
    """For older ways to call bias adjustment, check that if bias adjustment
    has been selected, the correct values have been set.
    The stat must be equal to raw and an appropriate bias adjustment
    method must have been set.
    """
    if request.stat == StatKey.BIAS_CORRECTION:
        request.bias_adjust = True
        logger.warning(
            "request['stat'] = 'bias_correction' is now deprecated and will soon be removed."
            "Please, consider using request['bias_adjust'] = True."
        )
    if request.bias_adjustment is True or request.bias_adjustment in {"True", "true"}:
        request.bias_adjust = True
        logger.warning(
            "request['bias_adjustment'] is now a deprecated key and will soon be removed."
            "Please, consider using request['bias_adjust'] = True."
        )
    method = request.bias_adjustment_method
    if method is not None:
        request.ba_future_method = method.value
        logger.warning(
            "request['bias_adjustment_method'] is now a deprecated key and will "
            "soon be removed. Please, consider using request['ba_agg_method']."
        )


def check_bias_adjustment(request):
    """Check that if bias adjustment has been selected, the correct values have been set.
    The stat must be equal to raw and an appropriate bias adjustment
    method must have been set.
    """
    if not request.bias_adjust:
        return
    if request.ba_reference_dir is None:
        raise ValueError("When bias adjusting, ba_reference_dir must be set.")
    if request.ba_agg_method not in ("sum", "mean"):
        raise ValueError("When bias adjusting, ba_reference_dir must be set.")
    if request.ba_future_method not in ("additive", "multiplicative"):
        raise ValueError("When bias adjusting, ba_reference_dir must be set.")
    try:
        date.fromisoformat(request.ba_future_start_date)
    except ValueError as exc:
        raise ValueError(
            f"request.ba_future_start_date ({request.ba_future_start_date}) "
            "is not a valid date in YYYY-MM-DD format."
        ) from exc
    if request.ba_reference_dir is None:
        raise ValueError("When bias adjusting, ba_reference_dir must be set.")


def check_request(request, logger):
    """Validate complete OPA request configuration.

    Main validation entry point that runs all validation checks on the request.

    Parameters
    ----------
    request : Request
        Request object to validate (may be modified in place).
    logger : Logger
        Logger for warnings and informational messages.

    Raises
    ------
    ValueError
        For invalid frequency combinations, missing required fields, or
        incompatible parameter values.
    AttributeError
        For missing required attributes for specific statistics.
    TypeError
        For incorrect parameter types.
    """
    # TODO: Specify what errors it can raise in docstring.

    if request.stat != StatKey.RAW:
        mix_of_stat_and_output_freq(
            request.output_freq, request.stat_freq, request, logger
        )

    # check requirements for specific statistics
    check_output_frequency(request)
    check_deprecation(request, logger)
    check_thresh_exceed(request, logger)
    check_histogram(request, logger)
    check_percentile(request, logger)
    check_iams(request)
    check_raw(request, logger)
    check_compression(request, logger)
    check_legacy_bias_adjustment(request, logger)
    check_bias_adjustment(request)
