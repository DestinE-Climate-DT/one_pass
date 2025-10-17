"""
Options and enumerations for OPA request configuration.

This module defines the available statistics and frequency options that can be
specified in an OPA request configuration. All options are provided as enumerations
to ensure type safety and provide clear documentation of available choices.
"""

from enum import Enum

# IDEA: Bring to one_pass/request/options.py,
# and have in that dir also request.py and validation.py?
# NOTE: StrEnum exists in Python3.11


class _EnumValuesMixin:  # pylint: disable=R0903
    """Mixin class providing utility methods for enumerations."""

    @classmethod
    def values(cls):
        """List all available enumeration values as strings.

        Returns
        -------
        tuple of str
            Tuple containing all enum values as strings.
        """
        return tuple(element.value for element in cls)


class StatKey(str, _EnumValuesMixin, Enum):
    """Available statistics for OPA computation.

    This enumeration defines all supported statistical operations that can be
    requested in the 'stat' field of an OPA request.

    Attributes
    ----------
    MEAN : str
        Arithmetic mean
    STD : str
        Standard deviation (sample)
    VAR : str
        Variance (sample)
    THRESH_EXCEED : str
        Count of threshold exceedances
    MIN : str
        Minimum value (with timestamp tracking)
    MAX : str
        Maximum value (with timestamp tracking)
    PERCENTILE : str
        Percentiles using TDigest algorithm
    HISTOGRAM : str
        Histogram using TDigest algorithm
    RAW : str
        Raw data passthrough (no aggregation)
    BIAS_CORRECTION : str
        (Deprecated) Use bias_adjust parameter instead
    SUM : str
        Sum of values
    IAMS : str
        Incremental Annual Maximum Statistic
    """

    MEAN = "mean"
    STD = "std"
    VAR = "var"
    THRESH_EXCEED = "thresh_exceed"
    MIN = "min"
    MAX = "max"
    PERCENTILE = "percentile"
    HISTOGRAM = "histogram"
    RAW = "raw"
    BIAS_CORRECTION = "bias_correction"
    SUM = "sum"
    IAMS = "iams"


class FrequencyKey(str, _EnumValuesMixin, Enum):
    """Available temporal frequencies for statistics and output.

    This enumeration defines all supported temporal frequencies for the 'stat_freq'
    and 'output_freq' fields in OPA requests.

    Attributes
    ----------
    HALF_HOURLY : str
        30-minute intervals
    HOURLY : str
        1-hour intervals
    TWO_HOURLY : str
        2-hour intervals
    THREE_HOURLY : str
        3-hour intervals
    SIX_HOURLY : str
        6-hour intervals
    TWELVE_HOURLY : str
        12-hour intervals
    DAILY : str
        Daily aggregation starting at midnight
    DAILY_NOON : str
        Daily aggregation starting at noon
    WEEKLY : str
        Weekly aggregation (Monday to Sunday)
    MONTHLY : str
        Monthly aggregation
    THREE_MONTHLY : str
        Quarterly aggregation
    ANNUALLY : str
        (Deprecated) Use YEARLY instead
    YEARLY : str
        Annual aggregation
    TEN_ANNUALLY : str
        (Deprecated) Use TEN_YEARLY instead
    TEN_YEARLY : str
        Decadal aggregation
    CONTINUOUS : str
        Continuous rolling aggregation (valid only for stat_freq)
    """

    HALF_HOURLY = "half_hourly"
    HOURLY = "hourly"
    TWO_HOURLY = "2hourly"
    THREE_HOURLY = "3hourly"
    SIX_HOURLY = "6hourly"
    TWELVE_HOURLY = "12hourly"
    DAILY = "daily"
    DAILY_NOON = "daily_noon"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    THREE_MONTHLY = "3monthly"
    ANNUALLY = "annually"
    YEARLY = "yearly"
    TEN_ANNUALLY = "10annually"
    TEN_YEARLY = "10yearly"
    CONTINUOUS = "continuous"
