"""Time management class for tracking OPA computation progress.

This module provides the OpaTime class which manages timestamps, counters,
and temporal state throughout the statistical computation process.
"""

import math
from typing import Optional, TYPE_CHECKING

import numpy as np
from pandas import Timestamp

from one_pass.options import FrequencyKey
from .conversion import (
    get_minutes_into_word,
    convert_word_to_minutes,
)

if TYPE_CHECKING:
    from one_pass.opa import Opa


class OpaTime:  # pylint: disable=R0902
    """Manager for time-related attributes in OPA computation.

    This class tracks the progress through statistic computation, manages
    timestamps, and handles time-based validation and state management.
    """

    def __init__(self, opa: "Opa"):
        """Initialize the OpaTime instance.

        Sets up time tracking attributes for the OPA computation.

        Parameters
        ----------
        opa : Opa
            Reference to the parent OPA instance.

        Attributes
        ----------
        count : int
            Main counter tracking how many time slices have been processed.
        n_data : int
            Number of time steps required to complete the statistic.
            Statistic is 'full' when count == n_data.
        init_timestamp : Timestamp
            Timestamp of the first piece of data for the current statistic.
        last_timestamp : Timestamp
            Timestamp of the most recently processed data.
        stat_freq_minutes : int
            Duration of the statistic frequency in minutes.
        count_continuous : int
            Counter for continuous statistics that doesn't reset between outputs.
        initialized : bool
            Flag indicating whether time attributes have been initialized.
        finalized : bool
            Flag indicating whether the current statistic period is complete.
        """
        # initialized | finalized |
        #       False |     False | OpaTime never initialized
        #        True |     False | OpaTime mid-aggregation
        #       False |      True | Previous aggregation finalized, OpaTime.reset() called
        #        True |      True | Current aggregation finalized,
        #                             i.e. OpaTime.reset() not yet called

        self.opa = opa
        self.count: int = None
        self.n_data: int = None
        self.init_timestamp: Optional[Timestamp] = None
        self.last_timestamp: Optional[Timestamp] = None
        self.count_continuous: int = 0
        self.init_count_timestamp: Optional[Timestamp] = None
        self._stat_freq_minutes = None
        self.initialized = False
        self.finalized = False
        self.cur_statistics: int = 0
        self.num_statistics: int = 0
        self.filename: str = ""
        self.first_timestamp: Optional[Timestamp] = None
        self.continuous = opa.request.stat_freq == FrequencyKey.CONTINUOUS
        self.timestep = opa.request.time_step

    @property
    def first_year(self) -> int:
        """Return the first year of the time series."""
        if self.init_timestamp is None:
            return None
        return self.init_timestamp.year

    def _increase(self, weight: float):
        """Increment time counters by the given weight.

        Parameters
        ----------
        weight : float
            Number of time steps to add to counters.

        Returns
        -------
        int
            The updated count (count_continuous for continuous stats, count otherwise).
        """
        self.count += weight
        if self.opa.request.stat_freq == FrequencyKey.CONTINUOUS:
            self.count_continuous += weight
            return self.count_continuous
        return self.count

    @property
    def stat_freq_minutes(self):
        """Statistical frequency in minutes.

        Returns
        -------
        int
            Duration of statistic frequency in minutes.

        Raises
        ------
        ValueError
            If OpaTime has not been initialized yet. This property is unavailable
            before initialization because some frequencies (monthly, yearly) have
            variable durations depending on the specific timestamp.
        """
        if self._stat_freq_minutes is None:
            raise ValueError(
                "OpaTime has not been initialized yet. "
                "stat_freq_minutes is not yet available because, certain frequencies "
                "vary number of minutes, e.g. 'MONTHLY' depends on the month, "
                "and 'YEARLY' depends on whether it is a leap year."
            )
        return self._stat_freq_minutes

    @property
    def timestamp(self):
        """(No longer available)"""
        raise AttributeError(
            "OpaTime.last_timestamp has been renamed to OpaTime.last_timestamp"
        )

    @stat_freq_minutes.setter
    def stat_freq_minutes(self, value):
        if self._stat_freq_minutes is not None:
            raise ValueError("Can only set stat_freq_minutes once.")
        self._stat_freq_minutes = value

    def _done(self):
        """Check if all computation is complete.

        Returns
        -------
        bool
            True if all statistics have been computed and output, False otherwise.
        """
        if self.continuous:
            return self.n_data == self.count
        return self.n_data == self.count and self.num_statistics == self.cur_statistics

    def _initialize_multiple(self, timestamp):
        """Initialize time tracking for multiple appended statistics.

        Sets up tracking for cases where output_freq > stat_freq, requiring
        multiple statistics to be accumulated before output.

        Parameters
        ----------
        timestamp : pd.Timestamp
            Initial timestamp for the computation.

        Raises
        ------
        ValueError
            If output_freq is less than stat_freq for non-continuous statistics.
        """
        opa = self.opa
        stat_freq = opa.request.stat_freq
        output_freq = opa.request.output_freq

        opa.time.first_timestamp = timestamp

        # converting output freq into the number of minutes
        output_freq_minutes = convert_word_to_minutes(
            time_word=output_freq,
            timestamp=timestamp,
        )

        # How many stats you need in the final xr.Dataset if output_freq
        # is larger than stat_freq. If you're starting part way through
        # an output_freq, timestamp_tot_append will be non-zero.
        # TODO: Instead, dynamically check whether we are
        #       "arriving to end of OUTPUT period"
        self.num_statistics = int(
            np.round((output_freq_minutes) / self.stat_freq_minutes)
        )
        # output_freq_min < self.time.stat_freq_min
        if opa.time.num_statistics < 1 and stat_freq != FrequencyKey.CONTINUOUS:
            raise ValueError(
                "Output frequency can not be less than frequency of statistic"
            )

    def initialize(
        self,
        timestamp: Timestamp,
    ):
        """Initialize time tracking attributes for a new statistic period.

        Sets up all counters, timestamps, and frequency calculations needed
        to track progress through the current statistic computation.

        Parameters
        ----------
        timestamp : pd.Timestamp
            Timestamp of the first data point in this statistic period.

        Notes
        -----
        This method initializes:
        - n_data : Number of time steps needed to complete the statistic
        - num_statistics : Number of statistics to append in one output file
        - init_timestamp : First timestamp of the current statistic
        - stat_freq_minutes : Duration of stat_freq in minutes
        - count : Reset to 0

        For continuous statistics, n_data may be adjusted based on when
        the first data arrives within the output frequency period.
        """

        opa = self.opa

        output_freq = opa.request.output_freq
        stat_freq = opa.request.stat_freq
        time_step = opa.request.time_step
        time_word = opa.request.time_word

        self.initialized = True
        self.finalized = False
        self.count = 0
        self.init_timestamp = timestamp
        self.last_timestamp = None
        self.stat_freq_minutes = convert_word_to_minutes(time_word, timestamp)
        self._validate_timestep(opa, timestamp)

        timestamp_minutes = get_minutes_into_word(
            timestamp, time_word, opa.time.first_year
        )
        if self.continuous:
            # if continuous, can start from any point in time so
            # n_data might not span the full output_freq (hence why
            # we minus timestamp_min)
            # this will not work for half hourly output
            self.n_data = math.ceil(
                (self.stat_freq_minutes - timestamp_minutes) / time_step
            )
            return

        # NON-CONTINUOUS STATISTIC

        if timestamp_minutes != 0 and (time_step % timestamp_minutes) != 0:
            # Incoming timestamp misaligned with timestep
            opa.logger.warning("Timings of input data span over new statistic")

        self.n_data = int(self.stat_freq_minutes / time_step)

        if stat_freq == output_freq:
            opa.time.num_statistics = 1
            return

        if opa.time.num_statistics == 0:
            # Initializing an OpaTime for an Opa that will contain multiple aggregations
            # and will output several data points in the same xr.DataSet.
            # This routine should happen only once, i.e. at the very beginning with the very
            # first timestamp.
            self._initialize_multiple(timestamp)

            opa.logger.debug(
                "Incoming time stamp %s corresponds to the start of the output_freq '%s'. "
                "Therefore, initialsing the xr.Dataset with %d statistics "
                "with stat_freq '%s'.",
                str(timestamp),
                output_freq.value,
                opa.time.num_statistics,
                stat_freq.value,
            )

        else:
            opa.logger.debug(
                f"Incoming time stamp {self.last_timestamp} corresponds to middway "
                f"through the {output_freq.value} output_freq. So "
                f"initialsing the xr.Dataset with {opa.time.num_statistics} "
                f"{stat_freq.value} statistics."
            )

    def reset(self):
        """Reset time tracking for the next statistic period.

        Clears temporal state while preserving finalization status and
        overall computation progress (like num_statistics).
        """

        self.finalized = self._done()
        self.initialized = False
        self.count = None
        self.init_timestamp = None
        self.last_timestamp = None
        self._stat_freq_minutes = None

    def update(self, weight: int, timestamp: Timestamp):
        """Update time tracking after processing new data.

        Parameters
        ----------
        weight : int
            Number of time steps that were just processed.
        timestamp : pd.Timestamp
            Timestamp of the last processed data point.
        """

        self._increase(weight)
        self.last_timestamp = timestamp
        self.finalized = self._done()

    def _validate_timestep_int(self, opa: "Opa"):
        """Checks if the time stamp of the data is wholly divisible
        by the time step. For example, if the time stamp was
        1990-01-01-00:15 but the time step was 60, 15/60 is not an
        integer value and this will pass an Exception
        """
        if (self.stat_freq_minutes / opa.request.time_step).is_integer():
            return
        raise ValueError(
            "Frequency of the requested statistic (stat_freq) must "
            "be wholly divisible by the timestep (dt) of the input data."
        )

    def _validate_timestep_size(self, opa: "Opa", timestamp: Timestamp):
        freq_minutes = convert_word_to_minutes(opa.request.time_word, timestamp)
        if freq_minutes < opa.request.time_step:
            raise ValueError(
                "The given time_step in the request is too large for "
                "the requested statistic"
            )

    def _validate_timestep(self, opa: "Opa", timestamp: Timestamp):
        self._validate_timestep_int(opa)
        self._validate_timestep_size(opa, timestamp)
