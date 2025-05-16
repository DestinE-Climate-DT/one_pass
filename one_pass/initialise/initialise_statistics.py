""" Initalise statistic attributes for the OPA class"""
from typing import List
import logging

import numpy as np
import dask.array as da
import xarray as xr
from crick import TDigest
import tqdm

class OpaStatistics:
    """Class that contains all of the cumulative statistics. Everything
    in this class will be large in memory.
    """
    def __init__(self, opa_self : object):
        """"Init of nested class statistics. This nested class contains
        all of the heavy numpy array that contain the rolling statistics.
        All of the attributes finish with "cum" short for cumulative.
        
        self.STAT_cum : np.ndarray containing the rolling stat for every
                statistic. STAT is set by the user request.
        self.final_cum : this is the final xr.Dataset containing the finished
                statistic. This is only added when the statistic is full
                (count = n_data)
        self.digests_cum : this is created for all statistics that require
                distributions. For percentiles and histograms the STAT_cum
                is only created when count == n_data, until then, we carry
                through these t-digest objects containing the information about
                the distrubtions.
        self.histogram_bin_edges_cum : This is the second final stat for histograms
        that contains the bin edges
        """
        if opa_self.request.stat != "bias_correction":
            setattr(self, opa_self.request.stat + "_cum", None)
        self.final_cum : xr.Dataset = None

        # for the standard deviation need both the mean and variance throughout
        # only create std_cum at the end when count = n_data and remove it again
        # to avoid checkpointing another value uncessarily
        if opa_self.request.stat in ("var", "std"):
            self.mean_cum : np.ndarray[float] = None
            if opa_self.request.stat == "std":
                self.var_cum : np.ndarray[float] = None

        # for min and max we also save the timings that correspond
        # to the mimumum and maximum values.
        elif opa_self.request.stat in ("min", "max"):
            self.timings_cum : np.ndarray[int] = None

        if opa_self.request.stat in (
                "histogram", "percentiles", "bias_correction"
            ):
            self.digests_cum : np.ndarray[object] = None

        if opa_self.request.stat == "histogram":
            self.histogram_bin_edges_cum : np.ndarray[float] = None
            self.final2_cum : xr.Dataset = None

        if opa_self.request.stat == "bias_correction":
            # dpening on the variable we either want daily aggregations
            # as mean values or as summuation values.
            if opa_self.request.variable not in opa_self.fixed.precip_options:
                self.mean_cum : np.ndarray[float] = None
            else:
                self.sum_cum : np.ndarray[float] = None

    def _duration_pick(self, opa_self : object, durations : List[int]):
        """ Function to remove any duration window
        that is lower than the time step

        Arguments
        ---------
        opa_self : Opa class
        duruations : list of requested durations

        Returns
        ---------
        durations : modified list of durations without ones
                that are too short
        """
        # loop over duration windows that fall in the range of the time step
        durations = [d for d in durations if d >= opa_self.request.time_step]

        # only select whole numbers
        for d in durations:
            if d%opa_self.request.time_step != 0:
                durations.remove(d)

        # creates integer list
        durations = list(map(int, durations))

        return durations

    def _init_ndata_durations(self, opa_self : object, value : np.ndarray):
        """This function picks the longest duration window
        and create a zero filled array with one dimension equal to the length
        of the maximum duration divided by the time step

        Attributes
        ----------
        opa_self : Opa class data
        value : np.ndarray. zero filled array of shape data_source but with length
                1 in the time dimension

        Returns
        ----------
        opa_self.iams.n_data_duration : number of time steps for each duration that
                need to be stored
        opa_self.iams.rolling_data : empty array to store the maximum value of
                n_data_duration
        opa_self.iams.count_duration : count to know how far through the rolling
                window the statistic is
        """
        n_data_duration = np.zeros(np.size(opa_self.iams.durations))
        # loop to calculate n data for each duration period
        for i in range(np.size(opa_self.iams.durations)):
            # calculate n_data_duration for each duration period
            n_data_duration[i] = int(
                (opa_self.iams.durations[i])/opa_self.request.time_step
            )

        # set attribute containing list all n_data_durations
        opa_self.iams.n_data_durations = n_data_duration
            # set count at 0 for each duration, each count in a list
        opa_self.iams.count_durations = np.zeros(np.size(opa_self.iams.durations))
        # setting count_durations_full = 0, this is do count + full_length
        opa_self.iams.count_durations_full = np.zeros(np.size(opa_self.iams.durations))

        # create empty array to store the maximum number of time steps we need
        # using the last (max) value of n_data_duration
        new_shape = (int(n_data_duration[-1]), np.shape(value)[1:])
        # unpack the nested tuple
        new_shape_whole = (new_shape[0], *new_shape[1])
        opa_self.iams.rolling_data = np.zeros(new_shape_whole)

    def init_digests(self, opa_self : object):
        """Function to initalise a flat array full of empty 
        tDigest objects. Global function.

        Arguments 
        ----------
        opa_self.data_set_info.size_data_source_tail : size of incoming data

        Returns
        ---------
        opa_self.digests_cum : a flat array of of the size of data_source_tail
                full of empty t digest objects with compression = 1
        """
        # list of dictionaries for each grid cell, preserves order
        digest_list = [{} for _ in range(
                opa_self.data_set_info.size_data_source_tail
            )]
        # converts the list into a numpy array which helps with re-sizing time
        digest_list = np.reshape(
                digest_list, opa_self.data_set_info.size_data_source_tail
            )

        # Read the compression from request. If it wasn't passed, set to 1
        compression = opa_self.request.compression
        if compression is None:
            compression = 1

        if opa_self.logger.isEnabledFor(logging.DEBUG):
            for j in tqdm.tqdm(
                    range(opa_self.data_set_info.size_data_source_tail),
                    desc="Initialising digests"
                ):
                digest_list[j] = TDigest(compression=compression)

        else:
        # this is looping through every grid cell
            for j in range(opa_self.data_set_info.size_data_source_tail):
                digest_list[j] = TDigest(compression=compression)

        setattr(self, "digests_cum", digest_list)

    def reset_all_rolling_stats(self, remove_final : bool = True):
        """Resets all of the attributes in the class opa_self.statistics
        to None, as they are no longer needed.

        Attributes
        ------------
        removal_final : bool. If this is true, as default it will also
                remove the final xr.Dataset "final_cum". If false, it
                will leave "final_cum" (and "final2_cum") as these are
                the appended xr.Datasets and are needed in the next
                calls
        """
        if remove_final:
            for element in self.__dict__.items():
                setattr(self, element[0], None)
        else:
            for element in self.__dict__.items():
                if element[0] != "final_cum":
                    if element[0] != "final2_cum":
                        setattr(self, element[0], None)

    def _initialise_simple_attrs(self, opa_self : object, value : np.ndarray):
        """Initialise data structures for 'simple' cumulative stats
        that just require the flatted array shape. For the standard
        deviation need both the mean and variance throughout only
        create std_cum at the end when count = n_data and remove it
        again to avoid checkpointing another value uncessarily
        """
        if opa_self.request.stat in ("var", "std"):
            setattr(self, "mean_cum", value)
            setattr(self, "var_cum", value)

        elif opa_self.request.stat in ("min", "max"):
            setattr(self, opa_self.request.stat + "_cum", value)
            setattr(self, "timings_cum", value)
        else:
            setattr(self, opa_self.request.stat + "_cum", value)

    def initialise_attrs(self, opa_self : object, data_source : xr.DataArray):
        """Initialises data structures for cumulative stats

        Arguments
        ---------
        opa_self : Opa Class data
        data_source : incoming data
        opa_self.fixed.precip_options : list of variable names corresponding 
                to precip that require summations for the bias correction
                as opposed to the mean

        Returns:
        --------
        self.STAT_cum : zero filled data array with the shape of
                the incoming data compressed to 1 in the time dimension
                For some statistics it may intialise more than one zero filled
                array as some stats require the storage of more than one stat
        opa_self.data_set_info.shape_data_source_tail : storing the shape of the
                compressed incoming data.
        opa_self.data_set_info.size_data_source_tail : storing the size of the
                compressed incoming data

        Maybe Returns:
        -------
        opa_self.count_continuous : like opa_self.count, counts the number of
                pieces of data seen but never gets reset
        """
        data_source_tail = data_source.tail(time=1)
        opa_self.data_set_info.shape_data_source_tail = np.shape(data_source_tail)
        opa_self.data_set_info.size_data_source_tail = np.size(data_source_tail)

        # forcing computation in float64, empty numpy array
        value = np.zeros(
                opa_self.data_set_info.shape_data_source_tail, dtype=np.float64
            )

        if opa_self.request.stat_freq == "continuous":
            opa_self.time.count_continuous = 0
            opa_self.time.init_count_time_stamp = opa_self.time.time_stamp

        if opa_self.request.stat not in ("bias_correction",
                        "percentile",
                        "histogram",
                        "iams",
                        "thresh_exceed"):

            self._initialise_simple_attrs(opa_self, value)

        elif opa_self.request.stat == "thresh_exceed":
            new_shape = (np.shape(value)[0],
                            len(opa_self.request.thresh_exceed),
                            np.shape(value)[1:]
                        )
            new_shape_whole = (new_shape[0], new_shape[1], *new_shape[2])
            if hasattr(data_source, "compute"):
                opa_self.new_shape_whole = new_shape_whole
                thresh_value = da.zeros(new_shape_whole)
            else:
                thresh_value = np.zeros(new_shape_whole)

            setattr(self, opa_self.request.stat + "_cum", thresh_value)

        elif opa_self.request.stat == "iams":

            # list all of all possible durations in minutes for iams stat
            durations = (5,10,15,20,30,45,60,90,120,180,240,
                360,540,720,1080,1440,2880,4320,5760,7200,8640,10080)
            # removing durations smaller than time step and not full
            # multiples
            opa_self.iams.durations = self._duration_pick(opa_self, durations)
            # creating array to hold the max value for each duration window
            new_shape = (np.size(opa_self.iams.durations), np.shape(value)[1:])
            # unpack the nested tuple
            new_shape_whole = (new_shape[0], *new_shape[1])
            durations_value = np.zeros(new_shape_whole)

            # opa_self.iams_cum has first dimension size of durations
            setattr(self, opa_self.request.stat + "_cum", durations_value)

            self._init_ndata_durations(opa_self, value)

        elif opa_self.request.stat in ("percentile", "histogram"):
            self.init_digests(opa_self)

        elif opa_self.request.stat == "bias_correction":
            # not ititalising the digests here, as only need to update
            # them with daily means, would create unncessary I/O at
            # checkpointing also going to need the daily means or sums
            # if precipitation
            if opa_self.request.variable not in opa_self.fixed.precip_options:
                setattr(self, "mean_cum", value)
            else:
                setattr(self, "sum_cum", value)
