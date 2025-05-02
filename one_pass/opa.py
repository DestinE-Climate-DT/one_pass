"""Main module for calling one pass functions"""
from typing import Dict, List, Optional
from dataclasses import dataclass

import os
import pickle
import logging
import zarr

import numpy as np
import xarray as xr

from one_pass import util
from one_pass.check_request import check_request
from one_pass.initialise import initialise_statistics
from one_pass.initialise import initialise_time
from one_pass.initialise import time_append
from one_pass.initialise.check_time import check_time_stamp
from one_pass.initialise.check_variable import check_variable
from one_pass.checkpointing.write_checkpoint import write_checkpoint
from one_pass.checkpointing.remove_checkpoints import remove_checkpoints
from one_pass.statistics import bias_correction
from one_pass.statistics.raw_data import check_raw
from one_pass.statistics.raw_data import check_raw_for_bc
from one_pass.statistics.get_final_statistics import get_final_statistics
from one_pass.statistics.update_statistics import update_statistics
from one_pass.saving.create_file_names import create_file_name
from one_pass.saving.create_data_sets import create_data_set
from one_pass.saving.remove_attributes import remove_attributes_continuous
from one_pass.saving.modify_attributes import update_attributes
from one_pass.saving.save_final import save_output_nc

@dataclass
class Fixed:
    """Data class that holds variables that need to remained fixed during
    all the statistic calculations. This will become a nested dataclass
    to the Opa class.

    pickle_limit : (int). This this the limit in GB that can be saved as
            a pickle file. If this statistic rolling summary is larger
            than this it will checkpoint the large data with zarr
    precip_options : List[str]. For the bias correction, if the variable
        corresponds to precipitation you want daily sums as opposed to daily
        means, this list is for all possible precipitation variables names
        that should be summed.
    """
    pickle_limit : int = 3
    precip_options = {
        'pr',
        'lsp',
        'cp',
        'tp',
        'pre',
        'precip',
        'rain',
        'precipitation',
        'precipitationCal'
    }

@dataclass
class Request:
    """
    Initalises all the variables passed from the user request to
    None. The whole request will be passed through check_request
    which will check that the request is valid with the all the required
    variables passed. This will become a nested dataclass to the Opa
    class.

    stat : str. String corresponding to the statistic that the user
            requests.
    time_step : int. Integer value giving the time step between the
            incoming data in minutes. This will most probably be 60
    variable : str. String value giving the variable of interest. If
            the incoming data is passed as an xr.Dataset this will be
            used to extract the variable of interest. Opa can only
            process one variable at a time.
    stat_freq : str. String giving the frequency of the request statistic.
            For example 'daily'.
    output_freq : str. String giving the frequency at which the final
            statistic should be saved into a netCDF. For example
            'monthly'. If stat_freq is set to 'daily' this would result
            in one final .nc file containing 31 daily statistics.
    checkpoint_filepath : str. File path where checkpoint are stored.
    checkpoint : bool. True or False saying if the statistics should
            be saved to a pickle file after each incoming datachunk
            has been passed.
    save : bool. True of False for if the final statistic should be saved
            to disk.
    save_filepath : str. Filepath specifying where the final statistics
            should be saved.
    checkpoint_file : str. Not actually defined in the request but made
            later, attaches checkpoint_filepath with the name of the
            checkpoint file.
    """
    stat : str = None
    time_step : int = None
    variable : str = None
    stat_freq : str = None
    output_freq : str = None
    checkpoint_filepath : str = None
    checkpoint : bool = None
    save : bool = None
    save_filepath : str = None
    checkpoint_file : str = None
    bias_adjust: bool = False          # run bias_adjust on input xr.DataArray
    ba_reference_dir: str = None       # directory where reference tdigest pkl are stored
    ba_lower_threshold: float = -np.inf   # do not apply ba on values beyond lower_threshold
    ba_non_negative: bool = False      # DO WE NEED THIS? correct variable to be non negative
    ba_agg_method: str = "sum"          # method to do daily aggregation (sum or mean)
    ba_future_method: str = "additive"       # method to use for future bias adjustment additive or multiplicative
    ba_future_weight: float = 1.0      # weight to be applied to future values in tdigests - in order to 'wash out' historical t-digest
    ba_future_start_date: str = "9999-12-31"   # when future starts
    ba_detrend: bool = False           # detrend variable
    ba_detrend_skip_years: int = 2     # number of years to not detrend at the beginning

@dataclass
class DataSetInfo:
    """class that stores information about the incoming data

    shape_data_source_tail : tuple. Dimensions of the incoming xr
            data but compressed to one in the two dimension. Used
            for re-sizing statistcs that need to be flattened.
    size_data_source_tail : int. Whole size of the flattened incoming
            xr data. It is the multiplication of shape_data_source_
            tail.
    data_var_attr : dict. Attributes of the incoming data for the
            underlying xr.DataArray corresponding to the variable
            of interest.
    data_set_attr : dict. If the incoming data is a xr.Dataset then
            this stores the attributes of the overall dataset to
            pass back to the final statistic.
    final_time_file_str : str. this is given if the data is appened
            (i.e. output_freq > stat_freq) and corresponds to the
            timestamp of the first piece of data in the final appended
            statistic. Used in the final file name.
    """
    shape_data_source_tail : tuple = None
    size_data_source_tail :int = None
    data_var_attr : dict = None
    data_set_attr: dict = None
    final_time_file_str : str = None

@dataclass
class Iams:
    """This data class will be set as a class attribute to the
    OPA class if the statistic requested is iams. It contains
    the following variables specific to the iams statistic

    durations : List[int]. list of integer values of minutes giving
            the length of the duration windows to be summed
            across.
    n_data_durations : List[int]. list of integer values in units of
            time_step (probably hours). durations / request.time_step.
    count_durations : numpy array of integer values giving the
            the current count of how many n_data_durations we
            have completed. Same shape as n_data_durations
    count_durations_full : numpy array of integer values giving the
            the current count of how many n_data_durations we
            have completed. It will not reset after filling the
            longest duration. Same shape as n_data_durations
    rolling_data : numpy array containing a subset of the
            incoming data source.
    """
    durations : List[int] = None
    n_data_durations : List[int] = None
    count_durations : np.ndarray[int] = None
    count_durations_full : np.ndarray[int] = None
    rolling_data : np.ndarray[np.float64] = 0

class Opa:
    """The main class. This can have up 8 nested dataclasses or
    full classes that contain all the information required. The
    nested classes are listed below.
    """

    def __init__(
        self,
        user_request: Dict,
        keep_checkpoints: bool = False,
        logging_level : Optional[str]="INFO"
    ):

        """
        Initalisation. Will initialise all data from their class
        attributes, then if a checkpoint file exists it will re-load
        the previous state of the Opa class from the pickle checkpoint.

        Attributes:
        ------------
        fixed: dataclass. Defined above as the dataclass Fixed.
        request : dataclass. Contains the user data request. Defined
                above as the dataclass Request.
        data_set_info : dataclass. Contains information about the
                incoming data. Defined above as the dataclass
                DataSetInfo.
        iams : dataclass. Contains variables specfic to the iams
                statistic and is only given as a nested dataclass
                if the statistic requested is iams. Defined above in
                the dataclass Iams.
        time : class. Class that defines all the time attributes
                corresponding to how far through the statistic you
                are. Only passed as a nested class if the statistic
                is not 'raw'. Initalised in the initialise_time from
                the class OpaTime.
        statistic : class. Class that contains all of the rolling
                statistics. These will finished with "_cum" for
                cumulative. Initalised in initialse_statistic as
                OpaStatistc
        append : class. Class that contains all the information required
                if output_freq > time_freq and data needs to be appeneded
                in a final xr.Dataset stored in stat "final_cum".
                Initialised in time_append as Append class.
        bc : class. Class that will only be initialised if the statistic
                is the bias correction. Stores data required for the
                bias correction which requires different outputs from other
                statistics. Initialised as BiasCorrection class in
                bias_correction.
        logger : logger class.
        """
        user_request = util.parse_request(user_request)
        # assign attributes from the request
        self.request = Request()
        self.keep_checkpoints = keep_checkpoints
        self.data_set_info = DataSetInfo()
        self.fixed = Fixed()
        self.logger = self._get_logger(logging_level=logging_level)
        self._process_request(user_request)
        # will check for errors in specified request
        check_request(self.request, self.logger)

        # creating nested classes containg all key values
        if self.request.stat != "raw":
            self.time = initialise_time.OpaTime(self)
            self.statistics = initialise_statistics.OpaStatistics(self)
            self.append = time_append.Append()
            self.bc = bias_correction.BiasCorrection()
            if self.request.stat == "iams":
                self.iams = Iams()

        if self.request.checkpoint and self.request.stat != 'raw':
            # Will check for checkpoint file and load if one
            # exists. will never checkpoint for raw data
            self._check_checkpoint()

    def _load_pickle(self, file_path : str):
        """Function that will load pickled data from a
        checkpoint file

        Arguments
        ----------
        file_path: str. path, including file name, to pickle file

        Returns
        ---------
        Opa class attributes loaded from the pickle file
        """
        with open(file_path, 'rb') as f:
            temp_self = pickle.load(f)
        f.close()

        for key in vars(temp_self):
            setattr(self, key, vars(temp_self)[key])

        del temp_self

    def _check_checkpoint(self):
        """Takes user request and creates the file name of the checkpoint
        file. If the checkpoint file is there it will update the nested
        class with data from the checkpoint

        Returns:
        --------
        self.request.checkpoint_file path : str. Sets full checkpoint file
                path with name of file.
        if checkpoint file is presentOpa (self) class object
        with old attributes
        """
        # already checked if path valid if check request
        file_path = self.request.checkpoint_filepath

        self.request.checkpoint_file = os.path.join(
            file_path,
            f"checkpoint_{self.request.variable}_"
            f"{self.request.stat_freq}_"
            f"timestep_{self.request.time_step}_"
            f"{self.request.output_freq}_"
            f"{self.request.stat}.pkl",
        )

        # see if the checkpoint file exists
        if os.path.exists(self.request.checkpoint_file):
            self._load_pickle(self.request.checkpoint_file)

            # if using a zarr file
            if self.time.using_zarr:
                # looping through all the data that is something_cum
                for element in self.statistics.__dict__.items():
                    if element[0] not in ("final_cum", "final2_cum"):
                        checkpoint_file_zarr = os.path.join(
                            self.request.checkpoint_filepath,
                            f"checkpoint_{self.request.variable}_"
                            f"{self.request.stat_freq}_"
                            f"{self.request.output_freq}_"
                            f"timestep_{self.request.time_step}_"
                            f"{element[0]}.zarr",
                        )

                        if os.path.exists(checkpoint_file_zarr):
                            setattr(self.statistics,
                                element[0],
                                zarr.load(store=checkpoint_file_zarr),
                            )

    def _process_request(self, user_request : dict):
        """Assigns all class attributes from the given user request

        Arguments:
        ----------
        user_request : dict. python dictionary from config.yml file
                or dictionary

        Returns:
        --------
        self.request attribute with values updated
        """
        for key in user_request:
            setattr(self.request, key, user_request[key])

    def _get_logger(self, logging_level : str):
        """Using in the logger function taken from the GSV to log
        errors, warnings and debugs.

        Arguments
        ---------
        logging_level : str
            Minimum severity level for log messages to be printed.
            Options are 'DEBUG', 'INFO', 'WARNING', 'ERROR' and
            'CRITICAL'.

        Returns
        -------
        logging.Logger
            Logger object for Opa logs.
        """
        logging_level = logging_level.upper()
        if logging_level not in {
            'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
        }:
            raise ValueError(
                "logging level not one of the following "
                "('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')"
            )

        logger = logging.getLogger(__name__)
        logger.setLevel(getattr(logging, logging_level))

        if not logger.handlers:  # was the logger already initialized?
            formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        return logger

    ############### end if __init__ #####################

    def _call_recursive(self, how_much_left : int , weight : int,
                        data_source : xr.DataArray
                    ):
        """If there is more data given to the OPA than what is required for the
        statistic, hwere we make a recursive call to itself with the remaining data

        Arguments
        ----------
        self : Opa class
        how_much_left : int. this is how much there is left of the stat to fill.
        weight : int. updated size of the time dimension for the incoming data
        data_source : xr.DataArray. Raw incoming data
        """
        self.logger.debug(
            "Called recursive. Incoming data spanned the end of "
            "the statistic so Opa has been called \ninternally with "
            "the remaining %s time stamps.", weight - how_much_left
        )
        data_source = data_source.isel(time=slice(how_much_left, weight))
        if self.request.stat == "bias_correction":
            Opa.compute_bias_correction(self, data_source)
        else:
            Opa.compute(self, data_source, bias_adjust=False)

    def _full_continuous_data(self, data_source : xr.DataArray,
                              how_much_left : int, weight : int):
        """Called when n_data = count but the stat_freq = continuous. So saving
        intermediate files at the frequency of output_freq.

        Here time append will never be an option. Will create final data,
        save the output if required and call the recursive function if required
        """
        if self.request.save:
            final_time_file_str = create_file_name(
                self, time_word = self.request.output_freq
            )
            save_output_nc(self, final_time_file_str)

        dm = getattr(self.statistics, "final_cum")
        if self.request.stat == "histogram":
            dm2 = getattr(self.statistics, "final2_cum")

        # this then removes final_cum and final2cum along any unnesscary
        # large stats, such as std_cum as that only requires the variance
        # to be carried through
        remove_attributes_continuous(self)

        # if there's more to compute - call before return
        if how_much_left < weight:
            self._call_recursive(how_much_left, weight, data_source)

        if self.request.stat == "histogram":
            # will only return 2 for the histogram
            return dm, dm2
        return dm

    def _finished_no_append(
            self, how_much_left  : int, weight : int,
            data_source : xr.DataArray
        ):
        """ Called when n_data = count and append.time_append = 1, meaning
        the final xr.Dataset will have a time dimension of 1 and statistics do
        not need to be appended.

        Steps
        1. create time name + save final file
        2. remove checkpoint (+ zarr if there)
        3. reset big attributes (STAT.cum)
        4. call recurisve if required
        """
        if self.request.save:

            # if bias_correction, this will save the daily mean or sum
            final_time_file_str = create_file_name(self)
            save_output_nc(self, final_time_file_str)

        remove_checkpoints(self)

        dm = getattr(self.statistics, "final_cum")
        if self.request.stat == "histogram":
            dm2 = getattr(self.statistics, "final2_cum")

        #  resets all the self.statistics attributes
        self.statistics.reset_all_rolling_stats()

        # if there's more to compute - call before return
        if how_much_left < weight:
            self._call_recursive(how_much_left, weight, data_source)

        if self.request.stat == "histogram":
            return dm, dm2
        return dm

    def _final_append(self, how_much_left, weight, data_source):
        """Last call when both n_data == count and the overall
        count_append = time_append. It will first save
        """
        # change file name
        if self.request.save:
            create_file_name(self, append=True)
            save_output_nc(self, self.append.final_time_file_str)

        if self.request.checkpoint:
            remove_checkpoints(self)

        self.statistics.reset_all_rolling_stats(False)

        self.append.reset_time_append_attributes()

        # if there's more to compute - call before return
        if how_much_left < weight:
            self._call_recursive(how_much_left, weight, data_source)

    def _middle_append(self, how_much_left, weight, data_source):
        """Appending to the final xr.Dataset this isn't full yet"""

        self.append.append_checkpoint_flag = True
        # for histograms and percentiles we don't need to checkpoint
        # digest_cum, percentiles_cum or histogram_cum as it's already
        # been converted to dm
        self.statistics.reset_all_rolling_stats(False)

        # if there's more to compute - call before return
        # and before checkpoint
        if how_much_left < weight:
            self._call_recursive(how_much_left, weight, data_source)

        # the flag is set after call recursive, meaning it will only
        # checkpoint the data once after all the recursive calls are
        # finished. Without, it would checkpoint but go back and checkpoint
        # all the previous states again
        if self.request.checkpoint and self.append.append_checkpoint_flag:
            write_checkpoint(self)
            self.append.append_checkpoint_flag = False

    def _first_append(self, how_much_left : int, weight :int,
                    data_source : xr.DataArray
                ):
        """First time that the finished statistc gets appended to
        the output data set. This will initiate some append attributes
        and set self.append.count_append = 1 (otherwise done when
        data is appended)
        """
        if self.request.save:
            final_time_file_str = create_file_name(self)
            self.append.first_append_attributes(final_time_file_str)
        else:
            self.append.first_append_attributes()
        # remove big values from self.statistics except final_cum

        self.logger.debug(
            "Appended %s out of %s completed %s statistics to final "
            "dataset", self.append.count_append, self.append.time_append,
            self.request.stat_freq
        )
        self.statistics.reset_all_rolling_stats(False)

        # if there's more to compute - call before return
        if how_much_left < weight:
            self._call_recursive(how_much_left, weight, data_source)

        if self.request.checkpoint and self.append.append_checkpoint_flag:
            write_checkpoint(self)
            self.append.append_checkpoint_flag = False

    def _finished_with_append(
            self, how_much_left : int, weight : int,
            data_source : xr.DataArray
        ):
        """Function called when n_data == count but output_freq is
        greater than stat_freq so the final dataset will contain
        multiple statistics."""
        if self.append.count_append == 0:
            self._first_append(how_much_left, weight, data_source)

        elif self.append.count_append < self.append.time_append:
            self._middle_append(how_much_left, weight, data_source)

        elif self.append.count_append == self.append.time_append:
            self._final_append(how_much_left, weight, data_source)

    ############## defining class methods ####################
    def compute(self, data_source : xr.Dataset, bias_adjust : bool = True):
        """Compute one_pass statistics. This is called for all
        statistic requests other than bias correction. Here the
        variable will be extracted from the dataset then the time
        stamp will be checked to see if it's the next piece of data
        for the statistic. If it is then then it will update the
        statistic. Once the statistic is 'full' it will create a
        final xr.Dataset with the new statistic and potentially
        save.

        Incoming
        ----------
        data_source : this is the data provided by the user.
            It must be either an xr.Dataset or xr.DataArray.
        bias_adjust : bool, optional
            Whether to bias adjust the data (if turned on by the user)
            This is used to not adjust data again in the recursive
            call, by default True

        Outputs
        ---------
        depending on the user request, the compute function
        will output the requested statistic over the specified time
        frequency after enough data has been passed to it.
        """

        # convert from a data_set to a data_array if required
        data_source = check_variable(self, data_source)

        # bias adjust data if requested
        if bias_adjust and self.request.bias_adjust:
            data_source = bias_correction.call_bias_adjust(data_source, self.request)

        # this checks if there are multiple time stamps
        weight = np.size(data_source.time.data)

        if self.request.stat == "raw":
            dm_raw = check_raw(self, data_source, weight)
            return dm_raw

        # check the time stamp and if the data needs to be initalised
        (
            data_source,
            weight,
            already_seen,
            n_data_att_exist,
            time_stamp_list,
        ) = check_time_stamp(self, data_source, weight)

        if already_seen:
            # stop code as we have already seen this data
            return data_source

        if n_data_att_exist is False:
            # stop code as this is not right data for the start
            return data_source

        how_much_left = update_statistics(
            self, weight, time_stamp_list, data_source
            )
#################### count = ndata #############################
        if self.time.count == self.time.n_data:

            get_final_statistics(self)
            update_attributes(self)
            create_data_set(self, data_source)

            if self.request.stat_freq == "continuous":

                if self.request.stat == "histogram":
                    dm, dm2 = self._full_continuous_data(
                            data_source, how_much_left, weight
                        )
                    return dm, dm2

                dm = self._full_continuous_data(
                        data_source, how_much_left, weight
                    )
                return dm

            # not continuous
            if self.append.time_append == 1:

                if self.request.stat == "histogram":
                    dm, dm2 = self._finished_no_append(
                            how_much_left, weight, data_source
                        )
                    return dm, dm2

                dm = self._finished_no_append(
                        how_much_left, weight, data_source
                    )
                return dm

            # time append > 1
            if self.request.stat == "histogram":
                self._finished_with_append(
                    how_much_left, weight, data_source
                )
                return self.statistics.final_cum, self.statistics.final2_cum

            self._finished_with_append(
                how_much_left, weight, data_source
            )
            return self.statistics.final_cum

    def compute_bias_correction(self, data_source):
        """Compute one_pass statistic for bias correction.
        This produces 3 outputs as opposed to the single output
        from the normal compute function.

        Incoming
        ----------
        data_source : This is the data provided by the user.
                It must be either an xr.Dataset or xr.DataArray.

        Outputs
        ---------
        1. Raw data will be output (and saved to disk if save is True)
        2. Daily aggregates. These will either be daily means of data
                of data or daily sums of the variable is precipitation.
                These will be output (and saved to disk if save is
                True)
        1. Monthly pickle file. This will be pickle (always saved,
                never output in memory) containing t-digest objects.
                These t-digest objects are updated with the daily
                aggregates and as such are only loaded into memory
                at the end of each day. When a new month starts a new
                file will be created unless one already exisits for that
                year.
        """
        # convert from a data_set to a data_array if required
        data_source = check_variable(self, data_source)

        # this checks if there are multiple time stamps
        # in a file and will do two pass statistic
        weight = np.size(data_source.time.data)

        # check the time stamp and if the data needs to be initalised
        (
            data_source,
            weight,
            already_seen,
            n_data_att_exist,
            time_stamp_list,
        ) = check_time_stamp(self, data_source, weight)

        # check if data has been 'seen', will only skip if data doesn't
        # get re-initalised
        if already_seen:
            # stop code as we have already seen this data
            return data_source

        if n_data_att_exist is False:
            # stop code as this is not right data for the start
            return data_source

        how_much_left = update_statistics(
            self, weight, time_stamp_list, data_source
            )

        dm_raw = check_raw_for_bc(
            self, data_source, weight, how_much_left
        )

#################### count = ndata #############################
        if self.time.count == self.time.n_data:

            # loads, updates and makes picklabe tdigests,
            # them saves them to either pickle or zarr
            self.bc.create_and_save_digests_for_bc(self, data_source)

            update_attributes(self, True)
            create_data_set(self, data_source)

            dm = self._finished_no_append(
                    how_much_left, weight, data_source
                )
            return dm_raw, dm
