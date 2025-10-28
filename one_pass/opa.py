"""Main module for calling one pass functions"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple, Union

import os
import pickle

from pandas import to_datetime
import xarray as xr

from one_pass.log import get_logger
from one_pass.request import Request
from one_pass.statistics.bias_correction import bias_adjust as run_bias_adjustment
from one_pass.statistics.opa_statistics import OpaStatistics
from one_pass.statistics.raw_data import check_raw
from one_pass.statistics.finalize import create_data_set, update_attributes
from one_pass.statistics.update import update_statistics
from one_pass.saving.save_final import save_output_nc
from one_pass.time import OpaTime, validate, split
from one_pass.options import StatKey
from one_pass.checkpointing.remove_checkpoints import remove_checkpoints
from one_pass.mpi import MPI_LAUNCHED


@dataclass
class DataSetInfo:
    """Class that stores information about the incoming data.

    Attributes
    ----------
    shape_data_source_tail : tuple, optional
        Dimensions of the incoming xarray data compressed to 1 in the time dimension.
        Used for re-sizing statistics that need to be flattened.
    size_data_source_tail : int, optional
        Total size of the flattened incoming xarray data. Calculated as the
        multiplication of shape_data_source_tail elements.
    data_var_attr : dict, optional
        Attributes of the incoming data for the underlying xr.DataArray
        corresponding to the variable of interest.
    data_set_attr : dict, optional
        If the incoming data is an xr.Dataset, this stores the attributes
        of the overall dataset to pass back to the final statistic.
    filename : str, optional
        Filename used when data is appended (i.e., output_freq > stat_freq).
        Corresponds to the timestamp of the first piece of data in the final
        appended statistic. Used in the final file name.
    """

    shape_data_source_tail: Optional[tuple] = None
    size_data_source_tail: Optional[int] = None
    data_var_attr: Optional[Mapping[str, Any]] = field(default_factory=dict)
    data_set_attr: Optional[Mapping[str, Any]] = field(default_factory=dict)
    filename: Optional[str] = None


class Opa:  # pylint: disable=R0902,R0903,R0913,R0917
    """Main OPA class for computing one-pass statistics.

    This class manages the computation of statistics using a one-pass algorithm.
    It can contain up to 8 nested dataclasses or full classes that contain all
    the information required for computation. The main nested classes include:
    - Request: User data request configuration
    - DataSetInfo: Information about incoming data
    - OpaTime: Time-related attributes and management
    - OpaStatistics: Rolling statistics storage and computation
    """

    # For zarr compression (temporarily disabled.)
    pickle_limit: int = 3

    def __init__(
        self,
        user_request: Dict,
        keep_checkpoints: bool = False,
        use_zarr: bool = False,
        workers: int = 1,
        logging_level: str = "INFO",
        as_float32: bool = False,
        logging_file: Optional[str] = None,
    ):
        """Initialize the OPA instance.

        Will initialize all data from their class attributes, then if a checkpoint
        file exists it will re-load the previous state of the Opa class from the
        pickle checkpoint.

        Parameters
        ----------
        user_request : dict
            Dictionary containing the user data request configuration. Will be
            parsed into a Request dataclass.
        keep_checkpoints : bool, default False
            Whether to keep checkpoint files after computation completes.
        logging_level : str, default "INFO"
            Logging level for the OPA logger. Options are 'DEBUG', 'INFO',
            'WARNING', 'ERROR', 'CRITICAL'.
        workers : int, default 1
            Number of workers to use for parallel computation.
        logging_file : str, optional
            File path for logging output. If None, logs to console.

        Notes
        -----
        The following attributes are initialized:
        - request : Request dataclass containing user configuration
        - data_set_info : DataSetInfo dataclass for incoming data information
        - time : OpaTime class for time attributes and management
        - statistics : OpaStatistics class for rolling statistics storage
        - logger : Logger instance for OPA logs
        """
        # assign attributes from the request
        self.logger = get_logger(
            self.__class__.__name__,
            logging_level=logging_level,
            filename=logging_file,
        )
        self._result = None
        self.request = Request.parse(user_request, self.logger)
        self.keep_checkpoints = keep_checkpoints

        if use_zarr:
            raise NotImplementedError(
                "Zarr compression temporarily disabled. Please, use use_zarr=False"
            )
        self.use_zarr = use_zarr
        self.as_float32 = as_float32
        self.workers = workers

        self.data_set_info = DataSetInfo()

        self.time: OpaTime
        self.statistics: OpaStatistics

        # Do not initialize classes if requested stat is "raw"
        if self.request.stat == StatKey.RAW:
            return

        # Else, initialize everything
        self.time = OpaTime(self)
        self.statistics = OpaStatistics(self)

        # Look for stored checkpoint if requested.
        # This will check for checkpoint file and
        # load if one exists.
        if self.request.checkpoint:
            self._check_checkpoint()

        self.logger.info("OPA ininitalized with request: %s", self.request)

    @property
    def finalized(self) -> bool:
        """Whether the current statistic period is complete."""

        return self.time.finalized

    @property
    def result(self):
        """Get the computed result.

        Returns
        -------
        xr.Dataset or tuple of xr.Dataset or None
            The computed statistic result. Returns None if no statistic
            has been finalized yet.

        Warns
        -----
        UserWarning
            If accessing result before statistic computation is finalized.
        """
        if self._result is None:
            # TODO: Make more verbose
            self.logger.warning("Accessing result before statistic is finalised.")
        return self._result

    @result.setter
    def result(self, value: Union[xr.Dataset, Tuple[xr.Dataset, xr.Dataset]]):
        save = self.request.save
        if self._result is not None and not save:
            self.logger.warning(
                "Buffered result is already set and save is not enabled. "
                "Overwriting with new result."
            )
        self.logger.debug("Setting result to %s", _repr_data_source(value))
        self._result = value

    def _check_checkpoint(self):
        """Check for existing checkpoint file and restore state if found.

        Takes the user request and creates the checkpoint file path. If the
        checkpoint file exists, it will load the previous state of the Opa
        class from the pickle checkpoint.

        Notes
        -----
        If a checkpoint file is found, the entire Opa instance state is
        restored from the checkpoint, allowing computation to resume from
        where it left off.
        """
        # pylint: disable=W0212

        checkpoint_path = self.request.checkpoint_file

        # see if the checkpoint file exists
        if os.path.exists(checkpoint_path):
            with open(checkpoint_path, "rb") as brf:
                loaded_object = pickle.load(brf)
                loaded_object._result = None
                self.__dict__.update(loaded_object.__dict__)

    def _done(self):
        """Handle completion of a compute cycle.

        Creates checkpoints if needed and returns finalized statistics when complete.

        Returns
        -------
        xr.Dataset or tuple of xr.Dataset or None
            Final computed statistics if the time period is finalized, otherwise None.
            For histogram statistics, returns a tuple of (None, None) when not finalized.
        """

        self.logger.info("Final stage of postprocess")

        # Handle checkpoints (this logic comes formerly from update_statistics)
        if self.request.checkpoint:
            # this will not be written when count == ndata
            if self.time.continuous or not self.time.finalized:
                self.checkpoint()

        if self.time.finalized:
            return self.result

        return None if self.request.stat != StatKey.HISTOGRAM else (None, None)

    # TODO: Earlier cache attrs of data_source so it doesn't
    # need to be passed down to create_data_set for its attributes
    def _rotate(self, data_source):
        """Finalize the current statistic and prepare for the next aggregation period.

        This method is called when the current statistic period is complete. It finalizes
        the current statistics into a dataset, resets time tracking, and saves output if
        configured.

        Parameters
        ----------
        data_source : xr.DataArray
            The data source used to extract metadata attributes for the final dataset.

        Notes
        -----
        This method performs the following steps:
        1. Updates dataset attributes with OPA metadata
        2. Converts cumulative statistics to final dataset
        3. Resets time tracking for the next period
        4. Resets cumulative statistics while keeping finalized results
        5. Saves to NetCDF and removes checkpoints if configured
        """

        self.logger.info("Starting rotate")

        update_attributes(self)
        self.statistics.add_final(create_data_set(self, data_source))

        # Reset tracking of timestamps.
        self.time.reset(hard=False)
        self.statistics.reset(hard=False)

        # the flag is set after call recursive, meaning it will only
        # checkpoint the data once after all the recursive calls are
        # finished. Without, it would checkpoint but go back and checkpoint
        # all the previous states again
        if self.time.finalized:
            if self.request.save:
                remove_checkpoints(self)
                self.save_nc()
            self.result = self.statistics.get_final()
            self.time.reset(hard=True)
            self.statistics.reset(hard=True)

    def _preprocess(
        self,
        data_source: Union[xr.DataArray, xr.Dataset],
        bias_adjust: bool = True,
    ):
        """Preprocess incoming data before statistical computation.

        Handles data conversion, bias adjustment, timestamp validation, initialization,
        and splitting of data into current and carryover portions.

        Parameters
        ----------
        data_source : xr.DataArray or xr.Dataset
            Input data to preprocess.
        bias_adjust : bool, default True
            Whether to apply bias adjustment if configured in the request.

        Returns
        -------
        tuple
            Three-element tuple (abort, current_data, carryover_data):
            - abort : bool
                If True, computation should be halted (data already seen or raw output).
            - current_data : xr.DataArray
                Data to be processed in this cycle.
            - carryover_data : xr.DataArray or None
                Data that extends beyond current period, to be processed recursively.
        """

        self.logger.info(
            "Starting preprocess with %d time steps", data_source.time.size
        )
        stat = self.request.stat

        # convert from a data_set to a data_array if required
        data_source = data_source_to_data_array(self, data_source)

        # bias adjust data if requested
        if bias_adjust and self.request.bias_adjust:
            data_source = run_bias_adjustment(data_source, self.request)

        if stat == StatKey.RAW:
            return True, check_raw(self, data_source), None

        # check the time stamp and if the data needs to be initalised
        data, already_seen, to_init = validate(self.time, data_source, self.logger)

        # First argument True will halt the computation
        if already_seen:
            return True, data, None

        # If data doesn't contain a valid start point for the statistic, skip it
        if not to_init and not self.time.initialized:
            self.logger.debug(
                "Skipping data - no valid start point found for %s statistic. "
                "Waiting for data that aligns with the statistic period.",
                self.request.stat_freq.value,
            )
            return True, None, None

        # If timestamps signal initialization and time is not initialized, do it.
        if not self.time.initialized and to_init:
            self.time.initialize(to_datetime(data.time.data[0]))

        # Id. for statistics
        if not self.statistics.initialized and to_init:
            self.statistics.initialize(data)

        current, carryover = split(self.time, data, self.logger)

        # First algument False will proceed with the computation
        return False, current, carryover

    def _postprocess(self, finalized, data_source, carryover):
        """Post-process after updating statistics.

        Handles rotation when statistics are complete, recursive processing of
        carryover data, and finalization.

        Parameters
        ----------
        finalized : bool
            If True, the current statistic period is complete and should be rotated.
            If False, wait for more data to complete the current statistic.
        data_source : xr.DataArray
            The data source used for metadata in dataset creation.
        carryover : xr.DataArray or None
            Remaining data that extends beyond the current period. If present,
            triggers recursive processing. If None, marks completion of input data.

        Returns
        -------
        xr.Dataset or tuple of xr.Dataset or None
            Finalized statistics if all processing is complete, otherwise None.
        """

        self.logger.info(
            "Starting postprocess with %d time steps", data_source.time.size
        )

        # If current statistic is finalized,
        #   rotate OPA, meaning:
        #       a. If number of statistics if not fulfilled*,
        #             add data point to dynamically created dataset
        #       b. If number of statistics is fulfilled*,
        #             save (if enabled) and schedule for return
        #
        # * Note: here "fulfilled" means
        #     EITHER
        #         (1) output_freq == stat_freq
        #               (e.g. daily aggregations, daily outputs),
        #     OR
        #         (2) output_freq > stat_freq AND num_statistics == cur_statistic
        #               (e.g. daily aggregations, monthly outputs,
        #                     but we are at the end of the month)
        if finalized:
            # Current statistic finalized, rotate.
            self._rotate(data_source)
        else:
            # Current statistic is not finalized, wait for more data.
            pass

        self.logger.debug(
            "_postprocess:opa.time.finalized: %s, arg finalized: %s",
            self.time.finalized,
            finalized,
        )

        # If there is carryover (remaining, to be processed) data,
        #   call OPA again for the remaining data.
        # Otherwise,
        #   Write checkpoint (if enabled)
        if carryover is None:
            # Done computing the (originally) passed data.
            return self._done()

        if self.time.finalized and not self.request.save:
            # TODO: Check in pre-process, and here only have an assert
            self.logger.warning(
                "Incoming data spans after output frequency but 'save' was set to False. "
                "Refusing to continue aggregation, since that would result in loss of data. "
                "Unprocessed time steps: %s",
                carryover.time.size,
            )
            return self._done()

        self.logger.debug(
            "Recursively processing carryover data. "
            "Incoming data spanned the end of the statistic so Opa "
            "has been called internally with the remaining %s time stamps.",
            carryover.time.size,
        )

        return self.compute(carryover)

    def compute(
        self,
        data_source: Union[xr.DataArray, xr.Dataset],
        bias_adjust: bool = True,
    ) -> Union[
        xr.DataArray,
        xr.Dataset,
        Tuple[xr.DataArray, xr.DataArray],
        None,
        Tuple[None, None],
    ]:
        """Compute one_pass statistics. This is called for all
        statistic requests other than bias correction. Here the
        variable will be extracted from the dataset then the time
        stamp will be checked to see if it's the next piece of data
        for the statistic. If it is then then it will update the
        statistic. Once the statistic is 'full' it will create a
        final xr.Dataset with the new statistic and potentially
        save.

        Parameters
        ----------
        data_source : this is the data provided by the user.
            It must be either an xr.Dataset or xr.DataArray.
        bias_adjust : bool, optional
            Whether to bias adjust the data (if turned on by the user)
            This is used to not adjust data again in the recursive
            call, by default True

        Returns
        -------
        depending on the user request, the compute function
        will output the requested statistic over the specified time
        frequency after enough data has been passed to it.
        """

        self.logger.info(  # pylint: disable=W1203
            f"Starting compute (with{'out' if not MPI_LAUNCHED else ''} MPI)",
        )

        self.logger.info("Input data source: %s", _repr_data_source(data_source))

        abort, data_source, carryover = self._preprocess(data_source, bias_adjust)

        if abort:
            return data_source

        self.logger.info(
            "Updating statistics with chunked data with %d time steps",
            data_source.time.size,
        )
        finalized = update_statistics(self, data_source)

        return self._postprocess(finalized, data_source, carryover)

    def checkpoint(self, directory: Optional[str | Path] = None):
        """Save current OPA state to a checkpoint file.

        Serializes the entire OPA instance to a pickle file for later restoration.
        This allows computation to be resumed from the current state.

        Parameters
        ----------
        directory : str or Path, optional
            Directory to save the checkpoint file. If None, uses the directory
            specified in `request.checkpoint_filepath`. This parameter allows
            saving checkpoints to custom locations without modifying the request
            configuration.

        Raises
        ------
        ValueError
            If checkpointing was disabled in the request configuration
            (i.e., `checkpoint=False` in the request).

        Notes
        -----
        - Checkpoint files are named using the convention:
          `checkpoint_{variable}_{stat_freq}_timestep_{timestep}_{output_freq}_{stat}.pkl`
        - Before saving, all dask arrays in statistics are loaded into memory
          to reduce disk space usage.
        - The checkpoint includes all OPA state needed to resume computation:
          time tracking, statistics accumulation, and configuration.

        Examples
        --------
        Save checkpoint to default location from request:

        >>> opa.checkpoint()

        Save checkpoint to custom directory:

        >>> opa.checkpoint(directory="/path/to/custom/checkpoint/dir")
        """

        if self.request.checkpoint is False:
            raise ValueError(
                "Cannot checkpoint OPA becasue in request, checkpoint was set to False."
            )

        if directory is None:
            # NOTE: Should be renamed (-> checkpoint_dir)
            directory = self.request.checkpoint_filepath

        fpath = os.path.join(directory, self.request.checkpoint_name)

        self.logger.info("OPA dump to checkpoint fpath: %s", fpath)

        with open(fpath, "wb") as bf:
            pickle.dump(self, bf, protocol=pickle.HIGHEST_PROTOCOL)

    def save_nc(self):
        """Save finalized dataset to NetCDF file.

        Saves the computed statistic to a NetCDF file. For computed statistics,
        this delegates to save_output_nc. For raw data, saving is handled
        directly in the check_raw function during computation.

        Notes
        -----
        - This method is only called for computed statistics, not for raw data.
        - NetCDF files are named using the convention:
          `{timestamp}_{variable}_timestep_{timestep}_{stat_freq}_{stat}.nc`
        - For histogram statistics, two files are created:
          one for bin counts (*_bin_counts.nc) and one for bin edges (*_bin_edges.nc)
        - The filename timestamp is determined from the data set information
          and represents the time range of the statistic.
        """
        # Raw data doesn't make OPA reach here.

        # NOTE: Should be renamed (-> checkpoint_dir)
        directory = self.request.save_filepath

        self.logger.info(
            "Saving finalized dataset to NetCDF file in directory: %s", directory
        )
        save_output_nc(self, directory=directory)


def _repr_data_source(
    obj: Union[
        None,
        xr.Dataset,
        xr.DataArray,
        Tuple[None, None],
        Tuple[xr.Dataset, xr.Dataset],
        Tuple[xr.DataArray, xr.DataArray],
    ]
) -> str:
    if obj is None:
        return "None"
    if isinstance(obj, tuple):
        return "(" + ", ".join([_repr_data_source(v) for v in obj]) + ")"
    if isinstance(obj, (xr.Dataset, xr.DataArray)):
        return "{:s} (dims: {:s}; times: [{:s} - {:s}] ({:d}))".format(  # pylint: disable=C0209
            str(obj.__class__.__name__),
            ", ".join(list(obj.sizes)),
            str(obj.time.data[0].astype("datetime64[s]")),
            str(obj.time.data[-1].astype("datetime64[s]")),
            obj.time.size,
        )
    return repr(obj)


def data_source_to_data_array(
    opa: Opa, data_source: Union[xr.Dataset, xr.DataArray]
) -> xr.DataArray:
    """Convert input data to xarray DataArray format.

    Checks if the incoming data is an xarray DataArray. If it's an xr.Dataset,
    converts to DataArray by extracting the specified variable and stores
    the metadata attributes for later use.

    Parameters
    ----------
    opa : Opa
        OPA instance containing the request configuration.
    data_source : xr.Dataset or xr.DataArray
        Input data to be converted.

    Returns
    -------
    xr.DataArray
        The input data as a DataArray. If input was a Dataset, contains
        the extracted variable specified in the request.

    Raises
    ------
    ValueError
        If unable to extract the specified variable from the Dataset.
    """
    if isinstance(data_source, xr.Dataset):
        # keeping the attributes of the full dataSet to give to the final dataSet
        opa.data_set_info.data_set_attr = data_source.attrs.copy()
        try:
            # converts to a dataArray
            data_source = data_source[opa.request.variable]
            opa.data_set_info.data_var_attr = data_source.attrs.copy()
        except (KeyError, AttributeError) as exc:
            raise ValueError(
                f"Unable to extract variable {opa.request.variable} from passed Dataset."
            ) from exc
    else:
        opa.data_set_info.data_var_attr = data_source.attrs.copy()
        # if this has been called recursively, this way you're not
        # overwriting data_set_attr with the variables from the dataArray
        if opa.data_set_info.data_set_attr is None:
            # still extracting attributes from dataArray here
            opa.data_set_info.data_set_attr = data_source.attrs.copy()

    return data_source  # type: ignore
