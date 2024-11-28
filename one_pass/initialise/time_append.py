"""Module for dealing with attributes specific to time_append
Time append is greater than one when you have an output_freq
greater than stat_freq."""

import xarray as xr
import pandas as pd

from one_pass.checkpointing.remove_checkpoints import remove_checkpoints

class Append:
    """ Appending class tkat keeps track of timings for appending data
    which will be needed if output_freq > stat_freq..
    """
    def __init__(self):
        """Initilisation of the append class. This class holds all
        the information for appending

        append_checkpoint_flag : bool. Flag to say if the data should be
                checkpointed or not. This is used in a recursive call when
                the data provided spans the end of the statistic. It will
                avoid data being 'back' checkpointed uncessarily.
        time_append : int. This is the number of final statistics that need
                to be appended together in the final xr.Dataset. Statistics
                are appended in the attribute statistics.final_cum. If stat
                _freq = output_freq this is = 1.
        count_append : int. The count corresponding to the time append,
                how many statistics have been appended to the final xr.Dataset.
        final_time_file_str : str. String value giving the time stamp of the
                first piece of appended data. This is used in the final
                netcdf file name.
        first_append_time_stamp : 
        """
        self.append_checkpoint_flag : bool = None
        self.time_append : int = 0
        self.count_append : int = 0
        self.final_time_file_str : str = None
        self.first_append_time_stamp : pd.Timestamp = None

    def first_append_attributes(self, final_time_file_str : str = None):
        """First checks if this is the first of the appending
        series. In this case count append will either not be set
        or it will be equal to 0. If it's the first it will

        Arguments
        ---------
        opa_self : Opa class
        final_time_file_str : if the final output is to be saved, the incoming
                file string will be given here and set as an attribute to be
                modified before the final save.

        Returns
        --------
        self.append_checkpoint_flag : Boolean flag used for the check
                pointing
        self.count_append : count to keep track of how many final stats
                you have appended
        opa_self.final_time_file_str : will store the file str to append later
                for the final file
        """
        self.append_checkpoint_flag = True
        self.count_append = 1
        if final_time_file_str is not None:
            self.final_time_file_str = final_time_file_str

    def reset_time_append_attributes(self):
        """Removes attributes for the time_append. This will be called when
        count_append = time_append and the final statistics is output. This
        avoids any wrong values being carried through into a new statistic from
        a checkpoint file
        """
        self.final_time_file_str = None
        self.time_append = 0
        self.count_append = 0

        # setting the flag here to False tells the code that the statistic is
        # finished. This means that if it was part of a recursive call,
        # no checkpoint will be written, as you don't need to
        # checkpoint finished data
        self.append_checkpoint_flag = False

    def data_output_append(self,
                        opa_self : object,
                        dm : xr.Dataset,
                        second_hist : bool = False
                        ):
        """Appeneds final Dataset along the time dimension if stat_output
        is larger than the requested stat_freq. It also sorts along the
        time dimension to ensure data is also increasing in time

        Arguments
        ---------
        opa_self : Opa class
        dm : incoming xr.Dataset that the final statistic with one time
                dimension.
        second_hist : bool. If true, it indicates that we should append
                final2_cum with the histogram bin edges

        Returns
        ---------
        opa_self.statistics.final_cum : Appended xr.Dataset along the time
                dimension with the new input xr.Dataset dm
        opa_self.statistics.final2_cum : Appended xr.Dataset along the time
                dimension with the new input xr.Dataset dm corresponding to
                the bin edges of the histograms
        opa_self.time.count_append : updated count append with new dm
        """
        if second_hist:

            opa_self.statistics.final2_cum = xr.concat(
                [opa_self.statistics.final2_cum, dm], "time")
            opa_self.statistics.final2_cum = \
                opa_self.statistics.final2_cum.sortby("time")

        else:
            opa_self.statistics.final_cum = xr.concat(
                [opa_self.statistics.final_cum, dm], "time")
            opa_self.statistics.final_cum = \
                opa_self.statistics.final_cum.sortby("time")

            # only updated count append once, not twice if it's for the
            # other histogram
            self.count_append = self.count_append + 1
            opa_self.logger.debug(
                f"Appended {self.count_append} out of {self.time_append} "
                f"completed {opa_self.request.stat_freq} statistics "
                f"to final dataset"
            )
    def remove_time_append(self, opa_self : object):
        """resets the time append class and removes checkpoints. This
        is called when the new time stamps are being checked.

        Arguements
        ----------
        opa_self : Opa class
        """
        opa_self.append = Append()
        remove_checkpoints(opa_self)
