"""Function to initialise the time attributes"""
import math
import numpy as np
from pandas import Timestamp

from one_pass.convert_time import convert_time

class OpaTime:
    """Class to store all the attributes related to how far through the
    statistic you are.
    """
    def __init__(self, opa_self : object):
        """Init module for the nested time class. This class contains
        all the information regarding timings of the OPA.
        
        count : int. this is the main count that keeps track of how many
                time slices have been fed into the statistic.
        n_data : int. The number of pieces of data (time steps)required to
                complete the requested statistic. Statistic is 'full' when
                count == n_data.
        time_stamp : Pandas timestamp. This is the timestamp of the current
                piece of data that the OPA is processing. If the data comes
                in as time chunk, it will be the time stamp of the last piece
                of data that has been processed.
        stat_freq_min : int. The number of minutes in the requested stat_freq
        init_time_stamp : Pandas timestamp. 
        using_zarr : bool. Flag to say if we are checkpointing the larger
                statistics will zarr because we have exceeded the maximum
                size of a pickle file.
        count_continuous : int. count for number of time stamps added to the
                statistic if the statistic frequency is continuous. This count
                will continuously update and not reset after each output file.
        """
        self.count : int = None
        self.n_data : int = None
        self.time_stamp : Timestamp = None
        self.stat_freq_min = None
        self.init_time_stamp : Timestamp = None
        self.using_zarr : bool = False

        if opa_self.request.stat_freq == "continuous":
            self.count_continuous : int = 0
            self.init_count_time_stamp : Timestamp = None

    def initialise_time(
            self, opa_self : object, time_stamp_min : int,
            time_stamp_tot_append : int
        ):
        """Called when total time in minutes of the incoming time stamp is
        less than the timestep of the data. Will initialise the time
        attributes related to the statistic call.

        Arguments:
        ----------
        time_stamp_min: int. total time in minutes of the incoming time stamp
                into the requested statistic duration. i.e. 0 minutes into
                a day if stat_freq = "daily"
        time_stamp_tot_append : int. the number of 'stat_freq' in units of
                min of the time stamp already completed (only used for
                appending data)

        Returns:
        --------
        self.time.n_data : int. number of required pieces of data for the statistic
                to complete
        self.time.time_append : int. how many completed statistics do we need to
                append in one file. If stat_freq = output_freq this will
                be 1.
        self.time.init_time_stamp : pd.Timestamp. The timestap of the first
                incoming piece of data for that statistic. Will become the
                timestamp of the final data array.
        opa_self.append.time_append : int. Number of statistics that need to
                be appended to the final dataset.
        """
        self.init_time_stamp = self.time_stamp
        if opa_self.request.stat_freq != "continuous":

            if (
                time_stamp_min == 0
                or (opa_self.request.time_step / time_stamp_min).is_integer()
            ):
                self.n_data = int(self.stat_freq_min / opa_self.request.time_step)
            else:
                opa_self.logger.warning("Timings of input data span over new statistic")
                self.n_data = int(self.stat_freq_min / opa_self.request.time_step)

            if opa_self.append.time_append == 0:
            # looking at output freq - how many cum stats you want to
            # save in one netcdf
                if opa_self.request.stat_freq == opa_self.request.output_freq:
                    opa_self.append.time_append = 1

                elif opa_self.request.stat_freq != opa_self.request.output_freq:
                    opa_self.append.first_append_time_stamp = self.time_stamp
                    # converting output freq into a number
                    output_freq_min = convert_time(
                        time_word=opa_self.request.output_freq,
                        time_stamp_input=self.time_stamp
                    )[0]

                    # how many stats you need in the final xr.Dataset if output_freq
                    # is larger than stat_freq. If you're starting part way through
                    # an output_freq, time_stamp_tot_append will be non-zero.
                    opa_self.append.time_append = int(np.round(
                        (output_freq_min - time_stamp_tot_append)/self.stat_freq_min
                        ))
                    if time_stamp_tot_append == 0:
                        opa_self.logger.debug(
                            f"Incoming time stamp {self.time_stamp} corresponds to the start\n "
                            f"of the {opa_self.request.output_freq} output_freq.So initialsing "
                            f"the xr.Dataset with {opa_self.append.time_append}\n "
                            f"{opa_self.request.stat_freq} statistics appended."
                        )
                    else:
                        opa_self.logger.debug(
                            f"Incoming time stamp {self.time_stamp} corresponds to middway\n "
                            f"through the {opa_self.request.output_freq} output_freq. So "
                            f"initialsing the xr.Dataset with {opa_self.append.time_append}\n "
                            f"{opa_self.request.stat_freq} statistics appended."
                        )
                    #output_freq_min < self.time.stat_freq_min
                    if(opa_self.append.time_append < 1 and
                        opa_self.request.stat_freq != "continuous"
                    ):
                        raise ValueError(
                            "Output frequency can not be less than frequency of statistic"
                        )
        else:
            # if continuous, can start from any point in time so
            # n_data might not span the full output_freq (hence why
            # we minus time_stamp_min)
            # this will not work for half hourly output
            self.n_data =  math.ceil(
                (self.stat_freq_min - time_stamp_min) / opa_self.request.time_step
            )

    def check_time_step_int(self, opa_self : object):
        """Checks if the time stamp of the data is wholly divisible
        by the time step. For example, if the time stamp was
        1990-01-01-00:15 but the time step was 60, 15/60 is not an
        integer value and this will pass an Exception
        """
        if (self.stat_freq_min / opa_self.request.time_step).is_integer():
            pass
        else:
            raise ValueError(
                "Frequency of the requested statistic (stat_freq) must"
                " be wholly divisible by the timestep (dt) of the input data"
            )
