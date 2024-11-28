""""create file names for final netCDF file"""

import pandas as pd
import xarray as xr

def create_raw_file_name(data_source : xr.DataArray, weight :int):
    """Creates the final file name for the netCDF file for the raw data.

    Arguments
    ---------
    data_source : Incoming data chunk
    weight : length of time dimensions in the incoming chunk

    Returns
    --------
    final_time_file_str : string output with the name of the final netCDF file
    """
    final_time_file_str = None
    time_stamp_sorted = sorted(data_source.time.data)
    time_stamp_list = [pd.to_datetime(x) for x in time_stamp_sorted]

    if weight > 1:
        final_time_file_str = (
            time_stamp_list[0].strftime("%Y_%m_%d_T%H_%M")
            + "_to_"
            + time_stamp_list[-1].strftime("%Y_%m_%d_T%H_%M")
        )
    else:
        final_time_file_str = time_stamp_list[0].strftime("%Y_%m_%d_T%H_%M")

    return final_time_file_str

def create_file_name(opa_self, append : bool = False, time_word : str = None):
    """Creates the final file name for the netCDF file. If append is True
    then the file name will span from the first requested statistic to the
    last. time_word corresponds to the continous option which outputs checks
    every month
    
    Arguments
    ---------
    opa_self : Opa class
    append : This is a boolean flag. If true, it means that the file name
            needs to span the full statistic time frame.
    time_word : this is used when stat_freq is continuous. In this
            situation we want to use the output_freq for naming the
            file, so time_word is input as the output_freq and the
            stat_freq gets temporarily swapped to the output_freq in
            order to create the name. It is returned to continuous
            at the end.
    Returns
    --------
    final_time_file_str : string output with the name of the final netCDF file
    """
    final_time_file_str = None

    # used for naming continuous data
    if time_word is not None:
        stat_freq_old = opa_self.request.stat_freq
        opa_self.request.stat_freq = time_word

    if append:

        # if opa_self.request.stat_freq == "half_hourly":
        #     opa_self.append.final_time_file_str = (
        #         opa_self.append.final_time_file_str
        #         + "_to_"
        #         + opa_self.time.init_time_stamp.strftime("%Y_%m_%d_T%H%_M")
        #     )

        if opa_self.request.stat_freq in (
                "hourly",
                "2hourly",
                "3hourly",
                "6hourly",
                "12hourly"
            ):
            opa_self.append.final_time_file_str = (
                opa_self.append.final_time_file_str
                + "_to_"
                + opa_self.time.init_time_stamp.strftime("%Y_%m_%d_T%H")
            )

        elif opa_self.request.stat_freq in ("daily", "weekly"):
            opa_self.append.final_time_file_str = (
                opa_self.append.final_time_file_str
                + "_to_"
                + opa_self.time.init_time_stamp.strftime("%Y_%m_%d")
            )

        elif opa_self.request.stat_freq in ("monthly","3monthly"):
            opa_self.append.final_time_file_str = (
                opa_self.append.final_time_file_str
                + "_to_"
                + opa_self.time.init_time_stamp.strftime("%Y_%m")
            )

        elif (opa_self.request.stat_freq == "yearly" or
                opa_self.request.stat == "10yearly"
            ):
            opa_self.append.final_time_file_str = (
                opa_self.append.final_time_file_str + "_to_" +
                opa_self.time.init_time_stamp.strftime("%Y")
            )
    else:

        # if opa_self.request.stat_freq == "half_hourly":
        #     final_time_file_str = \
        #         opa_self.time.init_time_stamp.strftime("%Y_%m_%d_T%H_%M")

        if opa_self.request.stat_freq in (
                "hourly",
                "2hourly",
                "3hourly",
                "6hourly",
                "12hourly",
                "daily_noon"
        ):
            final_time_file_str = \
                opa_self.time.init_time_stamp.strftime("%Y_%m_%d_T%H")

        elif opa_self.request.stat_freq == "daily":
            final_time_file_str = \
                opa_self.time.init_time_stamp.strftime("%Y_%m_%d")

        elif opa_self.request.stat_freq == "weekly":
            final_time_file_str = \
                opa_self.time.init_time_stamp.strftime("%Y_%m_%d")

        elif opa_self.request.stat_freq in ("monthly", "3monthly"):
            final_time_file_str = \
                opa_self.time.init_time_stamp.strftime("%Y_%m")

        elif (opa_self.request.stat_freq == "yearly" or
                opa_self.request.stat == "10yearly"
            ):
            final_time_file_str = \
                opa_self.time.init_time_stamp.strftime("%Y")

    if time_word is not None:
        opa_self.request.stat_freq = stat_freq_old

    return final_time_file_str
