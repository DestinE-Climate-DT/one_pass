""""save final statistic"""

import os

def save_output_nc(opa_self, final_time_file_str : str):
    """Function that saves final_cum Dataset to netcdf.
    This is only called for the statistics, not for the raw data

    Arguments
    ---------
    opa_self : Opa class
    dm : final xr.Dataset that needs to be saved
    final_time_file_str : the time str for the final file name
    """
    # for saving the daily aggregations of the bias correction
    if (opa_self.request.stat == "bias_correction" and
            opa_self.request.variable not in
            opa_self.fixed.precip_options
        ):
        file_name = os.path.join(
            opa_self.request.save_filepath,
            f"{final_time_file_str}_{opa_self.request.variable}_"
            f"timestep_{opa_self.request.time_step}_"
            f"{opa_self.request.stat_freq}_mean.nc",
        )

    elif (opa_self.request.stat == "bias_correction" and
            opa_self.request.variable in
            opa_self.fixed.precip_options
        ):
        file_name = os.path.join(
            opa_self.request.save_filepath,
            f"{final_time_file_str}_{opa_self.request.variable}_"
            f"timestep_{opa_self.request.time_step}_"
            f"{opa_self.request.stat_freq}_sum.nc",
        )

    elif opa_self.request.stat == "histogram":
        file_name = os.path.join(
            opa_self.request.save_filepath,
            f"{final_time_file_str}_{opa_self.request.variable}_"
            f"timestep_{opa_self.request.time_step}_"
            f"{opa_self.request.stat_freq}_{opa_self.request.stat}_bin_counts.nc",
        )

    else:  # normal other stats
        file_name = os.path.join(
            opa_self.request.save_filepath,
            f"{final_time_file_str}_{opa_self.request.variable}_"
            f"timestep_{opa_self.request.time_step}_"
            f"{opa_self.request.stat_freq}_{opa_self.request.stat}.nc",
        )

    dm = getattr(opa_self.statistics, "final_cum")
    dm.to_netcdf(path=file_name, mode="w")
    dm.close()

    # if it's a histom there will be two final outputs
    if opa_self.request.stat == "histogram":
        file_name = os.path.join(
            opa_self.request.save_filepath,
            f"{final_time_file_str}_{opa_self.request.variable}_"
            f"timestep_{opa_self.request.time_step}_"
            f"{opa_self.request.stat_freq}_{opa_self.request.stat}_"
            "bin_edges.nc",
        )
        dm = getattr(opa_self.statistics, "final2_cum")
        dm.to_netcdf(path=file_name, mode="w")
        dm.close()

def save_raw_output_nc(
            opa_self, dm_raw, final_time_file_str
    ):
    """Function that save the raw data. Will be called from
    raw_data.py

    Arguments
    ---------
    opa_self : Opa class
    dm_raw : raw data to be saved. Will have modified attributes
    """
    file_name = os.path.join(
        opa_self.request.save_filepath, f"{final_time_file_str}_"
        f"{opa_self.request.variable}_raw_data.nc"
    )

    dm_raw.to_netcdf(path=file_name, mode="w")
    dm_raw.close()
