"""Create final xr.Datasets"""

import numpy as np
import xarray as xr

from one_pass.saving.modify_attributes import get_datetime_str

def change_data_source_shape(
        opa_self, data_source : xr.DataArray,
        final_stat : np.ndarray
    ):
    """If the statistic is percentile, iams or thresh_exceed, then the
    final shape of the xr.dataset changes as it contains new co-ordinates.
    This function modifies the data_source to contain the new co-ords
    and dimensions
    """
    str_date_time = get_datetime_str()

    # adding new dimensions for thresholds
    if opa_self.request.stat == "thresh_exceed":

        data_source = data_source.expand_dims(
            dim={"thresholds": len(
                opa_self.request.thresh_exceed
            )},
            axis=1
        )

        new_attr_str = str(
            str_date_time
            + " threshold exceedance values assigned with the "
            "one_pass algorithm\n"
        )
        new_coord_attrs = {'history' : new_attr_str}

        # name the threshold coordinate
        data_source = data_source.assign_coords(
            thresholds = ("thresholds", opa_self.request.thresh_exceed,
            new_coord_attrs)
        )

    # adding new dimensions for percentiles
    if opa_self.request.stat == "percentile":

        data_source = data_source.expand_dims(
            dim={"percentile": np.size(
                opa_self.request.percentile_list
            )},
            axis=1
        )

        new_attr_str = str(
            str_date_time
            + " percentile values assigned with the "
            "one_pass algorithm\n"
        )
        new_coord_attrs = {'history' : new_attr_str}

        # name the percentile coordinate
        data_source = data_source.assign_coords(
            percentile = ("percentile", np.array(
                opa_self.request.percentile_list),
                new_coord_attrs)
        )

    #adding new dimensions for iams
    if opa_self.request.stat == "iams":
        # adding time dimension in final stat
        final_stat = np.expand_dims(
            final_stat, axis=0
        )
        # adding durations dimension in ds
        data_source = data_source.expand_dims(
            dim={"durations": np.size(opa_self.iams.durations)}, axis=1
        )

        new_attr_str = str(
            str_date_time
            + " duration values in minutes assigned with the "
            "one_pass algorithm\n"
        )
        new_coord_attrs = {'history' : new_attr_str}
        # name the durations co-ordinate
        data_source = data_source.assign_coords(
            durations = ("durations", np.array(
                opa_self.iams.durations),
                new_coord_attrs)
        )

    return data_source, final_stat

def create_final_dataset(opa_self,
                        data_source : xr.DataArray,
                        final_stat : np.ndarray):
    """Make the final data set with the modified attributes from 'modify_
    attributes function and potentially modified co-ordinates.

    Arguments
    ---------
    data_source : xr.DataArray. modified incoming data used for the dimensions
            variable names and co-ordinates
    final_stat : np.ndarray. the final numpy array containting the statistic

    Returns
    --------
    dm : final xr.Dataset created from the final_stat which is the STAT_cum
            depending on the statistic this will be extracted, potenitally
            re-gridded and set into a dataset with the original attributes
            (including updated attributes) and a time stamp corresponding
            to the first time stamp of the statistic
    """
    attributes = opa_self.data_set_info.data_var_attr

    dm = xr.Dataset(
        data_vars=dict(
            # need to add variable attributes
            [(str(data_source.name), (data_source.dims, final_stat, attributes))],
        ),
        coords=dict(data_source.coords),
        attrs=opa_self.data_set_info.data_set_attr,
    )

    if hasattr(opa_self.statistics, 'timings_cum'):
        str_date_time = get_datetime_str()
        new_attr_str = str(
            str_date_time
            + f" timestamp of {opa_self.request.stat_freq} {opa_self.request.stat} "
            " calculated using one_pass algorithm\n"
        )
        timing_attrs = {'history' : new_attr_str}
        dm = dm.assign(timings = (data_source.dims,
                                opa_self.statistics.timings_cum.data, timing_attrs))

    return dm

def create_data_set(opa_self, data_source : xr.DataArray):
    """Creates xarray dataSet object with final data depending
    on the statistic and updated metadata

    Arguments
    ----------
    opa_stat : Opa class
    data_source : xr.DataArray. original data used for sizing

    Returns
    ---------
    opa_self.statistics.final_cum : the final dm (returned from
            create_final_dataset) given as an attribute to the OPA
            class. This is given the name 'final_cum' so that it
            can be removed easily
    """
    # compress the dataset down to 1 dimension in time
    data_source = data_source.tail(time=1)
    # re-label the time coordinate
    data_source = data_source.assign_coords(
        time=(["time"], [opa_self.time.init_time_stamp], data_source.time.attrs)
    )

    # final stat will be the cummulative statistic array
    final_stat = None
    # this is the flag to extract the mean value for the bias_correction
    if (opa_self.request.stat == "bias_correction") and (
            opa_self.request.variable not in opa_self.fixed.precip_options
        ):
        final_stat = getattr(opa_self.statistics, "mean_cum")

    elif (opa_self.request.stat == "bias_correction") and (
            opa_self.request.variable in opa_self.fixed.precip_options
        ):
        final_stat = getattr(opa_self.statistics, "sum_cum")

    elif opa_self.request.stat == "histogram":
        data_source_old = data_source
        final_stat = getattr(opa_self.statistics, "histogram_cum")
        data_source = data_source.expand_dims(
            dim={"bin_count":
            np.shape(opa_self.statistics.histogram_cum)[0]},
            axis=1
        )

    else:
        final_stat = getattr(opa_self.statistics,
                            opa_self.request.stat + "_cum")

    if opa_self.request.stat in ("min", "max"):
        final_stat = final_stat.data

    # only does something for percentiles and iams
    data_source, final_stat = change_data_source_shape(
        opa_self, data_source, final_stat
    )

    if opa_self.append.count_append == 0:
        opa_self.statistics.final_cum = create_final_dataset(
                opa_self, data_source, final_stat
            )

    elif opa_self.append.count_append > 0:
        dm = create_final_dataset(
                opa_self, data_source, final_stat
            )
        opa_self.append.data_output_append(opa_self, dm)

    if opa_self.request.stat == "histogram":

        final_stat = getattr(opa_self.statistics, "histogram_bin_edges_cum")
        data_source_old = data_source_old.expand_dims(
            dim={"bin_edges": np.shape(
                opa_self.statistics.histogram_bin_edges_cum)[1]}
                ,axis=1
        )

        if opa_self.append.count_append == 0:
            opa_self.statistics.final2_cum = create_final_dataset(
                    opa_self, data_source_old, final_stat
                )
        else:
            dm = create_final_dataset(
                    opa_self, data_source_old, final_stat
                )
            opa_self.append.data_output_append(opa_self, dm, True)

def create_data_set_for_bc(
        opa_self, data_source : xr.Dataset,
        data_set_attrs : dict, data_var_attrs : dict
    ):
    """Creates xarray dataSet object for the bias correction digests

    Arguments
    ----------
    opa_stat : Opa class
    data_source : original data used for sizing

    Returns
    ---------
    dm : final xr.Dataset created from the final_stat which is the digest_cum
            re-gridded and set into a dataset with the original attributes
            (including updated attributes) and a time stamp corresponding
            to the first time stamp of the statistic
    """
    # compress the dataset down to 1 dimension in time
    data_source = data_source.tail(time=1)
    # re-label the time coordinate
    data_source = data_source.assign_coords(
        time=(["time"], [opa_self.time.init_time_stamp], data_source.time.attrs)
    )

    # final stat will be the digest cum array
    final_stat = getattr(opa_self.statistics, "digests_cum")

    dm = xr.Dataset(
        data_vars=dict(
            # need to add variable attributes
            [(str(data_source.name), (data_source.dims, final_stat, data_var_attrs))],
        ),
        coords=dict(data_source.coords),
        attrs=data_set_attrs,
    )

    return dm
