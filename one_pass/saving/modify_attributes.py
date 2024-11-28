""""modifiy attributes"""

from datetime import datetime
import xarray as xr

def assign_new_attributes(data_set_attr : dict,
                          data_var_attr : dict,
                          new_attr_str : str):
    """Actual function that takes the updated attribtue strings and
    modifies the metadata copy

    Returns
    ---------
    data_var_attr : modified attributes corresponding
            to the variable xr.DataArray. Attributes now include history about
            the updates to the digests.
    data_set_attr : modified attributes corresponding
            to the overall xr.Dataset. If a Dataset was originally passed the
            OPA will return a Dataset with both the overall attributes modified
            and the attributes for the variable.
    """
    # update attributes for the dataset
    hist = data_set_attr.get("history", "")
    # check that there is a new line at the end of the current history
    # but won't make a new line of the current field is empty
    if not hist.endswith("\n") and hist != "":
        hist += "\n"
    hist += new_attr_str
    data_set_attr.update({"history": hist})

    # update attributes for the data variable
    hist = data_var_attr.get("history", "")
    # check that there is a new line at the end of the current history
    # but won't make a new line of the current field is empty
    if not hist.endswith("\n") and hist != "":
        hist += "\n"
    hist += new_attr_str
    data_var_attr.update({"history": hist})

def get_datetime_str():
    """Sets the str with the current date to be included in the history
    attribute
    """
    current_time = datetime.now()
    date_time = datetime.fromtimestamp(current_time.timestamp())
    str_date_time = date_time.strftime("%Y-%m-%d %H:%M:%S OPA :")

    return str_date_time

def update_metadata_for_bc_digests(opa_self):
    """Updates metadata corresponding the bias-correction digest output.
    The bias-correction outputs 3 files, and one of them is monthly
    pickle files that contain the t-digests. The digests include the means
    or sums of the selected varaible over a daily frequency. This
    function is called from bias_correction.py

    Arguments
    ----------
    opa_self : Opa class
    """
    str_date_time = get_datetime_str()

    if opa_self.request.variable not in opa_self.fixed.precip_options:
        word = "means"
    else:
        word = "sums"

    new_attr_str = str(
        str_date_time
        + f" daily {word} added to the monthly digest for "
        + opa_self.request.stat + " calculated using one_pass algorithm\n"
    )

    data_set_attrs = opa_self.data_set_info.data_set_attr.copy()
    data_var_attrs = opa_self.data_set_info.data_var_attr.copy()

    assign_new_attributes(
        data_set_attrs,
        data_var_attrs,
        new_attr_str
    )

    return data_set_attrs, data_var_attrs

def update_raw_data_attributes(opa_self, data_source : xr.DataArray):
    """If stat : "raw" or "bias_correction" then the OPA will
    output the raw data without computing any statistics. Here
    we convert to xr.DataArray into an xr.Dataset in and then
    add an atribute explaining opa history.
    
    Attributes
    ----------
    opa_self : Opa class
    data_source : incoming raw data
    
    Returns
    ----------
    dm : final xr.Dataset containing the raw data and modified
            attributes
    """
    str_date_time = get_datetime_str()
    new_attr_str = str(
        str_date_time +
        " raw data at native temporal resolution saved by one_pass algorithm\n"
    )

    data_set_attrs = data_source.attrs.copy()
    try:
        data_source = getattr(data_source, opa_self.request.variable)
        data_var_attrs = data_source.attrs.copy()
        assign_new_attributes(
            data_set_attrs, data_var_attrs, new_attr_str
        )
        data_source = data_source.assign_attrs(data_var_attrs)
        dm = data_source.to_dataset(dim=None, name=opa_self.request.variable)
        dm = dm.assign_attrs(data_set_attrs)

    except AttributeError:
        data_var_attrs = data_set_attrs.copy()
        assign_new_attributes(
            data_set_attrs, data_var_attrs, new_attr_str
        )
        data_source = data_source.assign_attrs(data_set_attrs)
        dm = data_source.to_dataset(dim=None, name=data_source.name)
        dm = dm.assign_attrs(data_set_attrs)

    return dm

def update_attributes(opa_self, bc_mean : bool = False):
    """Gathers final data and meta data for the final xarray Dataset
    and writes data attributes of the Dataset depending on the statistic
    requested.

    Arguments
    ----------
    opa_self : Opa class
    bc_mean : Flag to state that the final data output is for daily aggregations
            created for the bias correction

    Returns
    ---------
    self.data_var_attr = modified attributes corresponding to the variable
            xr.DataArray. Attributes now include history attribute related
            to OPA.
    AND/OR
    self.data_set_attr = modified attributes corresponding to the overall
            xr.Dataset. If a Dataset was originally passed the OPA will return
            a Dataset with both the overall attributes modified and the
            attributes for the variable.
    """
    str_date_time = get_datetime_str()

    # this is the flag to extract the mean value for the bias_correction
    if bc_mean:
        if opa_self.request.variable not in opa_self.fixed.precip_options:
            new_attr_str = str(
                    str_date_time + " "
                    + opa_self.request.stat_freq +
                    " mean calculated using one-pass algorithm\n"
                )

        else:
            new_attr_str = str(
                    str_date_time + " "
                    + opa_self.request.stat_freq +
                    " sum calculated using one-pass algorithm\n"
                )
    elif opa_self.request.stat_freq == "continuous":
        new_attr_str = (
            f"{str_date_time} {opa_self.request.stat_freq} "
            f"{opa_self.request.stat} calculated using one_pass algorithm \n"
            "This xr.Dataset shows the rolling summary at "
            f"{opa_self.time.init_time_stamp}. The statistic started at "
            f"{opa_self.time.init_count_time_stamp} \n"
        )
    else:
        new_attr_str = str(
            str_date_time + " "
            + opa_self.request.stat_freq + " " + opa_self.request.stat +
            " calculated using one_pass algorithm\n"
        )

    assign_new_attributes(
    opa_self.data_set_info.data_set_attr,
    opa_self.data_set_info.data_var_attr,
    new_attr_str
    )
