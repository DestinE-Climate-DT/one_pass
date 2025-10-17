"""Module for handling raw data passthrough without statistical aggregation."""

from typing import TYPE_CHECKING

import os
import xarray as xr

from one_pass.saving.create_file_names import create_raw_file_name
from one_pass.statistics.finalize import get_now_str

if TYPE_CHECKING:
    from one_pass.opa import Opa


def save_raw_output_nc(opa, dm_raw, filename, convert_to_float32: bool = False):
    """Save raw data to NetCDF file.

    Saves unprocessed raw data with updated attributes to NetCDF format.
    This function is called from the raw_data module when stat='raw'.

    Parameters
    ----------
    opa : Opa
        OPA instance containing save configuration.
    dm_raw : xr.Dataset
        Raw data with modified attributes to be saved.
    filename : str
        Base filename for the output file.
    """
    file_name = os.path.join(
        opa.request.save_filepath,
        f"{filename}_" f"{opa.request.variable}_raw_data.nc",
    )

    # Optional float64 -> float32 conversion
    if convert_to_float32:
        for var_name in dm_raw.data_vars:
            if dm_raw[var_name].dtype == "float64":
                dm_raw[var_name] = dm_raw[var_name].astype("float32")

    dm_raw.to_netcdf(path=file_name, mode="w")
    dm_raw.close()


def assign_new_attributes(data_set_attr: dict, data_var_attr: dict, new_attr_str: str):
    """Update metadata dictionaries with new history attribute string.

    Modifies both dataset-level and variable-level attributes in place,
    appending new history information.

    Parameters
    ----------
    data_set_attr : dict
        Dataset-level attributes dictionary (modified in place).
    data_var_attr : dict
        Variable-level attributes dictionary (modified in place).
    new_attr_str : str
        History string to append to both attribute dictionaries.

    Notes
    -----
    This function modifies the input dictionaries in place rather than
    returning them.
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


def update_raw_data_attributes(opa: "Opa", data_source: xr.DataArray):
    """Add OPA metadata to raw data and convert to Dataset format.

    For raw data output (stat='raw'), converts the DataArray to a Dataset
    and adds history attributes documenting OPA processing.

    Parameters
    ----------
    opa : Opa
        OPA instance with request configuration.
    data_source : xr.DataArray
        Input raw data to process.

    Returns
    -------
    xr.Dataset
        Dataset containing the raw data with updated metadata attributes
        documenting OPA processing.
    """
    str_date_time = get_now_str()
    new_attr_str = str(
        str_date_time
        + " raw data at native temporal resolution saved by one_pass algorithm\n"
    )

    data_set_attrs = data_source.attrs.copy()
    try:
        data_source = data_source[opa.request.variable]
        data_var_attrs = data_source.attrs.copy()
        assign_new_attributes(data_set_attrs, data_var_attrs, new_attr_str)
        data_source = data_source.assign_attrs(data_var_attrs)
        dm = data_source.to_dataset(dim=None, name=opa.request.variable)
        dm = dm.assign_attrs(data_set_attrs)
    except KeyError:
        data_var_attrs = data_set_attrs.copy()
        assign_new_attributes(data_set_attrs, data_var_attrs, new_attr_str)
        data_source = data_source.assign_attrs(data_set_attrs)
        dm = data_source.to_dataset(dim=None, name=data_source.name)
        dm = dm.assign_attrs(data_set_attrs)

    return dm


def check_raw(opa: "Opa", data_source):
    """Process raw data without statistical computation.

    Called when the user requests stat='raw'. Instead of computing statistics,
    this function saves the incoming data with its original time dimension
    and updated metadata attributes.

    Parameters
    ----------
    opa : Opa
        OPA instance containing configuration and save settings.
    data_source : xr.DataArray
        Incoming raw data to be processed and optionally saved.

    Returns
    -------
    xr.Dataset
        Raw data converted to Dataset format with updated attributes.
    """
    filename = create_raw_file_name(data_source)
    convert_to_float32 = opa.as_float32

    # this will convert the dataArray back into a dataSet with the metadata
    # of the xr.Dataset and include a new 'history' attribute saying that
    # it's saving raw data for the OPA along with time stamps
    dm_raw = update_raw_data_attributes(opa, data_source)

    if opa.request.save:
        save_raw_output_nc(
            opa,
            dm_raw,
            filename,
            convert_to_float32=convert_to_float32,
        )

    return dm_raw
