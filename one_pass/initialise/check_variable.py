"""Checks to see if the incoming data is in the correct format"""

import xarray as xr

def check_variable(opa_self : object, data_source : xr.Dataset):
    """Checks if the incoming data is an xarray DataArray.
    If it's a xr.Dataset, will convert to DataArray and store
    the meta data attributes.

    Returns
    --------
    data_source : xr.DataArray. If data source was an xr.DataSet it will
            extract the variable of interest.
    opa_self.data_set_info.data_set_attr : the attributes to be given to the
            final Dataset
    opa_self.data_set_info.data_var_attr : the attributes to be given to the
            variable in the final Dataset
    """
    try:
        # this means it a data_set
        getattr(data_source, "data_vars")
        # keeping the attributes of the full dataSet to give to
        # the final dataSet
        opa_self.data_set_info.data_set_attr = data_source.attrs.copy()
        try:
            # converts to a dataArray
            data_source = getattr(data_source, opa_self.request.variable)
            opa_self.data_set_info.data_var_attr = data_source.attrs.copy()
        except AttributeError as exc:
            raise ValueError(
                "If passing xr.Dataset need to provide the correct variable."
            ) from exc

    except AttributeError:
        opa_self.data_set_info.data_var_attr = data_source.attrs.copy()
        # if this has been called recursively, this way you're not
        # overwriting data_set_attr with the variables from the dataArray
        if opa_self.data_set_info.data_set_attr is None:
            # still extracting attributes from dataArray here
            opa_self.data_set_info.data_set_attr = data_source.attrs.copy()

    return data_source
