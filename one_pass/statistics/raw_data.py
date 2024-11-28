""""Module to deal with raw data"""

from one_pass.saving.create_file_names import create_raw_file_name
from one_pass.saving.modify_attributes import update_raw_data_attributes
from one_pass.saving.save_final import save_raw_output_nc

def check_raw(opa_self, data_source, weight):
    """Function is called if the user has requested stat: 'raw'
    or 'bias correction'. If 'raw' they do not want to compute
    any statstic over any frequency, we will simply save the
    incoming data with the given time dimension. For bias-correction
    will also save the raw data but if the data span is longer than
    one day, it will chop the data up into daily files.
    
    Attributes
    -----------
    opa_self : Opa class
    data_source : incoming raw data (if stat : bias_correction, this
            will have been chopped to run until the end of the day)
    weight : length of the raw data time dimension
    """
    final_time_file_str = create_raw_file_name(data_source, weight)

    # this will convert the dataArray back into a dataSet with the metadata
    # of the xr.Dataset and include a new 'history' attribute saying that
    # it's saving raw data for the OPA along with time stamps
    dm_raw = update_raw_data_attributes(opa_self, data_source)

    if opa_self.request.save:
        save_raw_output_nc(opa_self, dm_raw, final_time_file_str)

    return dm_raw

def check_raw_for_bc(opa_self, data_source, weight, how_much_left):
    """Saves the raw data for the bias correction. There is a small
    difference in how raw data is dealt with for a 'raw' request vs
    for 'bias_correction'. As the bias-correction deals with daily
    aggregations, if the raw data spans a new day it will chop the data
    and save the first half here and the second half during the recursive
    call. This is because it would be too hard to deal with the raw
    data in one way and the daily aggregate in another.

    Arguments
    ----------
    opa_self : Opa class
    data_source : the incoming data
    weight : the length of the time dimension of the incoming data
    how_much_left : how many more time stamps are required to
            fill the statistic.

    Returns
    ---------
    dm_raw : the raw data with the updated attributes potentially
            chopped in time.
    """
    if how_much_left >= weight:
        # update rolling statistic with weight
        dm_raw = check_raw(opa_self, data_source, weight)

    # this will span over the new statistic
    elif how_much_left < weight:
        # extracting time until the end of the statistic
        data_source_left = data_source.isel(time=slice(0, how_much_left))
        dm_raw = check_raw(opa_self, data_source_left, how_much_left)

    return dm_raw
