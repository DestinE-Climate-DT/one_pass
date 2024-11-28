"""Function to calculate the statistics for the one pass algorithm
"""
from typing import List
import logging
from pandas import Timestamp
import numpy as np
import xarray as xr
import tqdm
from one_pass.checkpointing.write_checkpoint import write_checkpoint

def update_continuous_count(opa_self, weight):
    """If the statistic frequency is continuous
    then you want to have another attribute that counts
    the continuous count, even after the statistics has
    been saved and returned at the frequency of output_freq

    Arguments
    ----------
    weight : weight (length of time dimension) of the incoming
            time chunk

    Returns
    ---------
    temp_count : the temporary updated count used in some
            statistics
    opa_self.time.count_continuous : continuous count of ALL data
            pointsthat have contributed to the statistic
    """
    if opa_self.request.stat_freq == "continuous":
        opa_self.time.count_continuous += weight
        temp_count = opa_self.time.count_continuous
    else:
        temp_count = opa_self.time.count

    return temp_count

def remultiply_varience(opa_self : object):
    """Problem occurs when stat_freq is continuous and stat
    is variance, as when you output the variance you divide
    by (n-1). As after output the variance at the output_freq
    the variance needs to be re-multipled by (n-1) before
    we can update the rolling summary.

    Returns
    ---------
    opa_self.statistics.var_cum : remultipled by (n-1) where n is
            the continuous count defined above.
    """
    if opa_self.request.stat_freq == "continuous":
        if opa_self.time.count_continuous > 0:
            if opa_self.time.count == 0:
                opa_self.statistics.var_cum = opa_self.statistics.var_cum * (
                    opa_self.time.count_continuous - 1
                )

def two_pass_sum(data_source):
    """Computes normal sum using numpy two pass

    Arguments
    ----------
    data_source : incoming data chunk with a time
            dimension greater than 1

    Returns
    ---------
    temp: two-pass sum over the data chunk
    """
    ax_num = data_source.get_axis_num("time")
    temp = np.sum(data_source, axis=ax_num, dtype=np.float64, keepdims=True)

    return temp

def two_pass_mean(data_source : xr.DataArray):
    """Computes normal mean using numpy two pass

    Arguments
    ----------
    data_source : incoming data chunk with a time
            dimension greater than 1

    Returns
    ---------
    temp: two-pass mean over the data chunk
    """
    ax_num = data_source.get_axis_num("time")
    temp = np.mean(data_source, axis=ax_num, dtype=np.float64, keepdims=True)

    return temp

def two_pass_var(data_source : xr.DataArray):
    """Computes normal variance using numpy two pass,
    setting ddof = 1 for sample variance

    Arguments
    ----------
    data_source : xr.DataArray. incoming data chunk with a time
            dimension greater than 1

    Returns
    ---------
    temp: xr.DataArray two-pass variance over the data chunk
    """
    ax_num = data_source.get_axis_num("time")
    temp = np.var(
        data_source, axis=ax_num, dtype=np.float64, keepdims=True, ddof=1
    )

    return temp

def update_mean(opa_self, data_source, weight):
    """Computes one pass mean with weight corresponding to the number
    of timesteps being added. Also updates count.

    Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.time.count : updated with weight
    opa_self.statistics.mean_cum : updated cumulative mean
    """
    opa_self.time.count += weight
    temp_count = update_continuous_count(opa_self, weight)

    if weight > 1:
        # compute two pass mean first
        data_source = two_pass_mean(data_source)

    opa_self.statistics.mean_cum  = (opa_self.statistics.mean_cum + weight * \
        (data_source.data - opa_self.statistics.mean_cum) / (
        temp_count
    ))

def update_var(
        opa_self : object , data_source : xr.DataArray, weight : int
    ):
    """Computes one pass variance with weight corresponding to
    the number of timesteps being added. It does not update
    the count as this is done in update_mean which update_var
    calls.

    Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.statistics.var_cum : updated cumulative variance. If n < n_data
            and the statistic is not complete, this is equal to M2
            (see docs). If n == n_data and enough samples have been
            addded var_cum is divded by (n-1) to get actual variance.
    """
    remultiply_varience(opa_self)

    # storing 'old' mean temporarily
    old_mean = opa_self.statistics.mean_cum

    if weight == 1 :
        update_mean(opa_self, data_source, weight)
        opa_self.statistics.var_cum = opa_self.statistics.var_cum + weight * (
            data_source.data - old_mean) * (data_source.data - opa_self.statistics.mean_cum)

    else:
        # two-pass mean
        temp_mean = two_pass_mean(data_source)
        # two pass variance
        temp_var = (two_pass_var(data_source)) * (weight - 1)
        # see paper Mastelini. S
        if opa_self.request.stat_freq != "continuous":
            opa_self.statistics.var_cum = (
                opa_self.statistics.var_cum
                + temp_var.data
                + np.square(old_mean - temp_mean.data)
                * ((opa_self.time.count * weight) / (opa_self.time.count + weight))
            )
        else:
            opa_self.statistics.var_cum = (
                opa_self.statistics.var_cum
                + temp_var.data
                + np.square(old_mean - temp_mean.data)
                * (
                    (opa_self.time.count_continuous * weight)
                    / (opa_self.time.count_continuous + weight)
                )
            )

        update_mean(opa_self, data_source, weight)

    if opa_self.time.count == opa_self.time.n_data:
        # using sample variance NOT population variance
        if opa_self.request.stat_freq != "continuous":
            if (opa_self.time.count - 1) != 0:
                opa_self.statistics.var_cum = (
                    opa_self.statistics.var_cum / (opa_self.time.count - 1)
                )
        else:
            if (opa_self.time.count_continuous - 1) != 0:
                opa_self.statistics.var_cum = (
                    opa_self.statistics.var_cum/ (opa_self.time.count_continuous - 1)
                )

def update_sum(
        opa_self : object , data_source : xr.DataArray, weight : int
    ):
    """Computes one pass sum with weight corresponding to the number
    of timesteps being added. Also updates count.

    Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.time.count : updated with weight
    opa_self.statistics.sum_cum : updated cumulative sum
    """
    if weight > 1:
        data_source = two_pass_sum(data_source)

    opa_self.statistics.sum_cum = np.add(
        opa_self.statistics.sum_cum, data_source.data, dtype=np.float64
    )

    opa_self.time.count += weight
    update_continuous_count(opa_self, weight)

def update_min_internal(opa_self, data_source, data_source_time):
    """Updates the axis of attributes min_cum and timings and 
    updates the array data_source with any values in min_cum that 
    are smaller

     Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk. If weight > 1, this has already
            been reduced using the two-pass minimum along the time
            dimension.
    data_source_time : xarray of same shape as data_source but with
            the time dimension always equal to 1. This holds the time
            stamps of all the minimum values for that incoming chunk.
            If weight == 1, this is just an array of the current 
            time stamp.

    Returns
    ---------
    data_source : updated to contain minimum values between incoming
            data_source and opa_self.statistics.min_cum. NOTE: xr.where
            is counter-intuative. The below line will do, where data_source
            is greater than opa_self.statistics.min_cum, replace with
            opa_self.statistics.min_cum
    data_source_time : uupdated to contain the time stamp of the minimum
            values between incoming data_source and opa_self.statistics.min_cum.
    opa_self.statistics.min_cum["time"] : time dimension of min_cum updated
    opa_self.statistics.timings_cum["time"] : time dimension of timings_cum updated
    """
    opa_self.statistics.min_cum["time"] = data_source.time
    opa_self.statistics.timings_cum["time"] = data_source.time
    data_source_time = data_source_time.where(
            data_source < opa_self.statistics.min_cum, opa_self.statistics.timings_cum
        )
    # this gives the new opa_self.statistics.min_cum number when the  condition
    # is FALSE (location at which to preserve the objects values)
    data_source = data_source.where(data_source < opa_self.statistics.min_cum,
                                    opa_self.statistics.min_cum)

    return data_source, data_source_time

def update_min(
        opa_self : object , data_source : xr.DataArray, weight : int
    ):
    """ Main function for OPA min. Finds the cumulative minimum values
    of the data along with an array of timesteps corresponding to the
    minimum values. Updates count with weight.

     Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk.
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.statistics.min_cum : updated rolling summary of minimum values
    opa_self.statistics.timings_cum : updated rolling summary of time stamps corresponding
            to the minimum values
    opa_self.time.count : updated with weight
    """
    if weight == 1:
        timestamp = np.datetime_as_string((data_source.time.values[0]))
        data_source_time = xr.zeros_like(data_source)
        data_source_time = data_source_time.where(data_source_time != 0, timestamp)

    else:
        ax_num = data_source.get_axis_num("time")
        timings_cum = data_source.time
        min_index = data_source.argmin(axis=ax_num, keep_attrs=False)
        data_source = np.amin(data_source, axis=ax_num, keepdims=True)
        # now this will have dimensions 1,lat,lon
        data_source_time = xr.zeros_like(data_source)

        for i in range(0, weight):
            timestamp = np.datetime_as_string((timings_cum.values[i]))
            data_source_time = data_source_time.where(min_index != i, timestamp)

    if opa_self.request.stat_freq != "continuous":
        if opa_self.time.count > 0:
            data_source, data_source_time = update_min_internal(
                opa_self, data_source, data_source_time
            )
    else:
        if opa_self.time.count_continuous > 0:
            data_source, data_source_time = update_min_internal(
                opa_self, data_source, data_source_time
            )

    # convert to datetime64 for saving
    data_source_time = data_source_time.astype("datetime64[ns]")

    opa_self.time.count += weight
    update_continuous_count(opa_self, weight)

    # running this way around as Array type does not have the function .where,
    # this only works for data_array
    opa_self.statistics.min_cum = data_source
    opa_self.statistics.timings_cum = data_source_time

def update_max_internal(
        opa_self : object ,
        data_source : xr.DataArray,
        data_source_time : xr.DataArray
    ):
    """Updates the axis of attributes min_cum and timings and 
    updates the array data_source with any values in min_cum that 
    are smaller

     Arguments
    ---------
    opa_self : Opa class
    data_source : xr.DataArray. incoming data chunk. If weight > 1,
            this has already been reduced using the two-pass maximum along
            the time dimension.
    data_source_time : xr.DataArray of same shape as data_source but with
            the time dimension always equal to 1. This holds the time
            stamps of all the maximum values for that incoming chunk.
            If weight == 1, this is just an array of the current 
            time stamp.

    Returns
    ---------
    data_source : updated to contain maximum values between incoming
            data_source and opa_self.statistics.max_cum. NOTE: xr.where
            is counter-intuative. The below line will do, where data_source
            isgreater than opa_self.statistics.max_cum, replace with
            opa_self.statistics.max_cum
    data_source_time : uupdated to contain the time stamp of the maximum
            values between incoming data_source and opa_self.statistics.max_cum.
    opa_self.statistics.max_cum["time"] : time dimension of max_cum updated
    opa_self.statistics.timings_cum["time"] : time dimension of timings_cum updated
    """
    opa_self.statistics.max_cum["time"] = data_source.time
    opa_self.statistics.timings_cum["time"] = data_source.time
    data_source_time = data_source_time.where(
            data_source > opa_self.statistics.max_cum, opa_self.statistics.timings_cum
        )
    # this gives the new opa_self.statistics.max_cum number when the  condition is
    # FALSE (location at which to preserve the objects values)
    data_source = data_source.where(
            data_source > opa_self.statistics.max_cum, opa_self.statistics.max_cum
        )

    return data_source, data_source_time

def update_max(
        opa_self : object , data_source : xr.DataArray, weight : int
    ):
    """ Main function for OPA max. Finds the cumulative maximum values
    of the data along with an array of timesteps corresponding to the
    maximum values. Updates count with weight.

     Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk.
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.statistics.max_cum : updated rolling summary of maximum values
    opa_self.statistics.timings_cum : updated rolling summary of time stamps corresponding
            to the maximum values
    opa_self.time.count : updated with weight
    """
    if weight == 1:
        timestamp = np.datetime_as_string((data_source.time.values[0]))
        data_source_time = xr.zeros_like(data_source)
        data_source_time = data_source_time.where(data_source_time != 0, timestamp)
    else:
        ax_num = data_source.get_axis_num("time")
        timings_cum = data_source.time
        max_index = data_source.argmax(axis=ax_num, keep_attrs=False)
        #opa_self.max_index = max_index
        data_source = np.amax(data_source, axis=ax_num, keepdims=True)
        # now this will have dimensions 1,incoming grid
        data_source_time = xr.zeros_like(data_source)

        for i in range(0, weight):
            timestamp = np.datetime_as_string((timings_cum.values[i]))
            data_source_time = data_source_time.where(max_index != i, timestamp)

    if opa_self.request.stat_freq != "continuous":
        if opa_self.time.count > 0:
            data_source, data_source_time = update_max_internal(
                opa_self, data_source, data_source_time
            )
    else:
        if opa_self.time.count_continuous > 0:
            data_source, data_source_time = update_max_internal(
                opa_self, data_source, data_source_time
            )

    # convert to datetime64 for saving
    data_source_time = data_source_time.astype("datetime64[ns]")

    opa_self.time.count += weight
    update_continuous_count(opa_self, weight)
    opa_self.statistics.max_cum = data_source
    opa_self.statistics.timings_cum = data_source_time

def update_threshold(
        opa_self : object , data_source : xr.DataArray, weight : int
    ):
    """ Creates an array with the frequency that a threshold has
    been exceeded. Updates count with weight.

     Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk.
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.statistics.thresh_exceed_cum : updated rolling summary of number
            of times a threshold has been exceeded
    opa_self.time.count : updated with weight
    """
    # loop is for all the of thresholds
    for index, _ in enumerate(opa_self.request.thresh_exceed):
        # the sum is for the weight
        opa_self.statistics.thresh_exceed_cum[:,index,:,:] += np.sum(
                np.where(
                        data_source.data < abs(opa_self.request.thresh_exceed[index]),
                        0, 1
                    ),
                axis = 0, keepdims=True
                )

    opa_self.time.count += weight
    update_continuous_count(opa_self, weight)

def update_max_iams(opa_self : object, window_sum : np.array, i : int):
    """
    Function specfically for the iams statistic as it doesn't include
    the timings. Updating the incoming window_sum with any values in the
    rolling maximum

    Arguments
    ---------
    opa_self : Opa class
    window_sum : np.array. the sum of the rolling window corresponding to
            theduration length.
    i : int. the index of the durations loop

    Returns
    ---------
    opa_self.statistics.iams_cum : attribute updated with the maximum
            value between the opa_self and the rolling max.
    """
    # extract the rolling max for each duration
    rolling_max = opa_self.statistics.iams_cum[i,:]

    opa_self.statistics.iams_cum[i,:] = np.where(
        window_sum < rolling_max, rolling_max, window_sum
        )

def extract_durations(opa_self : object, i : int):
    """Extracts key information relating to each duration length.

    Arguments
    ---------
    opa_self : Opa class
    i : the index of the durations loop

    Returns
    ---------
    n_data_duration : the number of data pieces requred for each
            duration
    count_duration : the current count for each duration
    count_duration_full : the current full count for each duration
            the comparision between opa_self.time.count and count_duation
    """
    n_data_duration = int(
        getattr(opa_self.iams, "n_data_durations")[i]
    )
    count_duration = int(
        getattr(opa_self.iams, "count_durations")[i]
    )
    count_duration_full = int(
        getattr(opa_self.iams, "count_durations_full")[i]
    )

    return n_data_duration, count_duration, count_duration_full

def one_pass_iams(opa_self : object, full_length : int):
    """One-pass iams statistic implementation. Loops through all
    the requested durations (lengths in minutes) and finds the
    window, which is a section of the time-series opa_self.iams.
    rolling_data with the length of that duration. This could sit
    in the middle of the stored time-series chunk or it could span
    the end and the beginning.

    Arguments
    ---------
    opa_self : Opa class
    full_length : int. the length of the time-series required to be
            stored to cover all of the durations. It is the length
            of the longest duration.

    Returns
    ---------
    count_duration : the current count is updated by 1.
    count_duration_full : the current full count for each duration
            (the comparision between opa_self.time.count and count_duation)
            is updated by 1.
    """
    for i in range(np.size(opa_self.iams.durations)):
        #extract key information about that duration
        n_data_duration, count_duration, count_duration_full = (
            extract_durations(opa_self, i)
        )

        # re-setting count_duration back to 0 first time it hits this
        if count_duration >= full_length :
            count_duration = 0

        # only sum over data that has been filled (opa_self.time.count not
        # yet updated)
        if (count_duration_full + n_data_duration) <= opa_self.time.count:

            #not yet looping back to the start of the rolling data array
            if (count_duration + n_data_duration) <= full_length:

                window_sum = opa_self.iams.rolling_data[
                    count_duration : count_duration +
                    n_data_duration, :
                    ].sum(axis=0, keepdims = True)

            else:
                data_left = full_length - count_duration

                window_sum = opa_self.iams.rolling_data[
                    count_duration:, :
                    ].sum(axis=0, keepdims = True)

                # starting from the beginning
                window_sum = window_sum + opa_self.iams.rolling_data[
                    0 :n_data_duration - data_left, :
                    ].sum(axis=0, keepdims = True)

            count_duration += 1
            count_duration_full += 1

            # weight will be 1 here because looping through each time step
            update_max_iams(opa_self, window_sum, i)

        # end of duration loop (i)
        getattr(
            opa_self.iams, "count_durations"
        )[i] = count_duration
        getattr(
            opa_self.iams, "count_durations_full"
        )[i] = count_duration_full

def update_iams(
        opa_self : object , data_source : xr.DataArray, weight : int
    ):
    """This function updates the statistic iams. It starts by updating
    the variable opa_self.iams.rolling_data, which is a tempory data store
    of time steps with a time dimension equal to the data required
    for the longest duration.
    Once this is updated, it loops through all the durations required
    and takes the summations over each duration from this iams.rolling_data.
    The moving window for each duration is continuously updated with
    counts specific to each duration monitoring how far through you are.
    The window sum is then passed to a find maximum function where the max
    value for each duration will be updated. This will store the max
    value of all the rolling windows of duration x over the year.
    NOTE: this requires stat_freq and output_freq to be 'annually'.

    Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk.
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.iams.rolling_data : temporary storage incoming data updated.
    opa_self.time.count : updated with weight.
    """
    # length of the iams.rolling_data array
    full_length = int(opa_self.iams.n_data_durations[-1])
    # remainder of full count divided by length of array
    # = to how far through the iams.rolling_data you are
    # how much left of this iams.rolling_data needs to be filled
    # before starting from the beginning again
    loop_count = np.mod(opa_self.time.count, full_length)

    for j in range(weight):
        opa_self.iams.rolling_data[loop_count : loop_count + 1, :] = \
            data_source[j:j+1,:]
        one_pass_iams(opa_self, full_length)
        opa_self.time.count += 1
        loop_count = np.mod(opa_self.time.count, full_length)

def update_tdigest_large_weight(
        opa_self : object, data_source : xr.DataArray, weight : int
    ):
    """Sequential loop that updates the digest for each grid point
    when weight is greater than 1. Needs to reshape in  a differnt
    way

    Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.statistics.digests_cum : each digest for each grid cell
            is updated with the new data
    """
    data_source_values = data_source.values.reshape((weight, -1))
    iterable = range(opa_self.data_set_info.size_data_source_tail)
    if opa_self.logger.isEnabledFor(logging.DEBUG):
        for j in tqdm.tqdm(
                iterable,
                desc="Updating digests"
            ):
            # using crick or pytdigest
            getattr(opa_self.statistics, "digests_cum")[j].update(
            data_source_values[:, j]
        )
    else:
        for j in iterable:
            # using crick or pytdigest
            getattr(opa_self.statistics, "digests_cum")[j].update(
                data_source_values[:, j]
            )

def update_tdigest(
        opa_self : object, data_source : xr.DataArray, weight : int = 1
    ):
    """Sequential loop that updates the digest for each grid point.
    If the statistic is not bias correction, it will also update the
    count with the weight. For bias correction, this is done in the
    daily means calculation.

    Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.statistics.digests_cum : each digest for each grid cell is updated with
            the new data
    opa_self.time.count : updated with weight unless the statistic is bias-corr
            in which case that is updated in daily means
    """
    if weight == 1:

        if opa_self.request.stat == "bias_correction":
            data_source_values = np.reshape(
                data_source, opa_self.data_set_info.size_data_source_tail
            )
        else:
            data_source_values = np.reshape(
                data_source.values, opa_self.data_set_info.size_data_source_tail
            )

        iterable = range(opa_self.data_set_info.size_data_source_tail)
        if opa_self.logger.isEnabledFor(logging.DEBUG):
            for j in tqdm.tqdm(
                    iterable,
                    desc="Updating digests"
                ):
                getattr(opa_self.statistics, "digests_cum")[j].update(
                    data_source_values[j]
            )

        else:
        # this is looping through every grid cell
            for j in iterable:
                getattr(opa_self.statistics, "digests_cum")[j].update(
                    data_source_values[j]
                )

    else:
        update_tdigest_large_weight(opa_self, data_source, weight)

    if opa_self.request.stat != "bias_correction":
        opa_self.time.count += weight
        update_continuous_count(opa_self, weight)

def update(opa_self, data_source, weight=1):
    """Update function that will send data to the correct
    statistic.

    Arguments
    ---------
    opa_self : Opa class
    data_source : incoming data chunk
    opa_self.precip_options : different variable names for precipitation
            that require summation as opposed to mean for the
            bias-correction
    weight : length of time dimension of incoming data chunk

    Returns
    ---------
    opa_self.request.stat : each statistic rolling summary is updated
    opa_self.time.count : updated with weight
    """
    if opa_self.request.stat == "mean":
        update_mean(opa_self, data_source, weight)

    elif opa_self.request.stat in ("var", "std"):
        update_var(opa_self, data_source, weight)

    elif opa_self.request.stat == "min":
        update_min(opa_self, data_source, weight)

    elif opa_self.request.stat == "max":
        update_max(opa_self, data_source, weight)

    elif opa_self.request.stat == "thresh_exceed":
        update_threshold(opa_self, data_source, weight)

    elif opa_self.request.stat in ("percentile", "histogram"):
        update_tdigest(opa_self, data_source, weight)

    elif opa_self.request.stat == "sum":
        update_sum(opa_self, data_source, weight)

    elif opa_self.request.stat == "iams":
        update_iams(opa_self, data_source, weight)

    elif opa_self.request.stat == "bias_correction":
        # bias correction requires daily means or sums
        if opa_self.request.variable not in opa_self.fixed.precip_options:
            # want daily means
            update_mean(opa_self, data_source, weight)
        else :
            update_sum(opa_self, data_source, weight)

def update_statistics(
        opa_self: object ,
        weight : int,
        time_stamp_list : List[Timestamp],
        data_source : xr.DataArray
    ):
    """Main function call. First calculates how many more data points are
    needed to fill the statistic. If it's under it will pass all the new
    data to the statistc that needs updating and write a checkpoint (if
    required). If there's more data than required to fill the statistic,
    it will pass only what's required then then return how much is left.

    Arguments
    ---------
    opa_self : Opa class
    weight : length of time dimension of incoming data chunk
    time_stamp_list : list of all the time stamps in the incoming
            data_source
    data_source : incoming data chunk
    opa_self.precip_options : different variable names for precipitation
            that require summation as opposed to mean for the
            bias-correction

    Returns
    ---------
    how_much_left : how much is let of your statistic to fill
    opa_self : the updated Opa class with the new statistics
    """
    how_much_left = opa_self.time.n_data - opa_self.time.count
    # will not span over new statistic
    if how_much_left >= weight:
        # moving the time stamp to the last of the set
        opa_self.time.time_stamp = time_stamp_list[-1]
        # update rolling statistic with weight
        update(opa_self, data_source, weight)
        opa_self.logger.debug(
            "Statistic has just been updated with %s time stamps. \n"
            "It has completed %s out of the %s required to fill "
            "the statistic.", weight, opa_self.time.count,
            opa_self.time.n_data
        )
        if opa_self.request.checkpoint:
            # this will not be written when count == ndata
            if (opa_self.request.stat_freq != "continuous" and
                    opa_self.time.count < opa_self.time.n_data):
                # setting append checkpoint flag = False, so that
                # it won't write uncessary checkpoints if
                # stat_freq < output_freq
                if opa_self.append.time_append > 1:
                    opa_self.append.append_checkpoint_flag = False
                write_checkpoint(opa_self)

            elif opa_self.request.stat_freq == "continuous":
                # this will be written when count == ndata because
                # still need the checkpoint for continuous
                write_checkpoint(opa_self)

    # this will span over the new statistic
    elif how_much_left < weight:

        # extracting time until the end of the statistic
        data_source_left = data_source.isel(time=slice(0, how_much_left))
        # CHECK moving the time stamp to the last of the set
        opa_self.time.time_stamp = time_stamp_list[how_much_left]
        # update rolling statistic with weight required to fill the stat
        # still need to finish the statistic (see below)
        update(opa_self, data_source_left, how_much_left)
        opa_self.logger.debug(
            "Statistic has just been updated with %s time stamps. \n"
            "It has completed %s out of the %s required to fill "
            "the statistic.", how_much_left, opa_self.time.count,
            opa_self.time.n_data
        )
    return how_much_left
