"""Module to check incoming time stamp and figure out what
to do"""

from typing import List
import pandas as pd
import xarray as xr

from one_pass.convert_time import convert_time
from one_pass.convert_time import convert_time_append
from one_pass.checkpointing.remove_checkpoints import remove_checkpoints

def check_time_step(opa_self : object, weight : int, index : int,
                        time_stamp_list : List[pd.Timestamp]
                    ):
    """If the weight of the incoming data is greater than 1, (i.e. there
    is more than 1 time step), this will check if the time step between
    the incoming data matches that given in the request. If it doesn't
    a warning will be raised and the time step will be changed.

    Arguments
    -----------
    opa_self : obj. Opa class object containing request details
    weight : int. Weight of the incoming data chunk (number of time stamps)
    index : int. How far through the time list you are
    time_stamp_list : List[pd.Timestamps]. List of the incoming time stamps
            of the data
    """
    if weight > 1 and index < weight - 2:
        time_diff = int((time_stamp_list[index + 1] -
                        time_stamp_list[index]).total_seconds() / 60
                    )
        if opa_self.request.time_step != time_diff:
            opa_self.logger.warning(
                "The time step given in the request of %s "
                "minutes does not match the difference between "
                "the incoming time stamps of %s minutes."
                "The time step has been changed to %s.",
                opa_self.request.time_step, time_diff, time_diff
            )
            opa_self.request.time_step = time_diff

def check_n_data(opa_self : object):
    """Checks if the attribute n_data is already there

    Returns
    ---------
    n_data_att_exist : boolean flag to say if the attr is not None
    """
    if getattr(opa_self.time, "n_data") is None:
        n_data_att_exist = False
    else:
        n_data_att_exist = True

    return n_data_att_exist

def should_initialise(opa_self : object, time_stamp_min : int,
                        proceed : bool
                    ):
    """Checks to see if in the timestamp of the data is the 'first'
    in the requested statistic. If so it recommends to initalise
    everything if stat_freq is not continuous. If stat_freq is
    continuous then we also need to check if the statistics need
    to be reinitialised or not.

    Arguments
    ----------
    time_stamp_min : int. number of minutes of the incoming timestamp
            into the current requested freq
    proceed : bool. Boolean flag to say if the code should proceed

    Returns
    ---------
    proceed : bool. boolean flag to say code should proceed
    should_init_time : bool. boolean flag to say time should be
            initialised
    should_init_both : bool. boolean flag to say if both the time and
            the attributes should be initialised. If the stat_freq is
            continuous then it's possible that you just want to re-
            initalised the count and times but not the actual stat.
    """
    should_init_time = False
    should_init_both = False

    # this indicates that it's the first data otherwise time_stamp
    # will be larger

    if opa_self.request.stat_freq != "continuous":
        if time_stamp_min < opa_self.request.time_step:
            should_init_time = True
            should_init_both = True
            proceed = True
    else:
        # if n_data already exists then the statistic
        # has previously been initialised
        n_data_att_exist = check_n_data(opa_self)
        if n_data_att_exist:
            # Removed the = as this should be less
            if time_stamp_min < opa_self.request.time_step:
                should_init_time = True
                proceed = True
        else:
            should_init_time = True
            should_init_both = True
            proceed = True

    return proceed, should_init_both, should_init_time

def future_time_stamp(opa_self, min_diff : float, time_stamp : pd.Timestamp,
                        proceed : bool
                    ):
    """Called from compare_old_timestamp,Time stamp too far in the
    future. If this is only slighly into the future < (2*timestep),
    just throw a warning. If it's more into the future, throw error

    Arguments
    -----------
    opa_self : Opa_class
    min_diff : float. difference in minutes between the opa_self.time.time_stamp
            already in the class and the incoming time_stamp
    time_stamp : incoming pandas time stamp
    proceed : bool. boolean Flag that checks if it's ok to proceed

    Returns
    ---------
    proceed : will be True if the new time stamp is less than twice
            the expected gap in minutes. If True, it will re-set opa_self.
            time_stamp but will pass a warning. It will be False if the
            time gap is more than 2 times the time_step and will give
            an error.
    """
    if abs(min_diff) < 2 * opa_self.request.time_step:
        opa_self.logger.warning(
            f"Time gap of {min_diff} between {opa_self.time.time_stamp} and "
            f"{time_stamp} is larger than the expected difference, of "
            f"{opa_self.request.time_step} but not more than 2*{opa_self.request.time_step}"
            f". Has the time step changed slightly? The OPA will carry on."
        )
        proceed = True

    else:
        raise ValueError(
            f"Time gap of {min_diff} between {opa_self.time.time_stamp} and "
            f"{time_stamp} is larger than the expected difference, of "
            f"{opa_self.request.time_step}. It seems that a time stamp has been "
            "missed."
        )

    return proceed

def past_time_stamp(opa_self : object, min_diff : float,
                        time_stamp : pd.Timestamp
                    ):
    """ Called from compare_old_timestamp. Called if the incoming
    time stamp is in the past. Here we don't look at time append, only
    the current stat_freq. If the incoming time stamp is further back in
    time than the start of the stat_freq it will reset the variables
    n_data and count. If the incoming time stamp is further back in time
    than the old time_stamp but within the current stat_freq, it will
    pass here and be caught in a later check 'already_seen'. 

    Arguments
    ---------
    opa_self : Opa class
    min_diff : float. difference in minutes between incoming time stamp and
            opa_self.time.time_stamp
    time_stamp : pd.Timestamp. Time stamp of the incoming data
    """
    if opa_self.request.stat_freq != "continuous":
        # first check the number of mins into the statistic the 'old'
        # time stamp is
        time_stamp_min_old = convert_time(
            time_word=opa_self.request.stat_freq,
            time_stamp_input=opa_self.time.time_stamp
        )[1]

    else:
        time_stamp_min_old = convert_time(
            time_word=opa_self.request.output_freq,
            time_stamp_input=opa_self.time.time_stamp
        )[1]

    # here it's gone back to before the stat_freq it was previously
    # calculating so reset attributes
    if abs(min_diff) > time_stamp_min_old:
        if opa_self.append.time_append == 1:
            opa_self.logger.info(
                "Incoming time stamp %s is further back in time than "
                "both the previously seen time stamp %s "
                "and the current stat_freq being calculated. As stat_freq "
                "is equal to output_freq (so no appending) the checkpoint "
                "file (if checkpointing is true) has been removed and the "
                "time variables n_data, count, time_stamp have been reset.",
                time_stamp, opa_self.time.time_stamp,
            )
            remove_checkpoints(opa_self)
        elif opa_self.request.stat_freq == "continuous":
            opa_self.logger.info(
                "Incoming time stamp %s is further back in time than "
                "the previously seen time stamp %s. As the stat_freq "
                "is continuous it is not possible to roll back this stat "
                "so the checkpoint file (if checkpointing is true) has "
                "been removed and the time variables n_data, count, time_stamp "
                "have been reset.",
                time_stamp, opa_self.time.time_stamp,
            )
            remove_checkpoints(opa_self)
        else:
            opa_self.logger.info(
                "Incoming time stamp %s is further back in time than "
                "both the previously seen time stamp %s "
                "and the current stat_freq being calculated. As stat_freq "
                "is less to output_freq (so appending data) the checkpoint "
                "file will be dealt with later and only variables n_data, "
                "count and time_stamp have been reset.", time_stamp,
                opa_self.time.time_stamp,
            )
        for attr in ("n_data", "count", "time_stamp"):
            setattr(opa_self.time, attr, None)
    # else: if it just goes backwards slightly inside the stat you're
    # already computing, it will either re-int later
    # or it will be caught by 'already seen' later on

def should_roll_back_time_append(opa_self, time_stamp_min : int,
            time_stamp : pd.Timestamp, time_stamp_tot_append : int,
            proceed : bool
        ):
    """ Called from compare_old_timestamp. Called if the incoming
    time stamp is in the past and time_append is greater than 1 meaning
    it's appending data to a xr.Dataset. This functions checks if it
    needs to 'roll back' this appended Dataset.
    
    Arguments
    ---------
    opa_self : Opa class
    time_stamp_min : int. number of minutes the current time stamp is 'into'
            the requested stat_freq.
    time_stamp : pd.Timestamp. the incoming pandas time stamp.
    time_stamp_tot_append : int. the number of 'stat_freq' in units of
            min of the time stamp already completed (only used for
            appending data)
    proceed : boolean flag to say if it's ok to proceed or not
    """
    # first check if you've gone even further back than
    # the first statistic in the time_append data set
    if (time_stamp < opa_self.append.first_append_time_stamp or
            time_stamp_tot_append == 0
        ):
        opa_self.logger.info(
            "Incoming time stamp %s is either further back in time than, "
            "equal to, or within the first stat for the current "
            "checkpoint time stamp %s for the start of the %s "
            "xr.Dataset containing %s statistics. The checkpoint "
            "file has therefore been removed and the appended "
            "attributes reset.",
            time_stamp,
            opa_self.append.first_append_time_stamp,
            opa_self.request.output_freq,
            opa_self.request.stat_freq
        )
        opa_self.append.remove_time_append(opa_self)
    elif ((time_stamp_tot_append / opa_self.time.stat_freq_min)
            < opa_self.append.count_append
        ):
        should_init = should_initialise(
            opa_self, time_stamp_min, proceed
        )[1]
        if should_init:
            # rolling back the time append calculation
            # now you want to check if it's gone back an exact
            # amount i.e. it's gone back to the beginning of a
            # new stat or mid way through
            # if it's initalising it's fine, roll back
            gap = int(opa_self.append.count_append - (
                    time_stamp_tot_append / opa_self.time.stat_freq_min
                ))
            new_start = int(time_stamp_tot_append / opa_self.time.stat_freq_min)
            opa_self.statistics.final_cum.isel(time=slice(0, new_start))
            if opa_self.request.stat == "hist":
                opa_self.statistics.final2_cum.isel(time=slice(0, new_start))
            opa_self.append.count_append = int(
                time_stamp_tot_append / opa_self.time.stat_freq_min
            )
            opa_self.logger.info(
                "Incoming time stamp %s is further back in time than "
                "the previous time stamp %s but not further back than "
                "%s which is the start of the current %s output "
                "xr.Dataset containing %s statistics. As the incoming "
                "time stamp %s corresponds to the start of a new %s statistic,"
                "the final xr.Dataset is being rolled back by %s and "
                "count_append has been reset to %s", time_stamp,
                opa_self.time.time_stamp,
                opa_self.append.first_append_time_stamp,
                opa_self.request.output_freq,
                opa_self.request.stat_freq,
                time_stamp,opa_self.request.stat_freq,
                gap, opa_self.append.count_append
            )
        # can't roll back if this new time_stamp isn't the
        # start of a stat, so deleting
        else:
            opa_self.logger.info(
                "Incoming time stamp %s is further back in time than "
                "the previous time stamp %s but not further back than "
                "%s which is the start of the current %s output "
                "xr.Dataset containing %s statistics. As the incoming "
                "time stamp %s does not correspond to the start of a new "
                "%s statistic, the final xr.Dataset is being reset.",
                time_stamp, opa_self.time.time_stamp,
                opa_self.append.first_append_time_stamp,
                opa_self.request.output_freq,
                opa_self.request.stat_freq,
                time_stamp,
                opa_self.request.stat_freq
            )
            opa_self.append.remove_time_append(opa_self)

def compare_old_timestamp(opa_self : object, time_stamp : pd.Timestamp,
        time_stamp_min : int, time_stamp_tot_append : int, proceed : bool
    ):
    """Compares the incoming time_stamp against one that may already
    be there from a checkpoint. If no previous time stamp it won't do
    anything. If there is an old one (opa_self.time.time_stamp), it will
    compare the difference in time between the two time stamps.
    There are 4 options:
    1. Difference in time is equal to the time step.
            proceed = true
    2. Time stamp in the future. If this is only slighly into the
            future (2*timestep), just throw a warning if it's more into
            the future, throw error
    3. The time stamp is the same, this will just pass through and
            will be caught in a later check.
    4. Time stamp is in the past. This then depends on if there is
            a time_append option. If no, it will simply delete the
            checkpoint fileand carry on. If there is a time_append,
            it will check if it needs to 'roll back' this appended data
            set.
            
    Arguments
    ----------
    opa_self : Opa class
    time_stamp : pd.Timestamp. pandas time stamp of incoming data
    time_stamp_min : int. Pandas time stamp converted into minutes
            corresponding to how many minutes through the requested
            stat_freq you are
    time_stamp_tot_append : int. the number of 'stat_freq' in units of
            min of the time stamp already completed (only used for
            appending data)
    proceed : bool. boolean flag to say if you're happy to proceed.

    Returns
    ----------
    proceed : boolean flag to say if it's passed the checks.
    """
    # if n_data exisits it means that this is not the first entry into the
    # statistic.
    if opa_self.time.time_stamp is not None:
        # calculates the difference in the old and new time stamps in minutes
        min_diff = time_stamp - opa_self.time.time_stamp
        min_diff = min_diff.total_seconds() / 60

        # option 1, opa_self.request.time_step the time step directly before,
        # this is what you expect and you can carry on
        if min_diff == opa_self.request.time_step:
            proceed = True
        # option 2, it's a time step into the future more than the given
        # time step of the data, indicates data mising
        elif time_stamp > opa_self.time.time_stamp:
            proceed = future_time_stamp(opa_self, min_diff, time_stamp, proceed)

        # option 3, time stamps are the same, keep proceed as False
        elif time_stamp == opa_self.time.time_stamp:
            pass  # this will get caught in 'already seen'

        # option 4, it's a time stamp from way before, do you need to roll back?
        elif time_stamp < opa_self.time.time_stamp:
            past_time_stamp(opa_self, min_diff, time_stamp)

            if opa_self.append.time_append > 1:
                should_roll_back_time_append(opa_self, time_stamp_min, time_stamp,
                    time_stamp_tot_append, proceed)

    return proceed

def check_have_seen(opa_self, time_stamp_min : int):
    """Check if the OPA has already 'seen' the data done by comparing
    what the count of the current time stamp is against the actual
    count.

    Arguments
    --------
    opa_self : Opa class
    time_stamp_min : number of minutes the current time stamp is
            into the requested stat_freq
    Returns
    --------
    already_seen : boolean Flag to mark if the data has already
            been seen
    """
    if opa_self.time.count is not None:
        already_seen = bool(
                (time_stamp_min / opa_self.request.time_step) < opa_self.time.count
            )
    else:
        already_seen = False

    return already_seen

def check_time_stamp(opa_self, data_source : xr.DataArray, weight : int):
    """Function to check the incoming timestamps of the data and check if it is
    the first one of the required statistic. If there are multiple incoming
    timestamps, (weight > 1) it will loop through them. First the time stamp
    will pass through 'convert_time' which will convert the time stamp of the
    incoming data into the number of minutes into the requested statistic
    'time_stamp_min' and count the number of minutes in the requested frequency
    'stat_freq_min'. e.g. if the timestamp is 1990-01-01-01:00 and the stat_freq
    is "daily", time_stamp_min = 60 and stat_freq_min = 1440.

    This data will then pass through 'compare old timestamps' which
    will compare the incoming timestamps against any previous timestamps stored in a
    checkpoint file. This will check if there appears to be a gap in the data,
    if the time_stamp has gone backwards in time, or it's the next one that's
    expected.

    If this passes, the timestamp will be checked to see if it's
    the first in the required statistic. If so, it will initialise the function
    with the required variables. If not, the function will also check if it's
    either 'seen' the data before or if it should simply be skipped as it's not
    the correct data for the statistic. e.g. stat_freq = daily_noon and the first
    piece of data corresponds to midnight.

    Arguments
    ----------
    opa_self : Opa class
    data_source: Incoming xr.DataArray with associated timestamp(s)
    weight: the length along the time-dimension of the incoming array

    Returns
    ---------
    data_source : xr.DataArray. Potenitally modified in the time dimension if the
            first few time stamps don't correspond to the initial part of the statistic
    weight : int. Again potenitally modified weight if some of the time stamps in the
            original data source don't correspond to the statistic.
    already_seen : bool. Flag to signal that the data has already been passed into the
            statistic.
    n_data_att_exist : bool. Flag to signal that the attribute time.n_data is still None
            after the initialisation, meaning the incoming time stamp did not correspond
            to the required statistic and we're skipping the time stamp.
    time_stamp_list : List[pd.Timestamp]. List of the time stamps in the incoming data_
            source, potenitally modified if we are skipping some of the incoming data.

    Checks
    --------
    If it is not the first timestamp of the statistic, it will check:
    1. that the attribute n_data has already been assigned, otherwise will
            realise that this data doesn't correspond to this statistic
    2. If the first piece of incoming data doesn't correspond to the initial data,
            and weight is greater than 1, it will check the other incoming pieces
            of data to see if they correspond to the initial statistic
    """
    # assuming that incoming data has a time dimension
    time_stamp_sorted = sorted(data_source.time.data)
    time_stamp_list = [pd.to_datetime(x) for x in time_stamp_sorted]

    index = 0
    # boolean flag, don't exit the loop unless this is set to True
    proceed = False

    while (not proceed) and (index < weight):

        time_stamp = time_stamp_list[index]

        # quick check to make sure that the time step difference is what
        # you expect
        check_time_step(opa_self, weight, index, time_stamp_list)

        if opa_self.request.stat_freq != "continuous":
            (
                opa_self.time.stat_freq_min,
                time_stamp_min,
            ) = convert_time(opa_self.request.stat_freq, time_stamp, opa_self)
            (
                time_stamp_tot_append
            )   = convert_time_append(opa_self.request.stat_freq,
                                    opa_self.request.output_freq,
                                    time_stamp, opa_self
                                )

        else:
            # for continuous you want the output_freq to govern
            # n_data (number of samples to add before outputing)
            (
                opa_self.time.stat_freq_min,
                time_stamp_min
            ) = convert_time(
                time_word=opa_self.request.output_freq, time_stamp_input=time_stamp
            )
            # never need to append data for the continuous output
            time_stamp_tot_append = 0

        if opa_self.time.stat_freq_min < opa_self.request.time_step:
            raise ValueError("The given time_step in the request is too large for "
                                "the requested statistic"
                            )

        # check that the incoming timestamp is what you expect, it is the next
        # one in the sequence, proceed will change to True
        proceed = compare_old_timestamp(
            opa_self, time_stamp, time_stamp_min, time_stamp_tot_append, proceed
        )

        # checking that the time step input is a multiple of the stat freq
        opa_self.time.check_time_step_int(opa_self)

        # check if the time and attributes need to be initialised
        # proceed will change to True if it should be initialised
        proceed, should_init_both, should_init_time = should_initialise(
            opa_self, time_stamp_min, proceed
        )

        # if either is True initialise first the time
        if should_init_both or should_init_time:

            if opa_self.request.stat_freq == "continuous" and should_init_both is False:
                opa_self.logger.debug(
                    "Incoming time stamp %s corresponds to the start "
                    "of the %s ouput_freq for the continuous statistic "
                    "so initialising the time attributes for the new output_freq, "
                    "but not the actual statistic.",
                    time_stamp, opa_self.request.output_freq
                )
            opa_self.time.count = 0
            opa_self.time.time_stamp = time_stamp
            opa_self.time.initialise_time(opa_self, time_stamp_min,
                                            time_stamp_tot_append
                                        )
            if should_init_both:

                if opa_self.request.stat_freq == "continuous":
                    # for stat_freq = continuous, this will only happen the
                    # very first time
                    opa_self.logger.debug(
                        "Initiating the start of the continuous statistic "
                        "at time stamp %s. Continuous statistic will always "
                        "be initialised from the first time stamp given.",
                        time_stamp
                    )
                else:
                    opa_self.logger.debug(
                        "Incoming time stamp %s corresponds to the start "
                        "of the %s stat_freq so initialising the statistic values.",
                        time_stamp, opa_self.request.stat_freq
                    )
                opa_self.statistics.initialise_attrs(opa_self, data_source)

        # checks current time step compared to count
        already_seen = check_have_seen(opa_self, time_stamp_min)

        if already_seen:
            opa_self.logger.info(
                "Pass on this data at %s as it has already been "
                "seen and contributed to the statistic.", time_stamp
            )

        # this will change from False to True if it's just been initialised
        n_data_att_exist = check_n_data(opa_self)
        if n_data_att_exist is False:
            # here it hasn't initialsed the statistic, proceed is still
            # false and none of the other flags have been raised
            opa_self.logger.info(
                "Passing on this data at %s as it is not the "
                "initial data for the requested statistic.", time_stamp
            )
            # remove the year for 10 annual if it started half way through
            # this year
            opa_self.__dict__.pop("year_for_10annual", None)

        index = index + 1

    # it will enter this loop it's skipping the first few time steps that come
    # through but starts from mid way through the incoming data
    if index > 1 and proceed:
        index = index - 1
        # chops data from where it needs to start
        data_source = data_source.isel(time=slice(index, weight))
        time_stamp_list = time_stamp_list[index:]
        weight = weight - index

    return data_source, weight, already_seen, n_data_att_exist, time_stamp_list
