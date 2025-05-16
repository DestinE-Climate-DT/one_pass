"""Function to check if the config.dic has been
set correctly.
Will check that all key value pairs are present
and any setting required for certain statistics
"""

import os
from datetime import date

required_keys_with_set_output = [
    "stat",
    "stat_freq",
    "save",
    "checkpoint",
]

required_keys_with_variable_output = [
    "time_step",
    "variable",
]

non_required_keys_with_set_output = [
    "output_freq",
]

non_required_keys_with_variable_output = [
    "checkpoint_filepath",
    "save_filepath",
]

save_options = [
    True,
    False,
]

checkpoint_options = [
    True,
    False,
]

# list of allowed options for statistic
stat_options = [
    "mean",
    "std",
    "var",
    "thresh_exceed",
    "min",
    "max",
    "percentile",
    "histogram",
    "raw",
    "bias_correction",
    "sum",
    "iams",
]

# list of allowed options for statistic frequency
stat_freq_options = [
    "hourly",
    "2hourly",
    "3hourly",
    "6hourly",
    "12hourly",
    "daily",
    "daily_noon",
    "weekly",
    "monthly",
    "3monthly",
    "annually",
    "yearly",
    "10annually",
    "10yearly",
    "continuous",
]

# list of allowed options for output frequency
output_freq_options = [
    "hourly",
    "2hourly",
    "3hourly",
    "6hourly",
    "12hourly",
    "daily",
    "daily_noon",
    "weekly",
    "monthly",
    "3monthly",
    "annually",
    "yearly",
    "10annually",
]

ba_method_options = [
    "None",
    "multiplicative",
    "additive",
]

def missing_value(key, value, valid_values):
    """Function to define the ValueError if the requested value in any
    key : value pair is not in the supported list.
    """
    raise ValueError(
                f"The requested {key} : '{value}' is not supported, "
                f"valid values are: {valid_values}"
            )

def missing_key(key, valid_values, exc):
    """Function to define the KeyError if the one_pass request is missing
    a required key : value pair which has a specfic list of valid values
    """
    raise KeyError(
            "Data request (python dictionary or config.yml) must include"
            f" the key, value pair {key} : {valid_values}, "
            "See docs for details."
        ) from exc

def variable_missing_key(key, exc):
    """Function to define the KeyError if the one_pass request is missing
    a required key : value pair wtihtout a set list of allowed values
    """
    raise KeyError(
            "Data request (python dictionary or config.yml) must include"
            f" the key '{key}' with an appropriate value pair. "
            "See docs for details."
        ) from exc

def missing_non_required_key(key, set_value, logger):
    """
    Function to define warning if there was a non-required key value pair
    that was not passed in the request. The warning includes the value that
    the key value pair has been set to.
    """
    logger.warning(
        "Data request (python dictionary or config.yml) did not include"
        f" {key}. {key} has been set to {set_value}."
    )

def check_key_values(request, valid_options, key):
    """Checks for the attribute stat, stat_freq, save, checkpoint.
    If they exist it will check it's in the list of valid options.
    If they doesn't exist it will raise a KeyError.

    Attributes:
    ----------
    valid_options : lists of valid options for each key
    key : the key values that must be present in the request
            they will come 1 by 1. Given in list
            required_keys_with_set_output
    """
    valid_options = globals()[valid_options]

    # try to get the value of the key
    value = getattr(request, key)

    if value is not None:
        # if the key such as stat exists, check its value
        # is valid
        if value not in valid_options:
            missing_value(
                key, value, valid_options
            )

    else:
        # the key itself is missing from the request
        if key == "stat_freq":
            # only raise error if stat != raw
            israw = getattr(request, 'stat')
            if israw == "raw":
                pass
            else:
                missing_key(key, valid_options, KeyError)
        else:
            missing_key(key, valid_options, KeyError)

def check_variable_key_values(request, key):
    """Checks for the keys that have a variable value, given in list
    'required_keys_with_variable_output'.
    If they doesn't exist it will raise a KeyError.
    """
    passed_key = getattr(request, key)
    if passed_key is None:
        variable_missing_key(key, KeyError)

def check_non_required_key_values(request, logger, key):
    """Checks for the non-required keys in the request, given in list
    'non_required_keys_with_set_output'. which is output_freq
    If they doesn't exist it will set them equal to their default
    value and raise a warning explaining this.

    Arguments
    ---------
    key : members of list 'non_required_keys_with_set_output'
            which is just output_freq

    Returns
    --------
    Potentially modified request
    """

    value = getattr(request, key)
    if value is None:
        # only raise error if stat != raw
        israw = getattr(request, 'stat')
        if israw == 'raw':
            pass
        else:
            setattr(request, key, request.stat_freq)
            set_value = getattr(request, key)
            missing_non_required_key(key, set_value, logger)
    else:
        # check that the value is in the list
        if value not in output_freq_options:
            missing_value(key, value, output_freq_options)

def check_non_required_variable_key_values(request, logger, key):
    """Function that checks the file paths for saving and checkpointing
    from list 'non_required_keys_with_variable_output'.
    key == save_filepath or checkpoint_filepath. These keys are required
    if saving and checkpointing is True, but if they're set to False,
    these keys are not required.
    """
    file_path = getattr(request, key)

    if file_path is not None:
        if os.path.exists(os.path.dirname(file_path)):
        # check it points to a directory
            if os.path.isdir(file_path):
                # working directory, great
                pass
            else:
                # try to reset the file_path
                setattr(request, key, os.path.dirname(file_path))
                file_path = getattr(request, key)

                if os.path.isdir(file_path):
                    logger.warning(
                    f"Removed file name from {key}, as this"
                    f" is created dynamically. Filepath is now {file_path}"
                    )
                else:
                    raise ValueError(
                        f"Please pass a file path for {key} that does "
                        "not include the file name as this is created dynamically"
                    )
        else:
            try:
                os.mkdir(os.path.dirname(file_path))
                logger.warning(
                    f"created the new directory {file_path}"
                    f" for {key}"
                    )
            except Exception as exc:
                raise ValueError(f"Please pass a valid file path for {key}"
                                ) from exc
    else:
        variable_missing_key(key, AttributeError)

def key_error_freq_mix(output_freq, stat_freq):
    """""
    Defines the KeyError raised if there is a mismatch between
    the stat_freq and the output_freq
    """
    raise ValueError (
        f"Can not set output_freq equal to {output_freq} if stat_freq"
        f" is equal to {stat_freq}. Output_freq must always be greater "
        "than or the same as stat_freq."
    )

def mix_of_stat_and_output_freq(output_freq, stat_freq, request, logger):
    """
    Function to check the combination of stat_freq and
    output_freq.
    """
    output_freq_weekly_options = [
        "monthly",
        "3monthly",
        "annually",
        "10annually",
    ]

    stat_index = stat_freq_options.index(stat_freq)
    output_index = output_freq_options.index(output_freq)

    if output_index > stat_index + 3:
        if stat_freq == "hourly" and output_freq == "daily":
            pass
        else:
            logger.warning(
                f'Your set value of output_freq : {output_freq} '
                f'is significantly larger than your set value of stat_freq : '
                f'{stat_freq}. This will result in a final file size with a large '
                f'time dimension. We recommend you reduce your value of output_freq. '
                )

    if output_freq == "continuous" and stat_freq == "continuous":
        key_error_freq_mix(output_freq, stat_freq)

    elif stat_freq == "weekly":
        if output_freq in output_freq_weekly_options:
            key_error_freq_mix(output_freq, stat_freq)
    if stat_freq == "daily_noon":
        if output_freq == "daily":
            setattr(request, output_freq, stat_freq)
            logger.warning(
                'Changed output_freq from "daily" to "daily_noon" '
                " as stat_freq is set to daily_noon",
                )

    index_value = stat_freq_options.index(stat_freq)
    if stat_freq != "continuous":
        for j in range(len(output_freq_options[0:index_value])):
            # test that output_freq is always the same or greater than
            # stat_freq
            if output_freq in stat_freq_options[0:j]:
                key_error_freq_mix(output_freq, stat_freq)

def check_thresh_exceed(request, logger):
    """ Function that checks for the attribute thresh_exceed
    """
    if request.stat == "thresh_exceed":
        if not hasattr(request, "thresh_exceed"):
            raise AttributeError(
                "For the thresh_exceed statistic you need to provide "
                "the threshold value. This is set in a seperate key : value "
                "pair, 'thresh_exceed' : [some_value_1, some_value_2], "
                "where this is a list of exceedance values. This can be a "
                "single value."
            )
        if isinstance(request.thresh_exceed, str):
            raise ValueError(
                "The value for the threshold exceedance can not be a str. "
                "Please convert to a list format [some_value, some_value2]"
            )
        if not isinstance(request.thresh_exceed, list):
            logger.warning(
                "Values for threshold exceedance should be passed as a list. "
                f"The single value {request.thresh_exceed} has been converted "
                f"to the form [{request.thresh_exceed}]. To avoid this warning "
                "pass as a list."
            )
            setattr(request, "thresh_exceed", [request.thresh_exceed])

def check_histogram(request, logger):
    """ Function that checks for bins and raises warning about 
    default options 
    """
    if request.stat == "histogram":
        if not hasattr(request, "bins"):
            logger.warning(
                "Optional key value 'bins' : int or array_like. "
                "If 'bins' is an int, it defines the number of equal width bins in "
                "the given range. If 'bins' is an array_like, the values define "
                "the edges of the bins (rightmost edge inclusive), allowing for "
                "non-uniform bin widths. If set to 'None', or not included at all "
                "it will default to 10."
            )

        # if not hasattr(request, "range"):
        #     warnings.warn(
        #         "Optional key value 'range' : '[float, float]'. The lower and upper "
        #         "bounds to use when generating bins. If not "
        #         "provided, the digest bounds '[t.min(), t.max())]' are used. Note "
        #         "that this option is ignored if the bin edges are provided "
        #         "explicitly. ", UserWarning
        #     )

def percentile_warning(logger):
    """Function that issues warning about the incorrect values
    for the percentile list option
    """
    logger.warning(
        "key value pair 'percentile_list' has been set to [],"
        " without quotations, for the whole distribution."
        " For the percentile statistic you should provide"
        " a list of required percentiles, e.g. [0.01, 0.5, 0.99]"
        " for the 1st, 50th and 99th percentile, or"
        " if you want the whole distribution, 'percentile_list' : [] "
    )

def check_percentile(request, logger):
    """ Function that checks for percentile list """
    if request.stat != "percentile":
        return

    if not hasattr(request, "percentile_list") or request.percentile_list is None:
        percentile_warning(logger)
        setattr(request, "percentile_list", [])
        return

    if isinstance(request.percentile_list, str):
        raise ValueError(
            f"percentile_list {request.percentile_list} not a valid "
            "value. Values must be in a list and range between 0 and 1. "
            "For the whole distribution, put []"
        )

    if isinstance(request.percentile_list, list):
        if (
            not all(isinstance(element, (int, float)) for element in request.percentile_list)
            or any(element > 1 or element < 0 for element in request.percentile_list)
        ):
            raise ValueError(
                f"percentile_list {request.percentile_list} contains invalid "
                "values. Values must be in a list and range between 0 and 1. "
                "For the whole distribution, put []"
            )

def check_iams(request):
    """Function that checks that output and stat frequencies for iams """
    if request.stat =="iams":
        if request.stat_freq not in ("annually", "yearly"):
            raise ValueError(
                'Must set stat_freq equal to yearly/annually when requesting'
                ' iams statistic')
        if request.output_freq not in ("annually", "yearly"):
            raise ValueError(
                'Must set output_freq equal to yearly/annually when requesting'
                ' iams statistic')

def check_raw(request, logger):
    """Check that if raw has been selected as the statistic then the user knows that 
    the opa will not alter the time dimension and the variables "stat_freq" and 
    "output_freq" will be ignored.
    """
    if request.stat == "raw":
        logger.info(
                "You have selected raw data. The opa will save the raw data that it is "
                "passed, it will not alter the time dimension. The key value pairs "
                "'stat_freq' and 'output_freq' (if present) will be "
                "ignored in this request."
            )

def check_annually(request, logger):
    """Check if the input frequency is annually, will now change this to yearly
    and flag Future depcripiation warning.
    """
    if request.output_freq == "annually":
        setattr(request, "output_freq", "yearly")
        logger.warning(
            "The time request of 'annually' for output_freq will be "
            "depcripiated in future versions of the Opa. Please use the request "
            "'yearly' instead."
        )

    if request.stat_freq == "annually":
        setattr(request, "stat_freq", "yearly")
        logger.warning(
            "The time request of 'annually' for stat_freq will be "
            "depcripiated in future versions of the Opa. Please use the request "
            "'yearly' instead."
        )

def check_compression(request, logger):
    """Check whether cmpression has been passed, and defaults it to 1 in the case of 
    the stat being "histogram" or "percentile".
    """
    if request.stat in ("histogram", "percentile"):
        compression = request.compression
        if compression is None:
            logger.warning(
                "For 'histogram' or 'percentile' stats, not passing a compression "
                "results in a default compression of 1."
            )
        elif not isinstance(compression, (int, float)):
            raise TypeError(
                f"In request, compression should be a float, not {type(compression).__name__}"
            )


def check_legacy_bias_adjustment(request, logger):
    """For older ways to call bias adjustment, check that if bias adjustment
    has been selected, the correct values have been set.
    The stat must be equal to raw and an appropriate bias adjustment
    method must have been set.
    """
    if request.stat == "bias_correction":
        logger.warning(
            "request['stat'] = 'bias_correction' is now deprecated and will soon be removed."
            "Please, consider using request['bias_adjust'] = True."
        )
        if request.stat_freq != "daily" or request.output_freq != "daily":
            raise ValueError(
                "Must set stat_freq and output_freq equal to daily when"
                " requesting data for bias correction"
            )

    # try to get the value of the key
    try:
        value = getattr(request, "bias_adjustment")
        if value in ["True", True]:
            logger.warning(
                "request['bias_adjustment'] is now a deprecated key and will soon be removed."
                "Please, consider using request['bias_adjust'] = True."
            )
            if request.stat != "raw":
                raise ValueError(
                    "Currently, if bias adjustment has been set to True then stat has "
                    "to be equal to raw. The bias adjustment is only avaliable for "
                    "raw data."
                )

            try:
                method = getattr(request, "bias_adjustment_method")
                if method is not None:
                    if request.bias_adjustment_method not in ba_method_options:
                        logger.warning(
                            "The requested bias_adjustment method "
                            f"{request.bias_adjustment_method} is not supported. "
                            "It has been set to 'additive'. The valid values "
                            f"are: {ba_method_options}"
                        )
                        setattr(request, "bias_adjustment_method", None)
            except AttributeError:
                setattr(request, "bias_adjustment_method", None)
                logger.warning(
                    "If requesting bias adjustment then a bias adjustment method needs "
                    "to be set. This has been set to 'additive'. The possible values "
                    f"are {ba_method_options}."
                )
        else:
            logger.warning(
                f"{value} is not a valid value for bias_adjustment. If you want "
                "bias adjusted data, set bias_adjustment : True. Opa will ignore "
                "the bias adjustment field and compute the statistic requested."
            )
    except AttributeError:
        pass


def check_bias_adjustment(request, logger):
    """Check that if bias adjustment has been selected, the correct values have been set.
    The stat must be equal to raw and an appropriate bias adjustment
    method must have been set.
    """
    if not request.bias_adjust:
        return
    if request.ba_reference_dir is None:
        raise ValueError(
            "When bias adjusting, ba_reference_dir must be set."
        )
    if request.ba_agg_method not in ("sum", "mean"):
        raise ValueError(
            "When bias adjusting, ba_reference_dir must be set."
        )
    if request.ba_future_method not in ("additive", "multiplicative"):
        raise ValueError(
            "When bias adjusting, ba_reference_dir must be set."
        )
    try:
        date.fromisoformat(request.ba_future_start_date)
    except ValueError:
        raise ValueError(
            f"request.ba_future_start_date ({request.ba_future_start_date}) "
            "is not a valid date in YYYY-MM-DD format."
        )
    if request.ba_reference_dir is None:
        raise ValueError(
            "When bias adjusting, ba_reference_dir must be set."
        )


def check_request(request, logger):
    """
    Arguments
    ----------
    Incoming user request from dictionary or config.yml

    Returns
    ---------
    Error if there's something wrong with the request
    """
    for element in required_keys_with_set_output:

        valid_options = f'{element}_options'
        check_key_values(request, valid_options, element)

    for element in required_keys_with_variable_output:
        # this is variable and time step
        check_variable_key_values(request, element)

    for element in non_required_keys_with_set_output:
        # output_freq
        check_non_required_key_values(request, logger, element)

    for element in non_required_keys_with_variable_output:
        # Loop to check that save_filepath and checkpoint_filepath
        # are present, only if save or checkpoint is set to True

        # here checking that the 'save' and 'checkpoint' are True
        parts = element.split('_')
        before = parts[0]
        if getattr(request, before):
            check_non_required_variable_key_values(request, logger, element)

    if request.stat != 'raw':
        mix_of_stat_and_output_freq(
            request.output_freq, request.stat_freq, request, logger
            )

    # check requirements for specific statistics
    check_thresh_exceed(request, logger)
    check_histogram(request, logger)
    check_percentile(request, logger)
    check_iams(request)
    check_raw(request, logger)
    check_annually(request, logger)
    check_compression(request, logger)
    check_legacy_bias_adjustment(request, logger)
    check_bias_adjustment(request, logger)
