"""
Function to check if the config.dic has been
set correctly. 
Will check that all key value pairs are present 
and any setting required for certain statistics 
"""

import os

import numpy as np

# list of allowed options for statistic
stat_options = {
    "mean",
    "std",
    "var",
    "thresh_exceed",
    "min",
    "max",
    "percentile",
    "raw",
    "bias_correction",
    "sum",
}

# list of allowed options for statistic frequency
stat_freq_options = {
    "hourly",
    "3hourly",
    "6hourly",
    "12hourly",
    "daily",
    "daily_noon",
    "weekly",
    "monthly",
    "3monthly",
    "annually",
    "continuous",
}

# list of allowed options for output frequency
output_freq_options = {
    "hourly",
    "3hourly",
    "6hourly",
    "12hourly",
    "daily",
    "daily_noon",
    "weekly",
    "monthly",
    "3monthly",
    "annually",
}

def check_request(request):

    """
    Arguments
    ----------
    Incoming user request from dictionary or config.yml

    Returns
    ---------
    Error if there's something wrong with the request

    """
    try:
        # if time_append already exisits it won't overwrite it
        getattr(request, "stat")
        stat = request.stat

        if stat not in stat_options:
            valid_values = ", ".join(stat_options)
            raise ValueError(
                f"The requested stat '{stat}' is not supported, "
                f"valid values are: {valid_values}"
            )

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'stat' : some_stat, "
            f"corresponding to the statistic you require see docs for details"
        )

    try:
        # if time_append already exisits it won't overwrite it
        getattr(request, "stat_freq")
        stat_freq = request.stat_freq

        if stat_freq not in stat_freq_options:
            valid_values = ", ".join(stat_freq_options)
            raise ValueError(
                f"The requested stat_freq '{stat_freq}' is not supported, "
                f" valid values are: {valid_values}"
            )

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'stat_freq' : "
            f" some_freq, see docs for details"
        )

    try:
        getattr(request, "output_freq")
        output_freq = request.output_freq

        if output_freq == "continuous":
            raise ValueError(
                f"Can not put continuous as the output frequency, must specifcy "
                f" frequency (e.g. monthly) for on-going netCDF files"
            )

        if output_freq not in output_freq_options:
            valid_values = ", ".join(output_freq_options)
            raise ValueError(
                f"The requested output_freq '{output_freq}' "
                f" is not supported, valid values are: {valid_values}"
            )

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'output_freq' :"
            f" some_freq, see docs for details"
        )
        
    if output_freq == "monthly" and stat_freq == "weekly": 
        raise KeyError (
            f"Can not set output_freq equal to monthly if stat_freq"
            f" is equal to weekly, as months are not wholly divisable by "
            f"weeks."
        )

    try:
        getattr(request, "time_step")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'time_step' : "
            f" time_step in minutes of data, see docs for details"
        )

    try:
        getattr(request, "variable")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'variable' : "
            f" variable of interest, see docs for details"
        )

    try:
        getattr(request, "percentile_list")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'percentile_list' : "
            f" list of percentiles. If not required for your statistc set "
            f" 'percentile_list : None"
        )

    try:
        getattr(request, "thresh_exceed")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'thresh_exceed' : "
            f"threshold. If not required for your statistic set "
            f"'thresh_exceed' : None"
        )

    try:
        getattr(request, "save")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'save' : "
            f"True or False. Set to True if you want to save the "
            f"completed statistic to netCDF"
        )

    try:
        getattr(request, "checkpoint")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'checkpoint' : "
            f" True or False. Highly recommended to set to True so that "
            f" the Opa will save summaries in case of model crash"
        )

    try:
        getattr(request, "out_filepath")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'output_file' : "
            f" file/path/for/saving. If you do not want to save, and "
            f" you have set save : False, here you can put None"
        )

    try:
        getattr(request, "checkpoint_filepath")

    except AttributeError:
        raise KeyError(
            f"config.yml must include key value pair 'checkpoint_file' : "
            f" file/path/for/checkpointing. If you do not want to checkpoint, and"
            f" you have set checkpoint : False, here you can put None"
        )

    if request.save:
        file_path = getattr(request, "out_filepath")
        if os.path.exists(os.path.dirname(file_path)):
            # check it points to a directory
            if os.path.isdir(file_path):
                pass
            else:
                raise ValueError(
                    f"Please pass a file path for saving that does "
                    f"not include the file name as this is created dynamically"
                )
        else:
            if os.path.isdir(file_path):
                os.mkdir(os.path.dirname(file_path))
                print("created new directory for saving")
            else:
                raise ValueError("Please pass a valid file path for saving")

    if request.checkpoint:
        file_path = getattr(request, "checkpoint_filepath")
        if os.path.exists(os.path.dirname(file_path)):
            # check it points to a directory
            if os.path.isdir(file_path):
                pass
            else:
                raise ValueError(
                    f"Please pass a file path for checkpointing that "
                    f" does not include the file name, as this is "
                    f" created dynamically"
                )
        else:
            # if they have put a file name here it will drop it
            if os.path.isdir(file_path):
                os.mkdir(os.path.dirname(file_path))
                print("created new directory for checkpointing")
            else:
                raise ValueError("Please pass a valid file path for checkpointing")

    if request.stat == "thresh_exceed":
        if hasattr(request, "thresh_exceed") == False:
            raise AttributeError("need to provide threshold of exceedance value")

    if request.stat == "percentile":
        if hasattr(request, "percentile_list") == False:
            raise ValueError(
                f"For the percentile statistic you need to provide "
                f" a list of required percentiles, e.g. 'percentile_list' :"
                f" [0.01, 0.5, 0.99] for the 1st, 50th and 99th percentile, "
                f" if you want the whole distribution, 'percentile_list' : ['all']"
            )

        if request.percentile_list[0] != "all":
            for j in range(np.size(request.percentile_list)):
                if request.percentile_list[j] > 1:
                    raise ValueError(
                        f'Percentiles must be between 0 and 1 or ["all"] '
                        f" for the whole distribution"
                    )

    if request.stat == "bias_correction":
        if request.stat_freq != "daily":
            raise ValueError(
                f"Must set stat_freq equal to daily when requesting"
                f" data for bias correction"
            )

        if request.output_freq != "daily":
            raise ValueError(
                f"Must set output_freq equal to daily when requesting"
                f" data for bias correction"
            )

    return
