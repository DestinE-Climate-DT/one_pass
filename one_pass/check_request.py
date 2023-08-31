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

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'stat' : some_stat, "
            "corresponding to the statistic you require see docs for details"
        ) from exc

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

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'stat_freq' : "
            " some_freq, see docs for details"
        ) from exc

    try:
        getattr(request, "output_freq")
        output_freq = request.output_freq

        if output_freq == "continuous":
            raise ValueError(
                "Can not put continuous as the output frequency, must specifcy "
                " frequency (e.g. monthly) for on-going netCDF files"
            )

        if output_freq not in output_freq_options:
            valid_values = ", ".join(output_freq_options)
            raise ValueError(
                f"The requested output_freq '{output_freq}' "
                f" is not supported, valid values are: {valid_values}"
            )

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'output_freq' :"
            " some_freq, see docs for details"
        ) from exc
        
    if output_freq == "monthly" and stat_freq == "weekly":
        raise KeyError (
            "Can not set output_freq equal to monthly if stat_freq"
            " is equal to weekly, as months are not wholly divisable by "
            "weeks."
        )

    try:
        getattr(request, "time_step")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'time_step' : "
            " time_step in minutes of data, see docs for details"
        ) from exc

    try:
        getattr(request, "variable")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'variable' : "
            " variable of interest, see docs for details"
        ) from exc

    try:
        getattr(request, "percentile_list")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'percentile_list' : "
            " list of percentiles. If not required for your statistc set "
            " 'percentile_list : None"
        ) from exc

    try:
        getattr(request, "thresh_exceed")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'thresh_exceed' : "
            "threshold. If not required for your statistic set "
            "'thresh_exceed' : None"
        ) from exc

    try:
        getattr(request, "save")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'save' : "
            "True or False. Set to True if you want to save the "
            "completed statistic to netCDF"
        ) from exc

    try:
        getattr(request, "checkpoint")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'checkpoint' : "
            " True or False. Highly recommended to set to True so that "
            " the Opa will save summaries in case of model crash"
        ) from exc

    try:
        getattr(request, "out_filepath")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'output_file' : "
            " file/path/for/saving. If you do not want to save, and "
            " you have set save : False, here you can put None"
        ) from exc 

    try:
        getattr(request, "checkpoint_filepath")

    except AttributeError as exc:
        raise KeyError(
            "config.yml must include key value pair 'checkpoint_file' : "
            " file/path/for/checkpointing. If you do not want to checkpoint, and"
            " you have set checkpoint : False, here you can put None"
        ) from exc

    if request.save:
        file_path = getattr(request, "out_filepath")
        if os.path.exists(os.path.dirname(file_path)):
            # check it points to a directory
            if os.path.isdir(file_path):
                pass
            else:
                raise ValueError(
                    "Please pass a file path for saving that does "
                    "not include the file name as this is created dynamically"
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
                    "Please pass a file path for checkpointing that "
                    " does not include the file name, as this is "
                    " created dynamically"
                )
        else:
            # if they have put a file name here it will drop it
            if os.path.isdir(file_path):
                os.mkdir(os.path.dirname(file_path))
                print("created new directory for checkpointing")
            else:
                raise ValueError("Please pass a valid file path for checkpointing")

    if request.stat == "thresh_exceed":
        if not hasattr(request, "thresh_exceed"):
            raise AttributeError("need to provide threshold of exceedance value")

    if request.stat == "percentile":
        if not hasattr(request, "percentile_list"):
            raise ValueError(
                "For the percentile statistic you need to provide "
                " a list of required percentiles, e.g. 'percentile_list' :"
                " [0.01, 0.5, 0.99] for the 1st, 50th and 99th percentile, "
                " if you want the whole distribution, 'percentile_list' : ['all']"
            )

        if request.percentile_list[0] != "all":
            for j in range(np.size(request.percentile_list)):
                if request.percentile_list[j] > 1:
                    raise ValueError(
                        'Percentiles must be between 0 and 1 or ["all"] '
                        " for the whole distribution"
                    )

    if request.stat == "bias_correction":
        if request.stat_freq != "daily":
            raise ValueError(
                "Must set stat_freq equal to daily when requesting"
                " data for bias correction"
            )

        if request.output_freq != "daily":
            raise ValueError(
                "Must set output_freq equal to daily when requesting"
                " data for bias correction"
            )
