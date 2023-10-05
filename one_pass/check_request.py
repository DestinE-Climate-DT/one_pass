"""
Function to check if the config.dic has been
set correctly. 
Will check that all key value pairs are present 
and any setting required for certain statistics 
"""

import os
import numpy as np
import warnings

required_keys_with_set_output = [
    "stat",
    "stat_freq",
]

required_keys_with_variable_output = [
    "time_step",
    "variable",
]

non_required_keys_with_set_output = [
    "output_freq",
    "save",
    "checkpoint",
]

non_required_keys_with_variable_output = [
    "checkpoint_filepath",
    "save_filepath", 
]

save_options = [
    "True",
    "False"
]

checkpoint_options = [
    "True",
    "False"
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
    "10annually",
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
    "10annually",
]

def missing_value(key, passed_key, valid_values):
    
    """
    Function to define the ValueError if the requested value in any 
    key : value pair is not in the supported list.
    
    """
    raise ValueError(
                f"The requested {key} : '{passed_key}' is not supported, "
                f"valid values are: {valid_values}"
            )
    
def missing_key(key, valid_values, exc):        
    
    """ 
    Function to define the KeyError if the one_pass request is missing 
    a required key : value pair which has a specfic list of valid values
    """
    raise KeyError(
            "Data request (python dictionary or config.yml) must include"
            f" the key, value pair {key} : {valid_values}, "
            "See docs for details."
        ) from exc
    
def variable_missing_key(key, exc):        
    
    """ 
    Function to define the KeyError if the one_pass request is missing 
    a required key : value pair wtihtout a set list of allowed values
    """

    raise KeyError(
            "Data request (python dictionary or config.yml) must include"
            f" the key '{key}' with an appropriate value pair. "
            "See docs for details."
        ) from exc
    
def missing_non_required_key(key, set_value):

    """
    Function to define warning if there was a non-required key value pair
    that was not passed in the request. The warning includes the value that
    the key value pair has been set to.
    """

    warnings.warn(
        "Data request (python dictionary or config.yml) did not include"
        f" {key}. {key} has been set to {set_value}.", UserWarning 
    )

def check_key_values(request, valid_options, key):
    
    """ 
    Checks for the attribute stat and stat_freq.
    If they exist it will check it's in the list of valid options.
    If they doesn't exist it will raise a KeyError. 

    """
    valid_options = globals()[valid_options]

    try:
        passed_key = getattr(request, key)

        if passed_key not in valid_options:
            missing_value(
                key, passed_key, valid_options
            )

    except AttributeError as exc:
        missing_key(key, valid_options, exc)

def check_variable_key_values(request, key):

    """ 
    Checks for the keys that have a variable value, given in list 
    'required_keys_with_variable_output'.
    If they doesn't exist it will raise a KeyError. 
    """

    try:
        getattr(request, key)

    except AttributeError as exc:
        variable_missing_key(key, exc)
        
def check_non_required_key_values(request, key):
    
    """ 
    Checks for the non-required keys in the request, given in list 
    'non_required_keys_with_output'.
    If they doesn't exist it will set them equal to their default
    value and raise a warning explaining this.
    
    Returns
    --------
    Potentially modified request 
    """
      
    try:
        getattr(request, key)
    except AttributeError:
        if key == "output_freq":
            request.__setattr__(key, request.stat_freq)
        else:
            request.__setattr__(key, True)
        set_value = getattr(request, key)
        missing_non_required_key(key, set_value)

def check_non_required_variable_key_values(request, key):
    
    """
    Function that checks the file paths for saving and checkpointing
    from list 'non_required_keys_with_variable_output'. 
    key == save_filepath or checkpoint_filepath. These keys are required
    if saving and checkpointing is True, but if they're set to False, 
    these keys are not required.
    
    """
    if getattr(request, key):

    # only need the below if save or checkpoint is equal to True
        key = f"{key}_filepath"
        try:
            file_path = getattr(request, key)
            if os.path.exists(os.path.dirname(file_path)):
            # check it points to a directory
                if os.path.isdir(file_path):
                    # working directory, great
                    pass
                else:
                    # try to reset the file_path
                    #print(os.path.dirname(file_path))
                    request.__setattr__(key, os.path.dirname(file_path))
                    file_path = getattr(request, key)
                    
                    #print(key)
                    #print(file_path)
                    
                    if os.path.isdir(file_path):
                        warnings.warn(
                        f"Removed file name from {key}, as this"
                        f" is created dynamically. Filepath is now {file_path}", 
                        UserWarning
                        )
                    else:
                        raise ValueError(
                            f"Please pass a file path for {key} that does "
                            "not include the file name as this is created dynamically"
                        )
            else:
                try: 
                    os.mkdir(os.path.dirname(file_path))
                    warnings.warn(
                        f"created the new directory {file_path}"
                        f" for {key}", UserWarning
                        )
                except:
                    raise ValueError(f"Please pass a valid file path for {key}")

        except AttributeError as exc:
            variable_missing_key(key, exc)
      
def key_error_freq_mix(output_freq, stat_freq):
    
    """
    Defines the KeyError raised if there is a mismatch between 
    the stat_freq and the output_freq
    """
    raise ValueError (
        f"Can not set output_freq equal to {output_freq} if stat_freq"
        f" is equal to {stat_freq}. Output_freq must always be greater "
        "than stat_freq."
    )

def mix_of_stat_and_output_freq(output_freq, stat_freq):

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

    if output_freq == "continuous" and stat_freq == "continuous":
        key_error_freq_mix(output_freq, stat_freq)
        
    elif stat_freq == "weekly":
        if output_freq in output_freq_weekly_options:
            key_error_freq_mix(output_freq, stat_freq)
    
    #TODO: include daily_noon
    
    index_value = stat_freq_options.index(stat_freq)
    if stat_freq != "continuous":
        for j in range(len(output_freq_options[0:index_value])):
            # test that output_freq is always the same or greater than 
            # stat_freq 
            if output_freq in stat_freq_options[0:j]:
                key_error_freq_mix(output_freq, stat_freq)


    
def check_thresh_exceed(request):
                
    if request.stat == "thresh_exceed":
        if not hasattr(request, "thresh_exceed"):
            raise AttributeError(
                "For the thresh_exceed statistic you need to provide "
                " the threshold value. This is set in a seperate key:value"
                "pair, 'thresh_exceed' : some_value, where some_value is" 
                "the threshold you wish to set"
            )

def check_histogram(request):
    if request.stat == "histogram":
        if not hasattr(request, "bins"):
            warnings.warn(
                "Optional key value 'bins' : 'int' or 'array_like'. "
                "If 'bins' is an int, it defines the number of equal width bins in "
                "the given range. If 'bins' is an array_like, the values define "
                "the edges of the bins (rightmost edge inclusive), allowing for "
                "non-uniform bin widths. If set to 'None', or not included at all "
                "it will default to 10.",
                UserWarning
            )

        if not hasattr(request, "range"):
            warnings.warn(
                "Optional key value 'range' : '[float, float]'. The lower and upper "
                "bounds to use when generating bins. If not "
                "provided, the digest bounds '[t.min(), t.max())]' are used. Note "
                "that this option is ignored if the bin edges are provided "
                "explicitly. ", UserWarning
            )

def check_percentile(request):

    if request.stat == "percentile":
        if not hasattr(request, "percentile_list"):
            warnings.warn(
                "key value pair 'percentile_list' has been set to ['all']."
                " For the percentile statistic you should provide"
                " a list of required percentiles, e.g. [0.01, 0.5, 0.99]"
                " for the 1st, 50th and 99th percentile,"
                " if you want the whole distribution, 'percentile_list' : ['all']",
                UserWarning
            )
            request.__setattr__("percentile_list", "['all']")

        if request.percentile_list is None:
            raise ValueError(
                        'Percentiles must be between 0 and 1 or ["all"] '
                        " for the whole distribution"
                    )

        if request.percentile_list[0] != "all":
            for j in range(np.size(request.percentile_list)):
                if request.percentile_list[j] > 1 or request.percentile_list[j] < 0:
                    raise ValueError(
                        'Percentiles must be between 0 and 1 or ["all"] '
                        " for the whole distribution"
                    )

def check_iams(request):
    
    if request.stat =="iams":
        if request.stat_freq != "annually":
            raise ValueError(
                'Must set stat_freq equal to annually when requesting'
                ' iams statistic')
        if request.output_freq != "annually":
            raise ValueError(
                'Must set output_freq equal to annually when requesting'
                ' iams statistic')

def check_bias_correction(request):
    
    """
    Check that if bias_correction has been selected as the statistic
    then both output_freq and stat_freq are set to daily.
    """

    if request.stat == "bias_correction":
        if  request.stat_freq != "daily" or request.output_freq != "daily":
            raise ValueError(
                "Must set stat_freq and output_freq equal to daily when"
                " requesting data for bias correction"
            )

def check_request(request):
    
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

        check_variable_key_values(request, element)
            
    for element in non_required_keys_with_set_output:
        # save, checkpoint, output_freq
        check_non_required_key_values(request, element)
        
        if element != "output_freq":

            check_non_required_variable_key_values(request, element)

    output_freq = request.output_freq
    stat_freq = request.stat_freq
    mix_of_stat_and_output_freq(output_freq, stat_freq)
    
    # check requirements for specific statistics
    check_thresh_exceed(request)
    check_histogram(request)
    check_percentile(request)
    check_iams(request)
    check_bias_correction(request)

    

