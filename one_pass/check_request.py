"""
Function to check if the config.dic has been
set correctly. 
Will check that all key value pairs are present 
and any setting required for certain statistics 
"""

import numpy as np 
import os 

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
}

# list of allowed options for statistic frequency 
stat_freq_options = {
    "hourly",
    "3hourly",
    "6hourly",
    "12hourly",
    "daily",
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
                f"The requested stat '{stat}' is not supported, \
                valid values are: {valid_values}"
            )
    
    except AttributeError: 
        raise KeyError(
            "config.yml must include key value pair 'stat' : some_stat, \
            corresponding to the statistic you require see docs for details"
        )
    
    try:
        # if time_append already exisits it won't overwrite it 
        getattr(request, "stat_freq") 
        stat_freq = request.stat_freq
                
        if stat_freq not in stat_freq_options:
            valid_values = ", ".join(stat_freq_options)
            raise ValueError(
                f"The requested stat_freq '{stat_freq}' is not supported, \
                valid values are: {valid_values}"
            )
    
    except AttributeError: 
        raise KeyError(
            f"config.yml must include key value pair 'stat_freq' : \
            some_freq, see docs for details"
        )
    
    try:
        getattr(request, "output_freq")
        output_freq = request.output_freq
                
        if output_freq == "continuous":
            raise ValueError(
                f"Can not put continuous as the output frequency, must specifcy \
                frequency (e.g. monthly) for on-going netCDF files"
            )       
            
        if output_freq not in output_freq_options:
            valid_values = ", ".join(output_freq_options)
            raise ValueError(
                f"The requested output_freq '{output_freq}' \
                is not supported, valid values are: {valid_values}"
            )
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'output_freq' :\
            some_freq, see docs for details"
        )

    try:
        getattr(request, "time_step") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'time_step' : \
            time_step in minutes of data, see docs for details"
        )
        
    try:
        getattr(request, "variable") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'variable' : \
            variable of interest, see docs for details"
        )
    
    try:
        getattr(request, "percentile_list") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'percentile_list' : \
            list of percentiles. If not required for your statistc set \
            'percentile_list : None"
        )
        
    try:
        getattr(request, "threshold_exceed") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'threshold_exceed' : \
            threshold. If not required for your statistic set \
            'threshold_exceed : None"
        )
        
    try:
        getattr(request, "save") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'save' : \
            True or False. Set to True if you want to save the \
            completed statistic to netCDF"
        )
        
    try:
        getattr(request, "checkpoint") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'checkpoint' : \
            True or False. Highly recommended to set to True so that \
            the Opa will save summaries in case of model crash"
        )
        
    try:
        getattr(request, "out_filepath") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'output_file' : \
            file/path/for/saving. If you do not want to save, and \
            you have set save : False, here you can put None"
        )
        
    try:
        getattr(request, "checkpoint_filepath") 
    
    except AttributeError: 
        raise KeyError (
            f"config.yml must include key value pair 'checkpoint_file' : \
            file/path/for/checkpointing. If you do not want to checkpoint, and \
            you have set checkpoint : False, here you can put None"
        )
        
    #TODO: need to include making the directory if it doesn't exist 
    if(request.save): 
        file_path = getattr(request, "out_filepath") 
        
        if os.path.exists(file_path): 
            # check it points to a directory 
            if(os.path.isdir(file_path)):
                pass 
            else: 
                raise ValueError(
                    f"Please pass a file path for saving that does \
                    not include the file name as this is created dynamically"
                )
        else:
            os.mkdir(file_path)
            print('created new directory for saving')
            #raise ValueError("Please pass a valid file path for saving")
        
    #TODO: need to include making the directory if it doesn't exist 
    if(request.checkpoint): 
        file_path = getattr(request, "checkpoint_filepath")
        
        if os.path.exists(file_path): 
            # check it points to a directory 
            if(os.path.isdir(file_path)):
                pass 
            else: 
                raise ValueError(
                    f"Please pass a file path for checkpointing that \
                    dooe not include the file name as this is \
                    created dynamically"
                ) 
        else:
            os.mkdir(file_path)
            print('created new directory for checkpointing')
            #raise ValueError("Please pass a valid file path for checkpoint")
        
    
    if(request.stat == "thresh_exceed"):
        if (hasattr(request, "threshold") == False):
            raise AttributeError(
                'need to provide threshold of exceedance value'
            )
        
    if(request.stat == "percentile"):
        if (hasattr(request, "percentile_list") == False):
            raise ValueError(
                f'For the percentile statistic you need to provide \
                a list of required percentiles, e.g. "percentile_list" :\
                [0.01, 0.5, 0.99] for the 1st, 50th and 99th percentile, \
                if you want the whole distribution, "percentile_list" : ["all"]'
            )
        
        if (request.percentile_list[0] != "all"):
            for j in range(np.size(request.percentile_list)):
                if(request.percentile_list[j] > 1): 
                    raise ValueError(
                        'Percentiles must be between 0 and 1 or ["all"] \
                        for the whole distribution'
                    )
                    
        if(request.stat == "bias_correction"): 
            if (request.stat_freq != "daily"):
                raise ValueError(
                    f'Must set stat_freq equal to daily when requesting \
                        data for bias correction'
                )
        
            if (request.output_freq != "daily"):
                raise ValueError(
                    f'Must set output_freq equal to daily when requesting \
                        data for bias correction'
                )
        
    return 
            