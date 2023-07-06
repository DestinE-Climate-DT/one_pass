
''''Function to check if the config.dic was correct '''

import numpy as np 

stat_options = {
    "mean",
    "std",
    "var",
    "thresh_exceed",
    "min",
    "max",
    "histogram", 
    "percentile",
    "raw",
}

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

output_freq_options = {
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
        
    try:
        getattr(request, "stat") # if time_append already exisits it won't overwrite it 
    
        stat = request.stat
        
        if stat not in stat_options:
            valid_values = ", ".join(stat_options)
            raise ValueError(f"The requested stat '{stat}' is not supported, valid values are: {valid_values}")
    
    except AttributeError: 
        raise Exception("config.yml must include key value pair 'stat' : some_stat, corresponding to the statistic you require see READ.md for details")
    
    try:
        getattr(request, "stat_freq") # if time_append already exisits it won't overwrite it 
    
        stat_freq = request.stat_freq
                
        if stat_freq not in stat_freq_options:
            valid_values = ", ".join(stat_freq_options)
            raise ValueError(f"The requested stat_freq '{stat_freq}' is not supported, valid values are: {valid_values}")
    
    except AttributeError: 
        raise Exception("config.yml must include key value pair 'stat_freq' : some_freq, see READ.md for details")
    
    try:
        getattr(request, "output_freq") # if time_append already exisits it won't overwrite it 
    
        output_freq = request.output_freq
                
        if output_freq not in output_freq_options:
            valid_values = ", ".join(output_freq_options)
            raise ValueError(f"The requested output_freq '{output_freq}' is not supported, valid values are: {valid_values}")
    
    except AttributeError: 
        raise Exception ("config.yml must include key value pair 'output_freq' : some_freq, see READ.md for details")

    try:
        getattr(request, "bias_correction") # if time_append already exisits it won't overwrite it 
    
    except AttributeError: 
        raise Exception ("config.yml must include key value pair 'bias_correction' : True or False, see READ.md for details")
    
    if(request.stat == "thresh_exceed"):
        if (hasattr(request, "threshold") == False):
            raise AttributeError('need to provide threshold of exceedance value')
        
    if(request.stat == "percentile"):
        if (hasattr(request, "percentile_list") == False):
            raise AttributeError('For the percentile statistic you need to provide a list of required percentiles,'
                                    'e.g. "percentile_list" : [0.01, 0.5, 0.99] for the 1st, 50th and 99th percentile,'
                                    'if you want the whole distribution, "percentile_list" : ["all"]')
        
        if (request.percentile_list[0] != "all"):
            for j in range(np.size(request.percentile_list)):
                if(request.percentile_list[j] > 1): 
                    raise AttributeError('Percentiles must be between 0 and 1 or ["all"] for the whole distribution')
                
    return 
            