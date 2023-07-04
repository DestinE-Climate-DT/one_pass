"""Functions to check the requested statistic."""

stats = {
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

def check_stat(statistic = "mean"):

    """Function to check the requested statistic is a valid one 
    Args:
        statistic: the input statistic requested
    """
    if statistic not in stats:
        valid_values = ", ".join(stats)
        raise ValueError(f"The requested statistic '{statistic}' is not supported, valid values are: {valid_values}")
    
    return 
