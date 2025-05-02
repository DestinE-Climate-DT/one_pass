import glob
import os
import sys
import xarray as xr
import numpy as np

from one_pass.opa import Opa


def main():

    ###### OPA request #################

    request = {
        "stat": "mean",
        "stat_freq": "daily",
        "output_freq": "daily",
        "time_step": 60,
        "variable": "pr",
        "save": True,
        "checkpoint": True,
        "checkpoint_filepath": "/path/to/checkpoint/data/",
        "save_filepath": "/path/to/saved/data/",
    }

    ########### GSV Interface ################

    #### reading some data from disk - replace with call to GSV interface  ####
    data = xr.open_dataset("pr_12_months.nc")
    data = data.astype(np.float64)

    ############ Computing OPA #############

    for i in range(0, 24, 1):

        # can pass either a dictionary as above or data from the config file
        # daily_mean = Opa("config.yml")
        # Simulate streaming by extracting a moving window
        incoming_dataset = data.isel(time=slice(i, i + 1))

        # Because we are checkpointing, we create the Opa instance from scratch,
        # since the class will be constructed after the written binary file
        daily_mean = Opa(request)

        # Compute result after the incoming data
        dm = daily_mean.compute(incoming_dataset)  # computing algorithm with new data


if __name__ == "__main__":
    main()
