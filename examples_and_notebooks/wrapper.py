import numpy as np
from pathlib import Path
import xarray as xr

from one_pass.opa import Opa

DATA_FILEPATH = Path(__file__).absolute().parent.parent / "data" / "pr_12_months.nc"

OPAREQUEST = {
    "stat": "mean",
    "stat_freq": "daily",
    "output_freq": "daily",
    "time_step": 60,
    "variable": "uas",
    "save": True,
    "checkpoint": True,
    "checkpoint_filepath": "/path/to/checkpoint/data/",
    "save_filepath": "/path/to/saved/data/",
}


if __name__ == "__main__":

    # Reading some data from disk - replace with call to GSV interface
    data = xr.open_dataset(DATA_FILEPATH)
    data = data.astype(np.float64)

    for i in range(24):
        # can pass either a dictionary as above or data from the config file
        # daily_mean = Opa("config.yml")
        # Simulate streaming by extracting a moving window
        incoming_dataset = data.isel(time=slice(i, i + 1))

        # Because we are checkpointing, we create the Opa instance from scratch,
        # since the class will be constructed after the written binary file
        daily_mean = Opa(OPAREQUEST)

        # Compute result after the incoming data
        dm = daily_mean.compute(incoming_dataset)  # computing algorithm with new data
