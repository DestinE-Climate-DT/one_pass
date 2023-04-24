# One pass algorithms
This repository holds the code for the development and implementation of the one pass algorithms for the DestinationEarth project. These algorithms are intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations, taking the needs of the use cases into account. The work is contained in workpackage 9 (WP9).

### Disclaimer: 
The `one_pass` package is in a preliminary developement phase. Some features are still not implemented and you may encounter bugs. For feedback and issue reporting, feel free to open an issue in: https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass/-/issues

## Core idea 
The one_pass algorithms will eventually work with climate data streamted from the Generic State Vector (GSV) interface. The algorithms will take as input any xarray like object, either a DataSet or a DataArray and compute the requested statistics. For details of the algorithms used, please refer to the `README.ipynb`. 

## Version 
The current released version can be found at tag: `v0.2.0`. 
**This version requires different intialisation comapred to `v0.1.1`, see below for details.** 

## How to configure
The one pass algorithms are contained in the python script `opa.py` and need to be passed configuration information (data requests) in order from them to work. These requests can either be given as a python dictionary (see `wrapper.py`) or from the configuration file `config.yml`. The following need to be defined: 

- `stat:`. This variable defines the statistic you wish to compute. The current options are `"mean", "std", "var", "thresh_exceed", "min", "max"`.

- `stat_freq:` This defines the frequency of the requested statistic. The current options are `"hourly", "3hourly", "6hourly", "12hourly", "daily", "weekly", "monthly", "3monthly", "annually", "continuous"`. Be careful about spelling, it matters. Note: for the frequencies `"weekly", "monthly", "annually`, the statistic will work with the calendar, e.g. `"annually"` will only work if you the first piece of data provided corresponds to the 1st January, it will not compute a random 365 days starting on any random date. The same for monthly and weekly, where weekly runs from Monday - Sunday. The option of `"continuous`, will also output running outputs every month. 

- `output_freq:` This defines the frequency you want to save your final dataSet containing your statistic. Options are the same as `stat_freq`. Normally, set the same as `stat_freq`, however if you have requested `stat_freq: "hourly"` but you don't want an output file for every hour, set `output_freq: "daily"` and you will have a dataSet with a time dimension = 24 hourly statistics in one file. 

- `time_step:` This is the the step of your incoming data in **minutes**. This is repeated data from the GSV call and will eventually be combined with the GSV request however for now, it needs to be set seperately. 

- `'variable:` The climate variable you want to compute your statistic on. If you provide the Opa with a dataArray, you do not need to set this, however if you provide a dataSet then this is required. 

- `save:` Either `True` or `False`. If you set this to `False`, the final statistc will only be output in memory and will get overwritten when a new statistic is avaliable. It is recommended to set this to `True` and a netCDF file will be written when the statistic is completed. 

- `checkpoint:` Either `True` or `False`. This defines if you want to write intermediate checkpoint files as the statistic is provided new data. If set to `False` the rolling statistic will only be stored in memory and will be lost of if the memory is wiped. It is highly recommended to set this to `True`.

- `checkpoint_file:` This is the file path, **including the file name**, of your checkpoint files. The file extension should be `.pickle`. The naming convention (for now) is `checkpoint_stat_variable_stat_freq.pickle`.

- `out_filepath:` This is the file path only to where you want the final netCDF files to be written. The name of the file is computed inside the Opa as it contains the details of the requested statistic. 

Some general notes on the config file: 

1. Currently only one statistic can be initalised in each config file. This will probably change so that you can input lists into the config file and call the Opa multiple times but for now that is not supported. 

2. If you change the details of the config file, delete the checkpoint files that may have been written from the old file to avoid mis-calculation. 

3. See `config.yml` for an example config file. 

## How to run
When using the package, there are four main steps, all shown in `wrapper.py`. They are: 

1. Set up your configuration file `config.yml` or dictionary in your script (see above). 

2. Initalise the algorithm. Here you initalise the algorithm you wish to compute by passing either the python dictionary:

`some_stat = Opa(pass_dic)`

or the configuration file: 

`some_stat = Opa("config.yml")`

This will look for relevant checkpoint files, and, if they exisit, will re-initalise the statistic from the status of the checkpoint file. 

3. Get your data. This will be from a call to the GSV interface, however in `wrapper.py`, it just uses some test data as an example.

4. Compute the algorithm. This is done but calling `.compute` and providing the algorithm with your data, in the example below this is `ds`. 

`dm = some_stat.compute(ds)`

If you have set `save: True` in the config file, then you do not need to set `some_stat.compute(ds)` equal to anything as the output will be saved. If you keep `dm =`, then `dm` will be a dataSet of the computed statistic once you have passed all the information for the statistic to complete. Otherwise it will be NoneType. 

## Getting the source 

As there is currently no package-based installation for the `one_pass` the source must be obtained.

In Lumi / Levante (any platform with internet connection) this can be easily done cloning the repository directly:

```
git clone https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git
cd one_pass
git checkout v0.2.0`
```

In MareNostrum4 there is no outwards internet connection, so the code must be first cloned locally and then uploaded to the HPC.

## Dependencies 

The package depedends on the following modules: 
- `numpy`
- `xarray`
- `pandas`
- `dask`
- `dask.array`

All the dependencies are  given in the `setup.py` script or, if you're using conda, are given in the `environment.yml`.


## Support
For all feedback, comments and issues, feel free to open an issue or email me at katherine.grayson@bsc.es

## Roadmap
By the summer of 2023 we will have integrated into the one_pass, the possibility to compute percentiles and historgrams on the fly. 

## Authors and acknowledgment
Thanks Bruno! 

## License
??

