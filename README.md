# One pass algorithms
This repository holds the code for the development and implementation of the one pass algorithms for the DestinationEarth project. These algorithms are intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations, taking the needs of the use cases into account. The work is contained in workpackage 9 (WP9).

### Disclaimer: 
The `one_pass` package is in a preliminary developement phase. Some features are still not implemented and you may encounter bugs. For feedback and issue reporting, feel free to open an issue in: https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass/-/issues

## Core idea 
The one_pass algorithms will eventually work with climate data streamted from the Generic State Vector (GSV) interface. The algorithms will take as input any xarray like object (either a DataSet or a DataArray) from the GSV interface and will compute the requested statistics. For details of the algorithms used, please refer to the `README.ipynb`. 

## Version 
The current released version can be found at tag: `v0.3.1`. 

## How to configure
The one pass algorithms are contained in the python script `opa.py` and need to be passed configuration information (data requests) in order from them to work. These requests can either be given as a python dictionary (see `wrapper.py`) or from the configuration file `config.yml`. The following need to be defined: 

- `stat:`. This variable defines the statistic you wish to compute. The current options are `"mean", "std", "var", "thresh_exceed", "min", "max", "percentile"` or `"none"`. **Note `percentile` is new to v0.3.0, and `none` is new to v0.3.1 see below for details on how to use them in the config.**

- `stat_freq:` This defines the frequency of the requested statistic. The current options are `"hourly", "3hourly", "6hourly", "12hourly", "daily", "weekly", "monthly", "3monthly", "annually", "continuous"`. Be careful about spelling, it matters. Note: for the frequencies `"weekly", "monthly", "annually`, the statistic will work with the calendar, e.g. `"annually"` will only work if you the first piece of data provided corresponds to the 1st January, it will not compute a random 365 days starting on any random date. The same for monthly and weekly, where weekly runs from Monday - Sunday. The option of `"continuous`, will start from the first piece of data that you give it. 

- `output_freq:` This defines the frequency you want to save your final dataSet containing your statistic. Options are the same as `stat_freq`. Normally, set the same as `stat_freq`, however if you have requested `stat_freq: "hourly"` but you don't want an output file for every hour, set `output_freq: "daily"` and you will have a dataSet with a time dimension = 24 hourly statistics in one file. Note if you set `stat_freq = continuous` you can still set `output_freq` to whatever time frequency you which to output netCDF, however please DO NOT also set `output_freq = continuous`. **This is a change from v0.2.1**

- `time_step:` This is the the step of your incoming data in **minutes**. This is repeated data from the GSV call and will eventually be combined with the GSV request however for now, it needs to be set seperately. 

- `'variable:` The climate variable you want to compute your statistic on. If you provide the Opa with a dataArray, you do not need to set this, however if you provide a dataSet then this is required. 

- `save:` Either `True` or `False`. If you set this to `False`, the final statistc will only be output in memory and will get overwritten when a new statistic is avaliable. It is recommended to set this to `True` and a netCDF file will be written when the statistic is completed. 

- `checkpoint:` Either `True` or `False`. This defines if you want to write intermediate checkpoint files as the statistic is provided new data. If set to `False` the rolling statistic will only be stored in memory and will be lost of if the memory is wiped. It is highly recommended to set this to `True`.

- `checkpoint_filepath:` This is the file path, ** NOT including the file name**, of your checkpoint files. The name of the checkpoint file will be dynamically created. **this is a change from v0.2.1** 

- `out_filepath:` This is the file path only to where you want the final netCDF files to be written. The name of the file is computed inside the Opa as it contains the details of the requested statistic. 

Some general notes on the config file: 

1. Currently only one statistic can be initalised in each config file. This will probably change so that you can input lists into the config file and call the Opa multiple times but for now that is not supported. 

2. If you change the details of the config file, delete the checkpoint files that may have been written from the old file to avoid mis-calculation. **Note: the new version v0.2.2 should recognise if the checkpoint file is now redundant (in the case that you have gone further back in time) and should correct itself. In case of doubt, always delete it however** 

3. See `config.yml` for an example config file. 

4. **note on percentile in v0.3.0** If you choose the `stat: percentile` option, you also need the config file to include the line: `percentile_list: [0.2, 0.5]`, where the list is the percentiles that you request between the values of [0,1], it can be as long as you like but must be comma seperated. If you want the whole distribution, so all the percentiles from [0,1] you can put ["all"], where "all" must be inside the []. 

5. **note of thresh_exceed** If you choose the `stat: thresh_exceed` you need to include a threshold exceedance value. In the config file include the line `threshold: xxx` where xxx is your value for threshold exceedance.

6. **note on the none option** If you choose the `stat: none` the OPA will simply output the raw data that you feed to the algorithm. If `save: True` then it will save to disk. For the none option you still need to provide the variable of interest as the OPA can only process one variable at a time. If you provide a dataSet with multiple variables, only the variable given will be saved as a dataSet. If `stat: none`, the two options for `stat_freq` and `outout_freq` should be set to none as well, however if they are left as another time frequency they will simply be ignored. 

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

## Output from the opa 

The OPA package will always provide an xr.dataSet object with one variable (requested in the config.yml). This will either be saved to disk in the location specified in config.yml `out_filepath:` or it will simply be returned in memory. 

## Getting the source 

As there is currently no package-based installation for the `one_pass` the source must be obtained.

In Lumi / Levante (any platform with internet connection) this can be easily done cloning the repository directly:

```
git clone https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git
cd one_pass
git checkout v0.3.1`
```

In MareNostrum4 there is no outwards internet connection, so the code must be first cloned locally and then uploaded to the HPC. To load the correct modules run: 

`source load_modules_mn4.sh`

## Dependencies 

The package depedends on the following modules: 
- `numpy`
- `xarray`
- `pandas`
- `dask`
- `zarr`
- `pytest`
- `pytdigest`
- `netcdf4`
- `cytoolz`
- `tqdm`

All the dependencies are  given in the `setup.py` script or, if you're using conda, are given in the `environment.yml`.

## Support
For all feedback, comments and issues, feel free to open an issue or email me at katherine.grayson@bsc.es

## Roadmap
By the summer of 2023 we will have integrated into the one_pass, the possibility to compute percentiles and historgrams on the fly. 

## License
??

