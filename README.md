# One pass algorithms

This repository holds the code for the development and implementation of the one pass algorithms. These algorithms are intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations, taking the needs of the use cases into account. (WP9)

### Disclaimer: 
The `one_pass` package is in a preliminary developement phase. Some features are still not implemented and you may encounter bugs. For feedback and issue reporting, feel free to open an issue in: https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass/-/issues

The current released version can be found at tag: `v0.1.1`


## Core idea 

The one_pass algorithms will eventually work with data from the GSV interface. The algorithms will take as input any xarray like object, either a DataSet or a DataArray and compute the requested statistics. A jupyter workbook called `run_opa.ipynb` (configured to work on Levante with data from the AQUA re-gridder) is provided. **Please refer to this workbook for exact examples.** For details of the algorithms used, please refer to the `README.ipynb`. 

When using the package, there are three main steps: 

1. Set the timestep of your data in minutes the configuration file `config.yml`. This should be known from your call to the GSV interface or by looking at the time step of your data. 

2. Initalise the algorithm. Here you initalise the algorithm you wish to compute. You provide the tpye of statistic (mean, std, var etc.), the frequency over which to compute (hourly, daily etc.), do you want to save etc. Example below for a daily mean: 

`daily_mean = Opa(statistic = "mean", stat_freq = "daily", output_freq = "daily", save = False, variable = "tas", config_path = file_path)`

3. Compute the algorithm. This is done but calling `.compute` and providing the algorithm with your data, in the example below this is `ds`. 

`dm = daily_mean.compute(ds)`

**Refer to the workbook `run_opa.ipynb` for examples**

## Getting the source 

As there is currently no package-based installation for the `one_pass` the source must be obtained.

In Lumi / Levante (any platform with internet connection) this can be easily done cloning the repository directly:

```
git clone https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git
cd one_pass
git checkout v0.1.0`
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

## Usage
For detailed examples of the one_pass alogirthms, please refer to the jupyter notebook `run_opa.py`.

## Support
For all feedback, comments and issues, feel free to open an issue or email me at katherine.grayson@bsc.es

## Roadmap
By the summer of 2023 we will have integrated into the one_pass, the possibility to compute percentiles and historgrams on the fly. 

## Authors and acknowledgment
Thanks Bruno! 

## License
??

