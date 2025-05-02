# Changelog


## v0.7.0 - 2024/12/02

### Added
- Bias Correction / One Pass interaction via optional dependency.
- New keys in OPA request to ask for bias corrected statistics, set via `bias_adjust=True`.

## v0.6.4 - 2024/12/02

### Changed
- Build **crick** package via Github tag (see [Crick issue](https://github.com/dask/crick/issues/42))

### Fixed
- Maintenance of package version via `setuptool_scm`
- `pyproject.toml` for build package
- Few linting changes
- Fix CI/CD pipeline, add python 3.11 and 3.12

## v0.6.3 - 2024/10/25

### Added
- Changelog added

### Fixed
- Temporary fix in request key "bias adjustment" validation

## v0.6.2 - 2024/04/29

### Fixed
- Error fixed with 'time.time_stamp' in `check_time.py`

## v0.6.1 - 2024/03/19

### Changed
- File names for statistics now include the timestep of the data, not for raw data, see docs
- All new coordinates created by the one pass (percentiles, durations (iams), thresholds) will have associated attributes
- Thresholds are now passed as a list meaning the OPA can handle multiple thresholds. These will added as a new dimension with corresponding co-ordinate
- New lines added to the config file for the way users can request bias adjustment

### Fixed
- Bug in v0.6.0 meant that min and max and lost the variable for timings, this has been returned

## v0.6.0 - 2024/02/28
**Note:** the original v0.6.0 tag was deleted

### Changed
- If you want to call the bias correction then the function call is now `dm = opa_obj.compute_bias_correction(data)`
- The request for yearly statistics should now use the word `"yearly"` not `"annually"`. All calls with `"annually"` will still work but will give a warning that this value will be deprecated in the future.
- The OPA will now check the given `time_step` in the request if more than one piece of data has been passed. This will change the time step to the real time step and give a warning. This has been causing some errors in the workflow with incorrect time steps in the request.
- Checkpoint filenames will contain the all the details of the request, including the timestamp.
- Mix up of names between mean and sum for the final file
- The OPA now includes a debug logger to give more information on what is happening in case of failure. As with the gsv, to call the debugger `opa_obj = opa(config, logging_level='debug')`. This is outlined in the documentation
- Metadata will now append the `history` attribute, it will not add a new `history_opa` attribute

### Fixed
-  Fixed error in the `setup.py` and the installation not working properly. This tag has fixed this bug along with bug fixes for the bias_correction
- Bug fix for raw data. Previously in the request for raw data there was bug and you needed to include the keys in the request `stat_freq` and `output_freq`. These are not required anymore in the request for raw data and if they are  given they will be ignored. The OPA will not append raw data, it will simply save the data it is passed
- Further bug spotted with update of the monthly digest files not finding the correct file so were re-initiating the digests every time, this has now been fixed
- Erroneous print  which had not been removed

## v0.5.3 - 2024/01/10
- See merge request `digital-twins/de_340/one_pass!34`

## v0.5.2 - 2023/12/15

### Changed
- User warnings included about how raw data will not be aggregated, it will simply be saved with the time dimension that it is passed. stat_freq and output_freq are not required for a raw data request.
- User warnings included about the appropriate combinations between stat_freq and output_freq, don't want to allow an output_freq much larger than stat_freq.

### Fixed
- Fixed numpy less than 1.26 to satisfy crick dependencies

## v0.5.1 - 2023/11/30

### Changed
- OPA class no longer carrying through unnecessary attributes such as `'ds_tail'` - help with size and speed
- When reading from gsv some arrays were being loaded as dask, causing dask instructions to be written as checkpoints (and creating very large checkpoints), these are now loaded into memory before checkpointing so checkpoint is correct size

### Fixed
- Fixed bug that will avoid writing checkpoint for recursive calls if the final statistic is present
- Fixed attributes bug

## v0.5.0 - 2023/10/09

### Changed
- In data request, `out_filepath` has become `save_filepath`
- Removed unnecessary key value pairs from data request (e.g. percentile list, thresh_exceed) unless required for the statistic
- If `output_freq` not given in request will default to `save_freq`
- Histograms formally implemented
- New frequencies `2hourly` and `10annually` implemented
- For bias correction, `"save":false`, will always digest objects
- Checkpointing for digest objects now uses zarr if over pickle limit
- Environment has changed package for t-digest
- Percentiles no longer includes 0 percentile (which has no meaning)
- For full range of percentiles the request is now `"percentile_list" : []`
- If `"stat_freq":"continuous"`, and 'old' data is passed, if that data falls within the range of `output_freq` it will skip over the data and wait for the next piece, otherwise it will re-initialise the continuous statistic

## v0.4.2 - 2023/09/14

### Changed
- Includes IAMS statistic
- Changes to output for bias correction

### Fixed
- Bug fix for daily noon

## v0.4.1 - 2023/08/30

### Changed
- Added freq 'daily_noon' which runs for 24 hrs but starting at 13:00

## v0.4 - 2023/08/29
- Tag for deliverable **D340.9.1.1** that will be copied to the CSC github

## v0.1_bc - 2023/07/13
- Stable version

## v0.3.5 - 2023/07/06

### Fixed
- Fixed checkpointing bug when running continuous stat

## v0.3.4 - 2023/07/06

### Fixed
- Fixed rounding bug for continuous option

## v0.3.3 - 2023/07/05

### Changed
- Naming of variables back to just variable name e.g `'u10'`, not `'u10_mean'`

## v0.3.2 - 2023/07/05
- Stable release for AQUA deliverable

## v0.3.1 - 2023/06/30

### Changed
- This now has the option of the none statistic as well the option of `'all'` for a full range of percentiles

## v0.3.0 - 2023/06/21

### Changed
- This version now includes options for percentile calculation. Although not parallelised so computation may be slow.

## v0.2.3 - 2023/06/16

### Changed
- Now will checkpoint will ZAR files larger than 2 GB

## v0.2.2 - 2023/05/23

### Changed
- More robust at dealing with different time steps

## v0.2.1 - 2023/05/11

### Fixed
- Fixed bugs relating to the recursive call of the algorithm, dynamic checkpointing files and more robust file name creation

## v0.2.0 - 2023/04/24

### Added
- Full restartability with checkpointing
- Supports any data shape (doesn't need to be lat/lon)
- Option of continuous statistic (with monthly files)
- Functionality and accuracy tests included in test folder

## v0.1.1 - 2023/04/11

### Fixed
- Updated version with issue [\#7](https://earth.bsc.es/gitlab/digital-twins/de_340-2/one_pass/-/issues/7) fixed, previously code was throwing error if the timestamps of the data were not wholly divisible by the requested statistic. E.g. hourly data with time stamps at every 17 minutes past the hour would throw an error as a 'daily' statistic would not be a complete day. Now code will run but flag a warning

## v0.1.0 - 2023/03/30
- First release

