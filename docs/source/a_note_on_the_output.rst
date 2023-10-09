A note on the output
-----------------------

The output ``dm`` from the one_pass, given by ``dm = opa_stat.compute(data)`` will only not be ``None`` when enough data has been passed to the Opa class to complete the statistic. For example, if you have hourly data and have requested a daily statistic, ``dm`` will return after the 24th time step has been passed. If passing a continuous data stream, ``dm`` will be overwritten when new data is given to the Opa class. Set ``"save" : True`` to save ``dm`` to disk.

The output, ``dm`` will be an xr.Dataset. The dimensions will the same as the original dimensions of the input data, apart from the time dimension. The length of time dimension will be one, unless you set ``output_freq`` greater than ``stat_freq``. See the :doc:`the_data_request` for details.

The timestamp on the time dimension will correspond to the time stamp of the first piece of data that contributed to that statistic.

All of the original attributes (metadata) included in the Dataset will be present in the final Dataset with a new 'opa_history' attribute corresponding to details of the one_pass algorithm and the time of its creation. There will only be one variable in the final Dataset, as the one_pass only processes one variable at a time. The name of the variable and the units will be unchanged.

If you have requested to save the output the file name will be ``timestamp_variable_stat_freq_stat.nc``. For example, if you asked for a monthly mean of precipitation the file name would be output as ``2070_05_pr_monthly_mean.nc``. The length of the time_stamp will change depending on the stat_freq requested. For example, if you ask for ``"stat_freq" : "daily"``, the time_stamp will include the day, whereas if you request ``"stat_freq" : "monthly"`` the time_stamp will just include the month. The one_pass will not differentiate between different experimental runs. 

The only different file names will be for the following:

- ``"stat_freq" : "raw"``, which will follow the form ``timestamp_variable_raw_data.nc``
- ``"stat_freq" : "bias_correction"``, will have the three file names ``timestamp_variable_daily_mean.nc`` (or ``timestamp_variable_daily_sum.nc`` if the variable is for precipitation), ``timestamp_variable_raw_data.nc`` for the raw data and ``month_num_variable_bias_correction.pkl`` for the digest files (potentially also with a .zarr file, see the bias correction section in :doc:`the_data_request`).
- ``"stat_freq" : "histogram"`` will have two files ``timestamp_variable_histogram_stat_freq_bin_edges.nc`` and ``timestamp_variable_histogram_stat_freq_bin_counts.nc``.

