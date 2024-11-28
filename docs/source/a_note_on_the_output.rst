A note on the output
-----------------------

Structure of the final xr.Dataset
==================================
The output ``dm`` from the one_pass, given by ``dm = opa_stat.compute(data)`` will only not be ``None`` when enough data has been passed to the Opa class to complete the statistic. For example, if you have hourly data and have requested a daily statistic, ``dm`` will return after the 24th time step has been passed. If passing a continuous data stream, ``dm`` will be overwritten when new data is given to the Opa class. Set ``"save" : True`` to save ``dm`` to disk.

The output, ``dm`` will be an xr.Dataset. The dimensions will the same as the original dimensions of the input data, apart from the time dimension, unless the statistic requested was ``percentile``, ``thresh_exceed`` or ``iams``. The length of time dimension will be one, unless you set ``output_freq`` greater than ``stat_freq``. See the :doc:`the_data_request` for details. If you set ``stat_freq`` as ``continuous`` then the time dimension will always be one, as the output of the rolling statistic will be output at the frequency of ``output_freq``.

For the ``percentile``, ``thresh_exceed`` or ``iams`` statistic with different dimensions, the new dimension will ``percentile``, ``thresholds`` and ``durations`` (in minutes) respectively. These have corresponding co-ordinates.

Final timestamp
=================
The timestamp on the time dimension will correspond to the time stamp of the first piece of data that contributed to that statistic, unless ``stat_freq`` equal to ``continuous``. In this case the timestamp will correspond to the value of the rolling statistic at that point in time (i.e. beginning of that day if ``output_freq : daily``) and the metadata will show the timestamp of the first piece of data that contributed to the the continuous statistic.

Metadata
=========
All of the original attributes (metadata) included in the Dataset will be present in the final Dataset along with a new (or appended if it already exists) 'history' attribute corresponding to the details of the one_pass algorithm and the time of its creation. There will only be one variable in the final Dataset, as the one_pass only processes one variable at a time. The name of the variable and the units will be unchanged.

File name
==========
If you have requested to save the output the file name will be ``timestamp_variable_timestep_x_stat_freq_stat.nc``, where ``x`` is the value of the ``timestep``. For example, if you asked for a monthly mean of precipitation, created from hourly data, the file name would be output as ``2070_05_pr_timestep_60_monthly_mean.nc``. The length of the time_stamp will change depending on the stat_freq requested. For example, if you ask for ``"stat_freq" : "daily"``, the time_stamp will include the day, whereas if you request ``"stat_freq" : "monthly"`` the time_stamp will just include the month. 

If you have requested ``"stat_freq" : "continuous"`` then the file name will be ``timestamp_variable_timestep_x_output_freq_stat.nc`` as each output file will correspond to the rolling summary of the continuous statistic shown at output_freq frequency. The timestamp in the filename will be the start of that output_freq. Again, ``x`` will be the time step of the data used to create the statistic in minutes.

If you have requested an ``output_freq`` greater than the ``stat_freq`` the timestamp in the file name will span the output frequency. For example, ``"stat_freq" : "daily"`` with ``"output_freq" : "monthly"`` will give a file name of ``2071_01_01_to_2071_01_31_pr_timestep_60_daily_mean.nc``.

The one_pass file names  will not differentiate between different experimental runs. 

The other different file names will be for the following:

- ``"stat_freq" : "raw"``, which will follow the form ``timestamp_variable_raw_data.nc``
- ``"stat_freq" : "bias_correction"``, will have the three file names ``timestamp_variable_timestep_x_daily_mean.nc`` (or ``timestamp_variable_timestep_x_daily_sum.nc`` if the variable is for precipitation), ``timestamp_variable_raw_data.nc`` for the raw data and ``month_num_variable_bias_correction.pkl`` for the digest files (potentially also with a .zarr file, see the bias correction section in :doc:`the_data_request`).
- ``"stat_freq" : "histogram"`` will have two files ``timestamp_variable_timestep_60_histogram_stat_freq_bin_edges.nc`` and ``timestamp_variable_timestep_x_histogram_stat_freq_bin_counts.nc``.


.. note:: The inclusion of the ``timestep_x`` into the file name was introduced in v0.6.1.
