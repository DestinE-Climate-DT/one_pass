The data request
=======================

As discussed in :doc:`getting_started`, all of the one_pass configuration is set, either by a separate configuration file (that we call config.yml), or passed as a python dictionary. Both contain a set list of specific key:value pairs. Using these pairs you provide the details of the statistic you would like. The config.yml looks like:

.. code-block:: bash

   stat: "mean"
   stat_freq: "daily"
   output_freq: "daily"
   time_step: 60 
   param: "uas"
   save: True
   checkpoint: True
   checkpoint_filepath: "/file/path/to/checkpoint/"
   save_filepath: "/file/path/to/save/"

Most of the key:value pairs listed above must be present in the request. The exception is ``output_freq``, which will default to the value provided for ``stat_freq`` if it is not provided, while both ``checkpoint_filepath`` and ``save_filepath`` are not required if ``checkpoint`` and ``save`` have respectively been set to False. Other (optional) key:value pairs can be passed if required by a specific statistic. These are explained below. 

The same request can also be passed directly in python as a dictionary:

.. code-block:: python

   pass_dic = {"stat" : "mean",
   "stat_freq": "daily",
   "output_freq": "daily",
   "time_step": 60,
   "variable": "uas",
   "save": True,
   "checkpoint": True,
   "checkpoint_filepath": "/file/path/to/checkpoint/",
   "save_filepath": "/file/path/to/save/"}

The functionality of all the keys are outlined below.

.. note:: 

        **Be careful about spelling in the request, it matters.**

Statistics
---------------

The ``stat`` key defines which statistic you wish to compute on the streamed climate data. The one_pass package can only compute one statistic per request. The following options for ``stat`` are supported: 

.. code-block:: JSON
   
   "mean", "sum", "std", "var", "thresh_exceed", "min", "max", "iams", "percentile", "histogram", "raw", "bias_correction"

We use the folling definitions in the mathematical descriptions of the algorithms below: 

- :math:`n` is the current number of data samples (time stamps) passed to the statistic
- :math:`w` is the weight of incoming data chunk (number of time stamps)
- :math:`X_n = \{x_1, x_2, ..., x_n\}` represents the full data set up to time `n`
- :math:`X_w = \{x_{n-w+1}, \ldots, x_n\}`, is the incoming data chunk of weight :math:`w`
- :math:`S_{n-w}` is the summary of the statistic before the new chunk at time :math:`n-w`.
- :math:`g` is a 'one_pass' function that updates the previous summary :math:`S_{n-w}` with then new incoming data :math:`X_w`  

In the case where the incoming data has only one time step (:math:`w = 1`), :math:`X_w`, reduces to :math:`x_n`.

Summation
^^^^^^^^^^^^^

The summation statistic (written as ``"sum"`` in the statistic request) is calculated over the requested temporal frequency (see ``stat_freq``) by:

.. math::

   \sum_{i=1}^{n}X_n = g(S_{n-w}, X_w) = \sum_{i=1}^{n-w}X_{n-w} + \sum_{i=n-w+1}^{n}X_w,

where in the case of :math:`w>1, \sum_{i=n-w+1}^{n}X_w`, is calculated using numpy.

Mean
^^^^^^^^^^^

The mean statistic calculates the arithmatic mean over the requested temporal frequency, using the following:  

.. math::
   
   \bar{X}_n = g(S_{n-w}, X_w) = \bar{X}_{n-w} + w\bigg(\frac{\bar{X_w} - \bar{X}_{n-w}}{n}\bigg), 

where :math:`\bar{X}_n` is the updated mean of the full dataset, :math:`\bar{X}_{n-w}` is the previous rolling mean and, if :math:`w> 1, \bar{X_w}`, is the temporal mean over the incoming data computed with numpy. If :math:`w= 1, \bar{X_w} = x_n`.

Variance 
^^^^^^^^^^^^^

The variance (written as ``"var"``) is calculated over the requested temporal frequency, by updating two estimates iteratively. Let the two-pass summary :math:`M_{2,n}` be defined as:

.. math:: 

   M_{2,n} = \sum_{i = 1}^{n}(x_i - \bar{x}_n)^2.

For the case where :math:`w = 1`, the one_pass definition is given by: 

.. math:: 

   M_{2,n} = g(S_{n-1}, x_n) = M_{2,n-1} + (x_n - \bar{X}_{n-1})(x_n - \bar{X}_n), 
   
where :math:`\bar{X}_n` and :math:`\bar{X}_{n-1}` are given by the algorithm for the mean shown above. In the case where the incoming data has more than one time step (:math:`w > 1`), :math:`M_{2,n}` is updated by:

.. math::
   
      M_{2,n}= g(S_{n-w}, X_w) = M_{2,n-w} + M_{2,w} + \frac{\sqrt{(\bar{X}_{n-w} - \bar{X}_{w})} (w(n-w))}{n}, 

where :math:`M_{2,n-w}` is sum of the squared differences of the previously seen data, :math:`M_{2,w}` is the sum of the squared differences over the incoming data block (of weight :math:`w`) and :math:`\bar{X}_{n-w}` and :math:`\bar{X}_{w}` are the means over those same periods respectively. 

At the end of the iterative process (when the last value is given to complete the statistic), the sample variance is computed by:

.. math:: 
   
   \textrm{var}(X_n) = \frac{M_{2,n}}{n-1}.

See `S. Mastelini <https://www.sciencedirect.com/science/article/abs/pii/S0167865521000520>`__ for details. 

Standard Deviation 
^^^^^^^^^^^^^^^^^^^^^

The standard deviation (written as ``"std"``) calculates the standard deviation of the incoming data stream over the requested temporal frequency, by taking the square root of the variance: 

.. math:: 

   \textrm{std}(X_n) = \sqrt{\textrm{var}(X_n)}.

Minimum 
^^^^^^^^^^^^^^

The minimum value (written as ``"min"``) is given by: 

.. math:: 

   \textrm{min}(X_n) = g(S_{n-w}, X_w),
 
.. math:: 

   \textrm{ if } \textrm{min}(X_w) < \textrm{min}(S_{n-w}), \textrm{ then }  \textrm{min}(S_{n-w}) = \textrm{min}(X_w),

where if :math:`w > 1, \textrm{min}(X_w)` is calculated using the minimum function in numpy.

Maximum
^^^^^^^^^^^^^^

The maximum value (written as ``"max"``) is given by:

.. math:: 

   \textrm{max}(X_n) = g(S_{n-w}, X_w)

.. math:: 

   \textrm{ if } \textrm{max}(X_w) > \textrm{max}(S_{n-w}), \textrm{ then }  \textrm{max}(S_{n-w}) = \textrm{max}(X_w).

where if :math:`w > 1, \textrm{max}(X_w)` is calculated using the maximum function in numpy.

Threshold Exceedance 
^^^^^^^^^^^^^^^^^^^^^^^

The threshold exceedance statistic (written as ``"thresh_exceed"``) requires a value for the key:value pair ``thresh_exceed: some_value``, where ``some_value`` is an int or float value that defines the threshold for your chosen variable. The output of this statistic is the number of times that threshold is exceeded. It is calculated by: 

.. math::

  \textrm{exc}(X_n) = g(S_{n-w}, X_w), 
 
.. math:: 

  \textrm{ if } (X_w > \textrm{thresh exceed}), \textrm{ then } \textrm{exc}(X_{n}) = \textrm{exc}(S_{n-w}) + s

where :math:`s` is the number of samples in :math:`X_w` that exceeded the threshold. The variable in the final xr.Dataset output now corresponds to the number of times the data exceeded the threshold.

Percentile
^^^^^^^^^^^^^

The ``"percentile"`` statistic has an optional key:value pair ``"percentile_list" : [0.2, 0.5]`` where the list contains the requested percentiles between the values of ``[0,1]``. The list can be as long as you like but must be comma seperated. If you want the whole distribution, so all the percentiles from ``[0,1]``, leave the list empty ``[]``. If this key:value pair is not provided, the package will default to the full distribution ``[]``. The number of variables in the produced Dataset will correspond to the number of requested percentiles. If you request the full distribution, this will correspond to 100 variables, one for each percentile from 0.01 to 1. This statistic makes use of the `T-Digest algorithm <https://www.sciencedirect.com/science/article/pii/S2665963820300403>`__ using the `implementation <https://github.com/dask/crick/tree/0.0.4>`__. 

Histogram
^^^^^^^^^^^^

The ``"histogram"`` statistic uses the same t-digest algorithm as given in the percentiles statistic. This statistic has the optional key:value pair ``"bins" : int``, which sets the number of bins you would like. If this is not set, or set to None, the one_pass will default to 10. Unlike the other statistics, ``"histogram"`` will provide two output files, both in memory and saved to disk if ``"save" : True``. The first will be a netCDF of the bin counts, so the number of values in each bin. If saved, this will have the file name ``timestamp_variable_histogram_stat_freq_bin_counts.nc``, where date will correspond to the date or dates that the data spans and stat_freq is the requested frequency of the statistic (see below). The second netCDF file will correspond to the bin_edges and will have a file name ``timestamp_variable_histogram_stat_freq_bin_edges.nc``. The reason they provided in seperate files is that bin_edges will have one dimension ``bin_edges`` of ``length(bin_count) + 1``. 

Intensity annual maximum series
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The intensity annual maximum series statistic, written as ``"iams"`` computes the summations of a variable over a range of rolling time durations :math:`d` (in minutes) and then takes the maximum value. It is described by 

.. math:: 

   \textrm{iams}(X_n) = \textrm{max}(g(S_{n-w}), X_W),

where 

.. math::  

   g(S_{n-w}) = \bigg\{\sum_{i=1}^{d}(x_1, x_2, \cdots , x_d), \sum_{i=2}^{d+1}(x_2, x_3, \cdots , x_{d+1}), \cdots , 
.. math::
   \sum_{i=n-w-d}^{n-w-1}(x_{n-w-d}, x_{n-w-d+1}, \cdots , x_{n-w-1})\bigg\}

and 

.. math:: 

   X_W = \bigg\{\sum_{i=n-w-d+1}^{n-w}(x_{n-w-d+1}, x_{n-w-d+2}, \cdots , x_{n-w}), \cdots ,
.. math::
   \sum_{i=n-d}^{n-1}(x_{n-d}, \cdots , x_{n-1}), \sum_{i=n-d+1}^{n}(x_{n-d+1}, \cdots , x_{n})\bigg\}

The maximum of value from the set :math:`X_W` is compared to the maximum value of the previous summations :math:`g(S_{n-w})` and the one_pass maximum statistic is used to store the maximum value between the two. The durations :math:`d` are 

.. math:: 
  
  (5, 10, 15, 20, 30, 45, 60, 90, 120, 180, 240, 360, 540, \\
  720, 1080, 1440, 2880, 4320, 5760, 7200, 8640, 10080).

The two pass equivalent of the rolling summations is given by `xr.DataArray.rolling <https://docs.xarray.dev/en/stable/generated/xarray.DataArray.rolling.html>`__.  

For this statistic, you must set both ``"stat_freq"`` and ``"output_freq"`` to ``"annually"``.

Raw
^^^^^^^^^^

The ``"raw"`` statistic does not compute any statistical summaries on the incoming data, it simply outputs the raw data as it is passed. The only way it will modify the data is if a Dataset is passed with many climate variables, it will extract the variable requested and produce a Dataset containing only that variable (this is true for all statistic, see variable key:value). This option is included to act as a temporary data buffer for some use case applications. 

Note: The key:value pairs ``"stat_freq"`` and ``"output_freq"`` will be ignored if ``"stat":"raw"``. The one_pass will simply save the data it has been passed at native temporal resolution. This is to reduce uneccessary I/O operations required to temporally aggregate data to the correct length.

Bias-Correction
^^^^^^^^^^^^^^^^^

Another layer to the one_pass library is the bias-correction. This package is being developed seperately from the one_pass but will make use of the outputs from the one_pass package. Specifically if you set ``"stat" : "bias_correction"`` and ``"save" : True``, you will receive three outputs, as opposed to just one. 

1. Daily aggregations of the incoming data (either daily means or summations if the variable is precipitation) as netCDF. Currently the variable names that will use summation as opposed to means are ``'pr', 'lsp', 'cp', 'tp', 'pre', 'precip', 'rain'``.
2. The raw data at native temporal resolution as netCDF (this is equivalent to ``"raw"`` described above). However here, the raw data will never span the end and start of a new day.
3. A pickle file containing TDigest objects, called ``month_01_variable_bias_correction.pkl`` if the month is January for example. There will be one file for each month, and the digests will be updated with the daily time aggregations (means or summations) for that month. The months will be accumulated, for example, the month 01 file will contain data from all the Januaries of the years the model has covered. 

When using this statistic, make sure to set ``"stat_freq" : "daily"`` and ``"output_freq" : "daily"``. If you set ``"save" : False`` the raw data and the daily aggregated data will not be saved and will just be provided as outputs in memory, however the digest files will always be saved regardless.

If the pickle files containing the digest objects become too large (exceed the pickle limit of 2GB), they will be saved as .zarr files instead. In this case, there will be two files ``month_01_variable_bias_correction.zarr`` containing only the digests and a smaller ``month_01_variable_bias_correction.pkl`` containing all the metadata required for the Dataset. If this occurs during the month (as the digest files get larger) the pickle file ``month_01_variable_bias_correction.pkl`` will automatically switch from containing the full Dataset object containing the digests to containing only the metadata.

.. note:: The bias-correction statistic has been created specifically to pass data to the bias correction package. It does not provide bias corrected data itself.

Frequencies
-----------------

Statistic Frequency
^^^^^^^^^^^^^^^^^^^^^^

The statistic frequency (written as ``"stat_freq"``) is where you select the temporal period required for your statistic. It can take the following options: 

.. code-block:: 
   
   "hourly", "2hourly", "3hourly", "6hourly", "12hourly", "daily", "daily_noon", "weekly", "monthly", "3monthly", "annually", "10annually", "continuous"

Each option defines the period over which you would like the statistic computed. All frequncies will start from midnight except the frequency ``"daily_noon"``, which runs for a 24 hour period but starting at 13:00. 

For the frequencies ``"weekly"``, ``"monthly"``, ``"3monthly"``, ``"annually"``, ``"10annually"``, the one_pass package uses the Gregorian calendar, e.g. ``"annually"`` will only start accumlating data if the first piece of data provided corresponds to the 1st January, it will not compute a random 365 days starting on any random date. The same for ``"10annually"``, it will start from the first 1st January that is passed. If the data stream starts half way through the year, the one_pass will simply pass over the incoming data until it reaches the beginning of the new year. ``"3monthly"``, can be interpreted as quaterly and will compute JFM, AMJ, JAS, OND. ``"weekly"`` will run from Monday - Sunday. Leap years are included, so different days in Feburary will be taken into account. 

The option of ``"continuous"``, will start from the first piece of data that is provided and will continously update the statistic as new data is provided.

Output Frequency
^^^^^^^^^^^^^^^^^^^

The output frequency option (written as ``"output_freq"``) is an optional key:value pair and takes the same input options as ``"stat_freq"``. This option defines the frequency you want to output in memory (and save if requested) the xr.Dataset containing your statistic. The ``"output_freq"`` must be the same or greater than the ``"stat_freq"``. If this key:value pair is not provided in the request it will default to the value provided for ``"stat_freq"``, If ``"output_freq"`` is the same as ``"stat_freq"`` the Dataset produced by the one_pass will have a time dimension of length one, corresponding the summary statistic requested by ``"stat_freq"``. If, however, if you have requested ``"stat_freq": "hourly"`` but you set ``"output_freq": "daily"``, you will have a xr.Dataset with a time dimension of length 24, corresponding to 24 hourly statistical summaries in one file. Likewise, if you set ``"stat_freq":"daily"`` and ``"output_freq":"monthly"``, your final output will have a time dimension of 31 (if there are 31 days in that month), if you started from the first day of the month, or, if you started passing data half way through the month, it will correspond to however many days are left in that month. 

If you set ``"stat_freq" = "continuous"`` you must set ``"output_freq"`` to the frequency at which the one_pass outputs the current status of the statistic. **Do not** also set ``"output_freq" = "continuous"``. If you ``"set_freq":"daily_noon"``, and ``"output_freq":"daily"``, the one_pass will pass a warning letting you know that ``"output_freq":"daily_noon"`` for consistency. It is possible to set the ``"output_freq"`` to a higher value (e.g. ``"weekly"`` or ``"monthly"`` etc). It is not possible to set ``"stat_freq":"weekly"`` and ``"output_freq":"monthly"``, as weeks are not fully divisable by months. 

Also regardless of the ``"save"`` key, the updated version of the final output will be output in memory every time the ``"stat_freq"`` is complete. For example, a combination of ``"set_freq":"daily"`` and ``"output_freq":"weekly"``, will return an output at the end of every day, of the same xr.Dataset being appended by one day each time. 

Time step
----------------

The option ``"time_step"``  is the the time step of your incoming data in **minutes**. Currently this is also given in the configuration file for the GSV, we are aware that this is repeated data. Soon these configuration files will be combined however for now, it needs to be set here. Eventually, this information will be provided by the streamed climate data. 

Variable 
--------------

The climate variable you want to compute your statistic on. This variable is always required, even if you pass an xr.DataArray with only one variable.

**Note the one_pass can only work with one variable at a time, multiple variables will be handled by different calls in the workflow.**

Save
------------

Either ``True`` or ``False``. If you set this to ``False``, the final statistic will only be output in memory and will get overwritten when new data is passed to the Opa class. It is recommended to set this to ``True`` and a netCDF file will be written (in the ``"save_filepath"``) when the statistic is completed.

If you have requested to save the output, the file name will be ``timestamp_variable_stat_frequency_statistic.nc``. For example, if you asked for a monthly mean of precipitation the file name would be ``2070_05_pr_monthly_mean.nc``. The one_pass will not differentiate between different experimental runs.

Checkpoint
-----------------

Either ``True`` or ``False``. If this key:value pair is not provided it will default to ``True``. This defines if you want to write intermediate checkpoint files as the one_pass is provided new data. If ``True``, a checkpoint file will be written for every new chunk of incoming data. If set to ``False`` the rolling statistic will only be stored in memory and will be lost of if the programme crashes. Setting to ``True`` will allow for the statistics to be rolled back in time if the model crashes. It is highly recommended to set this to ``True``.


Checkpoint Filepath
-------------------------

This is the file path, **NOT including the file name**, of your checkpoint files. The name of the checkpoint file will be dynamically created. If ``"checkpoint":False``, this key:value pair is not required.

Save Filepath
-----------------

``"save_filepath"`` is the file path, **NOT including the file name**,  to where you want the final netCDF files to be written. The name of the file is dynamically created inside the one_pass as it contains the details of the requested statistic.













