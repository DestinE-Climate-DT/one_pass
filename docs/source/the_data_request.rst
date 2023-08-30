The data request
=======================

As discussed in :doc:`getting_started`, all of the one_pass configuration is set, either by a separate configuration file (that we call config.yml), or passed as a python dictionary. Both contain a set list of specific key:value pairs. Using these pairs you provide the details of the statistic you would like. The config.yml looks like:

.. code-block:: bash

   stat: "mean"
   percentile_list: None
   thresh_exceed: None
   stat_freq: "daily"
   output_freq: "daily"
   time_step: 60 
   param: "uas"
   save: True
   checkpoint: True
   checkpoint_filepath: "/file/path/to/checkpoint/"
   out_filepath: "/file/path/to/save/"

All of the above key:value pairs must be present, even if not required for your requested statistic. In this case, keep the output as ``None``. 

The same information can also be passed directly in python as a dictionary:

.. code-block:: python

   pass_dic = {"stat" : "mean",
   "percentile_list" : None,
   "thresh_exceed" : None,
   "stat_freq": "daily",
   "output_freq": "daily",
   "time_step": 60,
   "variable": "uas",
   "save": True,
   "checkpoint": True,
   "checkpoint_filepath": "/file/path/to/checkpoint/",
   "out_filepath": "/file/path/to/save/"}

The functionality of all the keys are outlined below.

.. note:: 

        **Be careful about spelling in the request, it matters.**

Statistics
---------------

The one_pass package supports the following options for ``stat`` : 

.. code-block:: JSON
   
   "mean", "sum", "std", "var", "thresh_exceed", "min", "max", "percentile", "raw", "bias_correction"
<<<<<<< HEAD:docs/source/the_data_request.rst

We use the folling definitions in the mathematical descriptions of the algorithms below: 

- :math:`n` is the current number of data samples (time stamps) passed to the statistic
- :math:`w` is the weight of incoming data chunk (number of time stamps)
- :math:`X_n = \{x_1, x_2, ..., x_n\}` represents the full data set up to time `n`
- :math:`X_w = \{x_{n-w+1}, \ldots, x_n\}`, is the incoming data chunk of weight :math:`w`
- :math:`S_{n-w}` is the summary of the statistic before the new chunk at time :math:`n-w`.
- :math:`g` is a 'one_pass' function that updates the previous summary :math:`S_{n-w}` with then new incoming data :math:`X_w`  

=======

We use the folling definitions in the mathematical descriptions of the algorithms below: 

- :math:`n` is the current number of data samples (time stamps) passed to the statistic
- :math:`w` is the weight of incoming data chunk (number of time stamps)
- :math:`X_n = \{x_1, x_2, ..., x_n\}` represents the full data set up to that point
- :math:`X_w = \{x_{n-w}, \ldots, x_n\}`, is the incoming data chunk of weight :math:`w`
- :math:`S_{n-w}` is the summary of the statistic before the new chunk at time :math:`n-w`
- :math:`g` is a 'one pass' function that updates the previous summary :math:`S_{n-w}` with then new incoming data :math:`X_w`  

>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst
In the case where the incoming data has only one time step (:math:`w = 1`), :math:`X_w`, reduces to :math:`x_n`.

Summation
^^^^^^^^^^^^^

<<<<<<< HEAD:docs/source/the_data_request.rst
The summation statistic (written as ``"sum"`` in the statistic request) is calculated by:

.. math::

   \sum_{i=1}^{n}X_n = g(S_{n-w}, X_w) = \sum_{i=1}^{n-w}X_{n-w} + \sum_{i=n-w+1}^{n}X_w,

where in the case of :math:`w>1, \sum_{i=n-w+1}^{n}X_w`, is calculated using numpy.

Mean
^^^^^^^^^^^

The mean statistic calculates the arithmatic mean over the requested temporal frequency, using the following:  

.. math::
   
   \bar{X}_n = g(S_{n-w}, X_w) = \bar{X}_{n-w} + w\bigg(\frac{\bar{X_w} - \bar{X}_{n-w}}{n}\bigg), 

where :math:`\bar{X}_n` is the updated mean of the full dataset, :math:`\bar{X}_{n-w}` is the previous rolling mean and, if :math:`w> 1, \bar{X_w}`, is the temporal mean over the incoming data computed with numpy. If :math:`w= 1, \bar{X_w} = x_n`.
=======
The summation statistic (written as ``sum`` in the statistic request) is calculated by:

.. math::

   \sum_{i=1}^{n}X_n = g(S_{n-w}, X_w) = \sum_{i=1}^{n-w}X_{n-w} + \sum_{i=n-w}^{n}X_w,

where in the case of :math:`w>1, \sum_{i=n-w}^{n}X_w`, is calculated using numpy.

Mean
^^^^^^^^^^^

The mean statistic calculates the arithmatic mean over the requested temporal frequency, using the following:  

.. math::
   
   \bar{X}_n = g(S_{n-w}, X_w) = \bar{X}_{n-w} + w\bigg(\frac{\bar{X_w} - \bar{X}_{n-w}}{n}\bigg) 

where :math:`\bar{X}_n` is the updated mean of the full dataset, :math:`\bar{X}_{n-w}` is the mean of the previous data and, if :math:`w> 1, \bar{X_w}`, is the temporal mean over the incoming data computed with numpy.
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

Variance 
^^^^^^^^^^^^^

The variance (written as ``"var"``) is calculated for the incoming data stream, over the requested temporal frequency, by updating two estimates iteratively. Let the two-pass summary :math:`M_{2,n}` be defined as:

.. math:: 

<<<<<<< HEAD:docs/source/the_data_request.rst
   M_{2,n} = \sum_{i = 1}^{n}(x_i - \bar{x}_n)^2.

For the case where :math:`w = 1`, the one_pass definition is given by: 

.. math:: 

   M_{2,n} = g(S_{n-1}, x_n) = M_{2,n-1} + (x_n - \bar{X}_{n-1})(x_n - \bar{X}_n), 
=======
   M_{2,n} = \sum_{i = 1}^{n}(x_i - \bar{x}_n)^2

For the case where :math:`w = 1`, the one-pass defintion is given by: 

.. math:: 

   M_{2,n} = g(S_{n-1}, x_n) = M_{2,n-1} + (x_n - \bar{X}_{n-1})(x_n - \bar{X}_n) 
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst
   
where :math:`\bar{X}_n` and :math:`\bar{X}_{n-1}` are given by the algorithm for the mean shown above. In the case where the incoming data has more than one time step (:math:`w > 1`), :math:`M_{2,n}` is updated by:

.. math::
   
<<<<<<< HEAD:docs/source/the_data_request.rst
      M_{2,n}= g(S_{n-w}, X_w) = M_{2,n-w} + M_{2,w} + \frac{\sqrt{(\bar{X}_{n-w} - \bar{X}_{w})} (w(n-w))}{n}, 
=======
      M_{2,n}= g(S_{n-w}, X_w) = M_{2,n-w} + M_{2,w} + \frac{\sqrt{(\bar{X}_{n-w} - \bar{X}_{w})} (w(n-w))}{n} 
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

where :math:`M_{2,n-w}` is sum of the squared differences of the previously seen data, :math:`M_{2,w}` is the sum of the squared differences over the incoming data block (of weight :math:`w`) and :math:`\bar{X}_{n-w}` and :math:`\bar{X}_{w}` are the means over those same periods respectively. 

At the end of the iterative process (when the last value is given to complete the statistic), the sample variance is computed by:

.. math:: 
   
<<<<<<< HEAD:docs/source/the_data_request.rst
   \textrm{var}(X_n) = \frac{M_{2,n}}{n-1}.
=======
   \textrm{var}(X_n) = \frac{M_{2,n}}{n-1}
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

See `S. Mastelini <https://www.sciencedirect.com/science/article/abs/pii/S0167865521000520>`__ for details. 

Standard Deviation 
^^^^^^^^^^^^^^^^^^^^^

The standard deviation (written as ``"std"``) calculates the standard deviation of the incoming data stream over the requested temporal frequency, by taking the square root of the variance: 

.. math:: 

<<<<<<< HEAD:docs/source/the_data_request.rst
   \textrm{std}(X_n) = \sqrt{\textrm{var}(X_n)}.
=======
   \textrm{std}(X_n) = \sqrt{\textrm{var}(X_n)}
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

Minimum 
^^^^^^^^^^^^^^

The minimum value (written as ``"min"``) is given by: 

.. math:: 

<<<<<<< HEAD:docs/source/the_data_request.rst
   \textrm{min}(X_n) = g(S_{n-w}, X_w),
 
.. math:: 

   \textrm{ if } \textrm{min}(X_w) < \textrm{min}(S_{n-w}), \textrm{ then }  \textrm{min}(S_{n-w}) = \textrm{min}(X_w),
=======
   \textrm{min}(X_n) = g(S_{n-w}, X_w)
 
.. math:: 

   \textrm{ if } \textrm{min}(X_w) < \textrm{min}(X_{n-w}), \textrm{ then }  \textrm{min}(X_{n-w}) = \textrm{min}(X_w)
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

where if :math:`w > 1, \textrm{min}(X_w)` is calculated using the minimum function in numpy.

Maximum
^^^^^^^^^^^^^^

The maximum value (written as ``"max"``) is given by:

.. math:: 

   \textrm{max}(X_n) = g(S_{n-w}, X_w)

.. math:: 

<<<<<<< HEAD:docs/source/the_data_request.rst
   \textrm{ if } \textrm{max}(X_w) > \textrm{max}(S_{n-w}), \textrm{ then }  \textrm{max}(S_{n-w}) = \textrm{max}(X_w).
=======
   \textrm{ if } \textrm{max}(X_w) > \textrm{max}(X_{n-w}), \textrm{ then }  \textrm{max}(X_{n-w}) = \textrm{max}(X_w)
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

where if :math:`w > 1, \textrm{max}(X_w)` is calculated using the maximum function in numpy.

Threshold Exceedance 
^^^^^^^^^^^^^^^^^^^^^^^

The threshold exceedance statistic (written as ``"thresh_exceed"``) requires a value for the key:value pair ``thresh_exceed: some_value``, where ``some_value`` is the threshold for your chosen variable. The output of this statistic is the number of times that threshold is exceeded. It is calculated by: 

.. math::

<<<<<<< HEAD:docs/source/the_data_request.rst
  \textrm{exc}(X_n) = g(S_{n-w}, X_w), 
 
.. math:: 

  \textrm{ if } (X_w > \textrm{thresh exceed}), \textrm{ then } \textrm{exc}(X_{n}) = \textrm{exc}(S_{n-w}) + s
=======
  \textrm{exc}(X_n) = g(S_{n-w}, X_w) 
 
.. math:: 

  \textrm{ if } (X_w > \textrm{thresh exceed}), \textrm{then} \textrm{exc}(X_{n-w}) = \textrm{exc}(X_{n-w}) + s
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

where :math:`s` is the number of samples in :math:`X_w` that exceeded the threshold. The variable in the final xr.Dataset output now corresponds to the number of times the data exceeded the threshold.

Percentile
^^^^^^^^^^^^^

The ``"percentile"`` statistic requires a value for the key:value pair ``"percentile_list" : [0.2, 0.5]`` where the list contains the requested percentiles between the values of ``[0,1]``. The list can be as long as you like but must be comma seperated. If you want the whole distribution, so all the percentiles from ``[0,1]``, put ``["all"]``, including the brackets ``[]``. The number of variables in the produced Dataset will correspond to the number of requested percentiles. If you request the full distribution, this will correspond to 101 variables, one for each percentile including 0 and 1. This statistic makes use of the `T-Digest algorithm <https://www.sciencedirect.com/science/article/pii/S2665963820300403>`__ using the `python implementation <https://github.com/protivinsky/pytdigest/tree/main>`__. 

Currently for the TDigests we have set a compression parameter at 25 (reduced from the default of 100), as we have to consider memory contraints. This value needs optimising. 

Raw
^^^^^^^^^^

The ``"raw"`` statistic does not compute any statistical summaries on the incoming data, it simply outputs the raw data as it is passed. The only way it will modify the data is if a Dataset is passed with many climate variables, it will extract the variable requested and produce a Dataset containing only that variable. This option is included to act as a temporary data buffer for some use case applications. 

Bias-Correction
^^^^^^^^^^^^^^^^^

Another layer to the one_pass library is the bias-correction. This package is being developed seperately from the one_pass but will make use of the outputs from the one_pass package. Specifically if you set ``"stat" : "bias_correction"`` you will receive three outputs, as opposed to just one. 

1. Daily aggregations of the incoming data (either daily means or summations depending on the variable) as netCDF
2. The raw daily data as netCDF 
<<<<<<< HEAD:docs/source/the_data_request.rst
3. A pickle file containing TDigest objects. There will be one file for each month, and the digests will be updated with the daily time aggregations (means or summations) for that month. The months will be accumulated, for example, the month 01 file will contain data from all the Januaries of the years the model has covered. 
=======
3. A pickle file containing TDigest objects. There will be one file for each month, and the digests will be udpated with the daily time aggregations (means or summations) for that month. The months will be accumulated, for example, the month 01 file will contain data from all the years the model has covered. 
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

When using this statistic, make sure to set ``"stat_freq" : "daily"`` and ``"output_freq" : "daily"``.

.. note:: The bias-correction statistic has been created specifically to pass data to the bias correction package. It does not provide bias corrected data itself.


Frequencies
-----------------

Statistic Frequency
^^^^^^^^^^^^^^^^^^^^^^

The statistic frequency (written as ``"stat_freq"``) can take the following options: 

.. code-block:: 
   
   "hourly", "3hourly", "6hourly", "12hourly", "daily", "weekly", "monthly", "3monthly", "annually", "continuous"

Each option defines the period over which you would like the statistic computed. For the frequencies ``"weekly"``, ``"monthly"``, ``"annually"``, the one_pass package uses the Gregorian calendar, e.g. ``"annually"`` will only start accumlating data if the first piece of data provided corresponds to the 1st January, it will not compute a random 365 days starting on any random date. If the data stream starts half way through the year, the one_pass will simply pass over the incoming data until it reaches the beginning of the new year. For ``"monthly"`` leap years are included. ``"weekly"`` will run from Monday - Sunday.

The option of ``"continuous"``, will start from the first piece of data that is provided and will continously update the statistic as new data is provided.

Output Frequency
^^^^^^^^^^^^^^^^^^^

The output frequency option (written as ``"output_freq"``) takes the same input options as ``"stat_freq"``. This option defines the frequency you want to output (or save) the xr.Dataset containing your statistic. If you set ``"output_freq"`` the same as ``"stat_freq"`` (which is the standard output) the Dataset produced by the one_pass will have a time dimension of length one, corresponding the summary statistic requested by ``"stat_freq"``. If, however, if you have requested ``"stat_freq": "hourly"`` but you set ``"output_freq": "daily"``, you will have a xr.Dataset with a time dimension of length 24, corresponding to 24 hourly statistical summaries in one file. Likewise, if you set ``"stat_freq":"daily"`` and ``"output_freq":"monthly"``, your final output will have a time dimension of 31 (if there are 31 days in that month), if you started from the first day of the month, or, if you started passing data half way through the month, it will correspond to however many days are left in that month. 

<<<<<<< HEAD:docs/source/the_data_request.rst
The ``"output_freq"`` must be the same or greater than the ``"stat_freq"``. If you set ``"stat_freq" = "continuous"`` you must set ``"output_freq"`` to the frequency at which the one_pass outputs the current status of the statistic. **Do not** also set ``"output_freq" = "continuous"``.
=======
The ``"output_freq"`` must be the same or greater than the ``"stat_freq"``. If you set ``"stat_freq" = "continuous"`` you must set ``"output_freq"`` to the frequency at which the one pass outputs the current status of the statistic. **Do not** also set ``"output_freq" = "continuous"``.
>>>>>>> ac1181b (updated documentation):docs/source/the_config_file.rst

Time step
----------------

The option ``"time_step"``  is the the time step of your incoming data in **minutes**. Currently this is also given in the configuration file for the GSV, we are aware that this is repeated data. Soon these configuration files will be combined however for now, it needs to be set here. Eventually, this information will be provided by the streamed climate data. 

Variable 
--------------

The climate variable you want to compute your statistic on. If you provide the one_pass with a xr.DataArray, you do not need to set this, however if you provide an xr.Dataset then this is required. 

**Note the one_pass can only work with one variable at a time, multiple variables will be handled by different calls in the workflow.**

Save
------------

Either ``True`` or ``False``. If you set this to ``False``, the final statistic will only be output in memory and will get overwritten when new data is passed to the Opa class. It is recommended to set this to ``True`` and a netCDF file will be written (in the ``"out_filepath"``) when the statistic is completed.

If you have requested to save the output, the file name will be ``timestamp_variable_stat_frequency_statistic.nc``. For example, if you asked for a monthly mean of precipitation the file name would be ``2070_05_pr_monthly_mean.nc``. The one_pass will not differentiate between different experimental runs.

Checkpoint
-----------------
Either ``True`` or ``False``. This defines if you want to write intermediate checkpoint files as the one_pass is provided new data. If ``True``, a checkpoint file will be written for every new chunk of incoming data. If set to ``False`` the rolling statistic will only be stored in memory and will be lost of if the programme crashes. Setting to ``True`` will allow for the statistics to be rolled back in time if the model crashes. It is highly recommended to set this to ``True``.


Checkpoint Filepath
-------------------------

This is the file path, **NOT including the file name**, of your checkpoint files. The name of the checkpoint file will be dynamically created.

Save Filepath
-----------------

``"out_filepath"`` is the file path to where you want the final netCDF files to be written. The name of the file is dynamically created inside the one_pass as it contains the details of the requested statistic.













