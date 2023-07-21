The config.yml file
=======================

As discussed in :doc:`getting_started`, all of the one_pass configuration is set by a seperate configuration file, called config.yml. In this file you provide the details of the statistic you would like. The config.yml looks like:

.. code-block:: JSON

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

All of the above key:value pairs must be present in the config.yml, even if not required for your requested statistic. In this case, keep the output as ``None``. 

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

The functionality of all the keys in the config.yml are outlined below.

.. note:: 

        **Be careful about spelling in the config file, it matters.**

Statistics
---------------

The one_pass package supports the following options for ``stat`` : 

.. code-block:: JSON
   
   "mean", "std", "var", "thresh_exceed", "min", "max", "percentile", "raw", "bias_correction"

Mean
^^^^^^^^^^^

The mean statistic calculates the arithmatic mean over the requested temporal frequency, using the following:  

.. math::
   
   \bar{x}_n = g(S_{n-1}, x_n) = g(\bar{x}_{n-1}, x_n)  = \bar{x}_{n-1} + w\bigg(\frac{x_n - \bar{x}_{n-1}}{n}\bigg) 

where :math:`n` is the current number of data samples or time steps (count) that have been passed, including the incoming data, :math:`\bar{x}_n` is the updated mean, :math:`\bar{x}_{n-1}` is the mean of the previous data and :math:`w` is the weight of the incoming data chunk (the number of time stamps). In the case where the incoming data has more than one time step, so :math:`w > 1`, :math:`x_n` is the temporal mean over the incoming data computed with numpy.

Variance 
^^^^^^^^^^^^^

The variance (written as ``"var"`` in the config.yml) is calculated for the incoming data stream, over the requested temporal frequency, by updating two estimates iteratively.

.. math:: 

   \textrm{Let } M_{2,n} = \sum_{i = 1}^{n}(x_i - \bar{x}_n)^2 \textrm{   then:  }\hspace{4cm}

   M_{2,n} = g(S_{n-1}, x_n) = g(M_{2,n-1}, x_n) = M_{2,n-1} + w(x_i - \bar{x}_{n-1})(x_i - \bar{x}_n) 
   
where :math:`\bar{x}_n` is given by the algorithm of the mean shown above. At the end of the iterative process (when the last value is given to complete the statistic) 

.. math:: 
   
   \textrm{var}(X_n) = \frac{M_{2,n}}{n-1}

In the case where the incoming data has more than one time step (:math:`w > 1`), :math:`M_{2,n}` is updated by

.. math::
   
      M_{2,n} = M_{2,n-w} + M_{2,w} + \frac{\sqrt{(\bar{x}_{n-w} - \bar{x}_{w})} (w(n-w))}{n} 

where :math:`M_{2,n-w}` is sum of the squared differences of the previously seen data, :math:`M_{2,w}` is the sum of the squared differences over the incoming data block (of weight :math:`w`) and :math:`\bar{x}_{n-w}` and :math:`\bar{x}_{w}` are the means over those same periods respectively. See `S. Mastelini <https://www.sciencedirect.com/science/article/abs/pii/S0167865521000520>`__ for details. 

Standard Deviation 
^^^^^^^^^^^^^^^^^^^^^

The standard deviation (written as ``"std"``) calculates the standard deviation of the incoming data stream over the requested temporal frequency, by taking the square root of the variance: 

.. math:: 

   \sqrt{\textrm{var}(X_n)}

Minimum 
^^^^^^^^^^^^^^

The minimum value (written as ``"min"``) is given by: 

.. math:: 

   \textrm{min}_n = g(S_{n-1}, x_n) = g(\textrm{min}_{n-1}, x_n)  =\textrm{ if } (x_n < \textrm{min}_{n-1}) \textrm{ then }  \textrm{min}_{n-1} = x_n


Maximum
^^^^^^^^^^^^^^

The maximum value (written as ``"max"``) is given by:

.. math:: 

   \textrm{max}_n = g(S_{n-1}, x_n) = g(\textrm{max}_{n-1}, x_n)  =\textrm{ if } (x_n < \textrm{max}_{n-1}) \textrm{ then }  \textrm{max}_{n-1} = x_n


Threshold Exceedance 
^^^^^^^^^^^^^^^^^^^^^^^

The threshold exceedance statistic (written as ``"stat" : "thresh_exceed"``) requires a value for the key:value pair ``thresh_exceed: some_value``, where ``some_value`` is the threshold for your chosen variable. The output of this statistic is the number of times that threshold is exceeded. It is calcuated by: 

.. math:: 

  \textrm{exc}_n = g(S_{n-1}, x_n) = g(exc_{n-1}, x_n = \textrm{ if } (x_n > \textrm{thresh_exceed}) = \textrm{exc}_{n-1} = \textrm{exc}_{n-1} + 1

The variable in the final xr.dataSet output now corresponds to the number of times the data exceeded the threshold.

Percentile
^^^^^^^^^^^^^

The ``"percentile"`` statistic requires a value for the key:value pair ``"percentile_list" : [0.2, 0.5]`` where the list contains the requested percentiles between the values of ``[0,1]``. The list can be as long as you like but must be comma seperated. If you want the whole distribution, so all the percentiles from ``[0,1]``, put ``["all"]``, including the brackets ``[]``. The number of variables in the produced dataSet will correspond to the number of requested percentiles. If you request the full distribution, this will correspond to 101 variables, one for each percentile including 0 and 1. This statistic makes use of the `T-Digest algorithm <https://www.sciencedirect.com/science/article/pii/S2665963820300403>`__ using the `python implementation <https://github.com/protivinsky/pytdigest/tree/main>`__. 

Currently for the TDigests we have set a compression parameter at 25 (reduced from the default of 100), as we have to consider memory contraints. This value needs optimising. 

Raw
^^^^^^^^^^

The ``"raw"`` statistic does not compute any statistical summaries on the incoming data, it simply outputs the raw data as it is passed. The only way it will modify the data is if a dataSet is passed with many climate variables, it will extract the variable requested and produce a dataSet containing only that variable. This option is included to act as a temporary data buffer for some use case applications. 

Bias-Correction
^^^^^^^^^^^^^^^^^

Another layer to the one-pass library is the bias-correction. This package is being developed seperately from the one_pass but will make use of the outputs from the one_pass package. Specifically if you set ``"stat" : "bias_correction"`` you will recieve three outputs, as opposed to just one. 

1. Daily aggregations of the incoming data (either daily means or summations depending on the variable) as netCDF
2. The raw daily data as netCDF 
3. A pickle file containing tDigest objects. There will be one file for each month, and the digests will be udpated with the daily time aggregations (means or summations) for that month. The months will be accumulated, for example, the month 01 file will contain data from all the years the model has convered. 

When using this statistic, make sure to set ``"stat_freq" : "daily"`` and ``"output_freq" : "daily"``.

.. note:: The bias-correction statistic has been created specifically to pass data to the bias_correction package. It does not provide bias corrected data itself.


Frequncies
-----------------

Statistic Frequency
^^^^^^^^^^^^^^^^^^^^^^

The statistic frequency (written as ``"stat_freq"``) can take the following options: 

.. code-block:: 
   
   "hourly", "3hourly", "6hourly", "12hourly", "daily", "weekly", "monthly", "3monthly", "annually", "continuous"

Each option defines the period over which you would like the statistic computed. For the frequencies ``"weekly"``, ``"monthly"``, ``"annually"``, the one_pass package uses the gregorian calendar, e.g. ``"annually"`` will only start accumlating data if the first piece of data provided corresponds to the 1st January, it will not compute a random 365 days starting on any random date. If the data stream starts half way through the year, the one_pass will simply pass over the incoming data until it reaches the beginning of the new year. For ``"monthly"`` leap years are included. ``"weekly"`` will run from Monday - Sunday.

The option of ``"continuous"``, will start from the first piece of data that is provided and will continously update the statistic as new data is provided.

Output Frequency
^^^^^^^^^^^^^^^^^^^

The output frequency option (written as ``"output_freq"``) takes the same input options as ``"stat_freq"``. This option defines the frequency you want to output (or save) the dataSet containing your statistic. If you set ``"output_freq"`` the same as ``"stat_freq"`` (which is the standard output) the dataSet produced by the one_pass will have a time dimension of length 1, corresponding the summary statistic reuqested by ``"stat_freq"``. If, however, if you have requested ``"stat_freq": "hourly"`` but you don't want an output file for every hour, set ``"output_freq": "daily"`` and you will have a dataSet with a time dimension of length 24, corresponding to 24  hourly statistical summaries in one file. 

Note if you set ``"stat_freq" = "continuous"`` you must set ``"output_freq"`` to the frequnecy at which the one pass outputs the current status of the statistic. **Do not** also set ``"output_freq" = "continuous"``.

Time step
----------------

The option ``"time_step"``  is the the time step of your incoming data in **minutes**. Currently this is also given in the configuration file for the GSV, we are aware this repeated data. Soon these configuration files will be combined however for now, it needs to be set here. Eventually, this information will be provided by the streamed climate data. 

Variable 
--------------

The climate variable you want to compute your statistic on. If you provide the one_pass with a dataArray, you do not need to set this, however if you provide a dataSet then this is required. 

**Note the one_pass can only work with one variable at a time, multiple variables will be handled by different calls in the workflow.**

Save
------------

Either ``True`` or ``False``. If you set this to ``False``, the final statistc will only be output in memory and will get overwritten when a new statistic is avaliable. It is recommended to set this to ``True`` and a netCDF file will be written (in the ``"out_filepath"``) when the statistic is completed.


Checkpoint
-----------------
Either ``True`` or ``False``. This defines if you want to write intermediate checkpoint files as the one_pass is provided new data. If true, a checkpoint file will be written for every new chunk of incoming data. If set to ``False`` the rolling statistic will only be stored in memory and will be lost of if the programme crashes. It will also allow for the statistics to be rolled back in time if the model crashes. It is highly recommended to set this to ``True``.


Checkpoint Filepath
-------------------------

This is the file path, **NOT including the file name**, of your checkpoint files. The name of the checkpoint file will be dynamically created.

Save Filepath
-----------------

``"out_filepath"`` is the file path to where you want the final netCDF files to be written. The name of the file is dynamically created inside the one_pass as it contains the details of the requested statistic.













