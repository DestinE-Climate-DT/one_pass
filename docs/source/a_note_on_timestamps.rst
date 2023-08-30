A note on timestamps
------------------------------

The initialisation of the Opa class is given by the line

.. code-block:: python

   opa_stat = Opa("config.yml") # initalise some statistic using the config.yml file

If checkpointing has been set to ``True`` in the request, then this line will look for a checkpoint file to start from. If checkpointing has been set to ``False`` or no checkpoint file exists, it will initialise the class from scratch.

When the next line is run

.. code-block:: python 

   dm = opa_stat.compute(data) # pass some data to compute 

The Opa class will check the time stamp of the new data and, if the class has been initalised from a checkpoint file, it will compare the time stamp of the new data to the last time stamp in the checkpoint file. It will return one of four options.

- option 1 : the new time stamp is exactly the old time stamp plus the time step. This will pass without error.

- option 2 : the new time stamp is greater than the old time stamp by :math:`<= 2` x the time step. This will return the phrase: ``'Time gap at str(time_stamp) too large, there seems to be data missing, small enough to carry on'`` and will continue with the calculation.

- option 3 : the new time stamp is greater than the old time stamp by :math:`> 2` x the time step. This will cause the one_pass to exit with Value Error : ``'Time gap at str(time_stamp) too large, there seems to be some data missing'``.

- option 4 : the new stamp is further back in time than the old time stamp from the checkpoint file. This indicates that the model has crashed and has been re-started or the calculation is starting again. If this new time stamp then corresponds to the beginning of the requested statistic, the one_pass will start the calculation again and overwrite the checkpoint file. If this new time stamp does not correspond to the start of the requested statistic, but rather part way through, the Opa will simply skip the data and wait for the data that corresponds to the next time stamp in the sequence. **In this way, the one_pass does not require to be manually re-set**.

