A note on timestamps
--------------------

Depending on the differences in timestamp, the `compute` method of the `Opa` class will have a different behavior.

Firstly, the Opa instance needs to be initialized:

.. code-block:: python

   opa = Opa("config.yml")

If the request key ``checkpoint`` has been set to ``True``, then during initialization a checkpoint pickle file will be searched to to start from. If checkpointing has been set to ``False`` or no checkpoint file exists, the class will be initialized from scratch.

Then, when running

.. code-block:: python 

   dm = opa.compute(data)

the Opa class will check the time stamp of the new data and, if the class has been initalised from a checkpoint file, it will compare the time stamp of the new data to the last time stamp in the checkpoint file. While running this method one of the following options will occur:

- **Option 1:** The new time stamp is exactly the old time stamp plus the time step. This will pass without error.

- **Option 2:** The new time stamp is greater than the old time stamp by :math:`<= 2` x the time step. This will raise the warning: ``'Time gap at str(time_stamp) too large, there seems to be data missing, small enough to carry on'`` and will continue with the calculation.

- **Option 3:** The new time stamp is greater than the old time stamp by :math:`> 2x` the time step. This will cause the one_pass to exit with ``ValueError``: ``'Time gap at str(time_stamp) too large, there seems to be some data missing'``.

- **Option 4:** The new stamp is further back in time than the old time stamp from the checkpoint file. This indicates that the model has crashed and has been re-started or the calculation is starting again. If this new time stamp then corresponds to the beginning of the requested statistic, the one_pass will start the calculation again and overwrite the checkpoint file. If this new time stamp does not correspond to the start of the requested statistic, but rather part way through, the Opa will simply skip the data and wait for the data that corresponds to the next time stamp in the sequence. **In this way, the one_pass does not require to be manually re-set**.
