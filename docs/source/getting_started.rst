Getting started 
==================

Basic Concept
--------------

The one_pass python package is designed to act on streamed climate data, generated from the Climate Digitial Twin's (DT) climate models, and extracted from the `GSV interface <https://earth.bsc.es/gitlab/digital-twins/de_340/gsv_interface>`__. By the end of Phase 1, the calls to both the GSV interface (for data retrival) and one_pass (for summary statistics) will be configured by the workflow manager `Autosubmit <https://autosubmit.readthedocs.io/en/master/>`__. For information on how to configure the workflow contact work package (WP) 8. It is not necessary however for the one_pass to work with Climate DT data and be configured by Autosubmit; it is a standalone package that can be used with any streamed climate data. This documentation covers how to use the one_pass package and demonstrates its usage with 'fake' streaming.

The one_pass package has been built around a few core concepts: 

- The temporal aggreation of data through statistical summaries over different time frequencies.
- The output of raw data as a temporary data storage.
- Passing both raw data and data percentiles for use in the bias-correction layer. 

The One Pass package implements the One-Pass method by means of the `Opa` class and the concept of _checkpoints_. In streaming, the data arrives in chunks of arbitrary length. At any given point, a checkpoint is a pickle (binary) representation of the `Opa` class. This serves as a temporary buffer of the data that is aggregated while the requested statistic is not complete.

Following the mathematical representation, at a given step :math:`i`, the checkpoint and the incoming data chunk would be used to compute the state of the `Opa` class at the next timestep :math:`i + 1`. Once enough data has been streamed to complete a statistics, the checkpoints are removed by default.

Parallel processing is currently implemented for most statistics through `dask <https://examples.dask.org/xarray.html>`__ to speed up computation. 
 
Running in Python 
--------------------

As with any python library the first step is to import the package 

.. code-block:: python

   from one_pass.opa import Opa 

After importing the library only two lines of python code are required to run

.. code-block:: python 

   opa = Opa("config.yml") # initalise some statistic using the config.yml file
   dm = opa.compute(data) # pass some data to compute


Above, all the details of the requested statistic are given by the data request, given here in the ``config.yml`` file (a python dictionary can also be passed). How to configure the data request is explained in :doc:`the_data_request`. The incoming ``data`` is an `xarray <https://docs.xarray.dev/en/stable/>`__ object containing some climate data over a given (structured or unstructured) grid and a certain temporal period. The second line (containing the .compute call) will be run multiple times, as the data stream progresses, continously providing new data to the Opa class. Once sufficient data has been passed to the Opa class (enough to complete the requested statistic), ``dm`` will return the output of the summary statistic. For details of output file naming conventions and structure of the output, refer to :doc:`a_note_on_the_output`. For detailed examples and tutorials, refer to the :doc:`examples_tutorials` section which contain example Jupyter notebooks. 

.. note::

   Once the passed ``data`` completes the requested statistics, checkpoints are removed by default. Since version `v0.7.1`, this can be prevented by initializing the ``opa`` instance as ``opa = Opa("config.yml", keep_checkpoints=True)``.

.. note:: 

   There is currently a problem with GRIB variable names that start with a number, as when saving they are saved with a / in front. This issue is being worked on. 

