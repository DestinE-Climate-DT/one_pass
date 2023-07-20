Getting started 
==================

Basic Concept
--------------
The one_pass python package is designed to act on streamed climate data, generated from the Climate Digitial Twin's (DT) climate models, and extracted from the `GSV interface <https://earth.bsc.es/gitlab/digital-twins/de_340/gsv_interface>`__. By the end of Phase 1, the calls to both the GSV interface (for data retrival) and one_pass (for summary statistics) will be configured by the workflow manager `Autosubmit <https://autosubmit.readthedocs.io/en/master/>`__. For information on how to configure the workflow contact work package (WP) 8. This documentation covers how to use the one_pass package and demonstrates its usage with 'fake' streaming.

The one_pass package has been built around a few core concepts: 

- The temporal aggreation of data through statistical summaries over different time frequncies.
- The output of raw data as a temporary data storage.
- Passing both raw data and data percentiles for use in the bias-correction layer. 

Parallel processing is currently implemented for most statistics through `dask <https://examples.dask.org/xarray.html>`__ to speed up computation. 
 
Running in Python 
--------------------
As with any python library the first step is to import the package 

.. code-block:: python

   from one_pass.opa import Opa 

After importing the library only two lines of python code are required to run

.. code-block:: python 

   opa_stat = Opa(config.yml) # initalise some statistic using the config.yml file
   dm = opa_stat.compute(data) # pass some data to compute 


Above, all the details of the requested statistic are given by the config.yml file, explained in :doc:`the_config_file`. The incoming :``data`` is an `xArray <https://docs.xarray.dev/en/stable/>`__ object containing some climate data over the given (structured or unstructured) grid a certain temporal period. The second line will be run multiple times, as the data stream progress, continously providing new data to the Opa. Once sufficient data has been passed to the Opa, ``dm`` will return the output of the summary statistic. For detailed examples and tutorials, refer to the :doc:`examples_tutorials` section which contain example Jupyter notebooks. 


