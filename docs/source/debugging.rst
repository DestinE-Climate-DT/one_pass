Debugging
============

From v0.6.0 onwards the one pass library has a debugging function that will allow you to see more of the internal workings of the package. To run the function using the debugging call:

.. code-block:: python

   from one_pass.opa import Opa

After importing the library only two lines of python code are required to run

.. code-block:: python

   opa_stat = Opa("config.yml", logging_level="DEBUG") # setting the logging level here
   dm = opa_stat.compute(data) # pass some data to compute

In addition to the ``DEBUG``, the other options for the logger are ``INFO``.


