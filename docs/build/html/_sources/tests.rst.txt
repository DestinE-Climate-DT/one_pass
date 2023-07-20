Tests
=========

The opa package uses pytest, a common python testing framework, for both writing and running tests. The Gitlab also uses Continuous Integration/Continuous Development (CI/CD) which runs the testing suite whenever changes are pushed to the reposity. The CI/CD runs on the Lumi HPC platform (is this true?), allowing for testing on different data sets?. Code coverage is also impelmented in the CI/CD. 

Running Tests Locally
---------------------------

To run the testing suite locally you must have pytest loaded in your active environment.

First navigate to the tests folder: 

.. code-block:: bash
   
   cd tests 

Then simply run the following command: 

.. code-block:: bash 
   
   pytest

This will run all of the tests. To simply run one of the tests, run: 

.. code-block:: bash
   
   pytest test_accuracy.py
 
