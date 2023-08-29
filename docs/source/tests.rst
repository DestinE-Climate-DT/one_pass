Tests
=========

The one_pass package uses pytest, a common python testing framework, for both writing and running tests. The Gitlab also uses Continuous Integration/Continuous Development (CI/CD) which runs the testing suite whenever changes are pushed to the reposity. The CI/CD pipeline currently uses a small test data file located in the repository for testing. In the future it will use data located on different HPC platforms to test the functionality and accuracy of the code. Code coverage is also impelemented in the CI/CD. 

Running Tests Locally
---------------------------

To run the testing suite locally you must have pytest loaded in your active environment, as well as the dependencies for the one_pass package. 

First navigate to the tests folder: 

.. code-block:: bash
   
   cd tests 

Then simply run the following command: 

.. code-block:: bash 
   
   pytest

This will run all of the tests. To simply run one of the tests, run: 

.. code-block:: bash
   
   pytest test_accuracy.py
 
The tests cover the accuracy of all the implemented statistics, over different temporal periods and with different time length chunks. They also cover functionality, error handeling and checks on the statistic request. 
