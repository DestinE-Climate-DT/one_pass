#!/usr/bin/env python

from setuptools import setup

# List of dependencies for running our tests. Not intended to be
# required for one pass algorithms runtime.
tests = [
    'coverage==7.2.*',
    'pytest==7.2.*',
    'pytest-cov==4.*',
    'pytest-mock==3.10.*'
]

setup(name='one_pass',
      version='0.1.0',
      description='One Pass Algorithms',
      author='Katherine Grayson',
      author_email='katherine.grayson@bsc.es',
      url='https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass',
      python_requires='>=3.9, <3.11',
      packages=['one_pass'],
      extras_require={
          'tests': tests,
          'all': tests  # later, here we can do tests + docs + something...
      },
      install_requires=[
	'click==8.1.3',
	'cloudpickle==2.2.1',
	'dask==2023.3.2',
	'fsspec==2023.3.0',
	'importlib-metadata==6.1.0',
	'locket==1.0.0',
	'numpy==1.24.2',
	'packaging==23.0',
	'pandas==1.5.3',
	'partd==1.3.0',
	'python-dateutil==2.8.2',
	'pytz==2023.3',
	'PyYAML==6.0',
	'six==1.16.0',
	'toolz==0.12.0',
	'xarray==2023.3.0',
	'zipp==3.15.0', 
      ]
    )
