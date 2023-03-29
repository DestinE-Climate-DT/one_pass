#!/usr/bin/env python

from setuptools import setup

# List of dependencies for running our tests. Not intended to be
# required for one pass algorithms runtime.
tests = [
    'coverage==7.2.*',
    'pytest==7.2.*',
    'pytest-mock==3.10.*'
]

setup(name='one_pass',
      version='0.0.1',
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
        'cfgrib',
        'dask',
        'docker',
        'gribscan',
        'ecCodes',
        'intake',
        'intake-esm<=2021.8.17',
        'intake-xarray',
        'jinja2',
        'metpy',
        'numpy',
        'pandas',
        'pyYAML',
        'sparse', 
        'xarray',
        'smmregrid@git+https://github.com/jhardenberg/smmregrid'
      ]
    )
