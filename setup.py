#!/usr/bin/env python

from setuptools import setup, find_packages

version = "0.6.1"

# List of dependencies for running our tests. Not intended to be
# required for one pass algorithms runtime.
tests = [
    "coverage==7.2.*",
    "pytest==7.2.*",
    "pytest-cov==4.*",
    "pytest-mock==3.10.*",
]

setup(
    name="one_pass",
    version=version,
    description="One Pass Algorithms",
    author="Katherine Grayson",
    author_email="katherine.grayson@bsc.es",
    url="https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass",
    python_requires=">=3.9",
    packages=find_packages(),
    extras_require={
        "tests": tests,
        "all": tests,  # later, here we can do tests + docs + something...
    },
    install_requires=[
        "numpy",
        "xarray",
        "dask",
        "zarr",
        "cython",
        "pytest",
        "netcdf4",
        "cytoolz",
        "tqdm",
        "sphinx",
        "sphinx-rtd-theme",
        "crick==0.0.5",
        "pyarrow",
    ],
)
