#!/usr/bin/env python

from setuptools import setup

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
    version="0.4.1",
    description="One Pass Algorithms",
    author="Katherine Grayson",
    author_email="katherine.grayson@bsc.es",
    url="https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass",
    python_requires=">=3.9",
    packages=["one_pass"],
    extras_require={
        "tests": tests,
        "all": tests,  # later, here we can do tests + docs + something...
    },
    install_requires=[
        "numpy",
        "xarray",
        "dask",
        "zarr",
        "pytdigest",
        "pytest",
        "netcdf4",
        "cytoolz",
        "tqdm",
        "sphinx",
        "sphinx-rtd-theme",
    ],
)
