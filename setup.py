#!/usr/bin/env python

from setuptools import setup, find_packages

requirements = [
    "numpy==2.*",
    "xarray",
    "dask",
    "zarr==2.*",
    "cython==3.*",
    "netcdf4==1.*",
    "cytoolz==1.*",
    "tqdm==4.*",
    "sphinx==8.*",
    "sphinx-rtd-theme==3.*",
    # Creek wheels built from tag, see
    # https://github.com/dask/crick/issues/42
    "crick==0.0.8",
    "pyarrow==18.*",
]


# Optional dependency: Bias adjustment
bias_adjustment = [
    "bias_corr @ git+https://earth.bsc.es/gitlab/digital-twins/de_340-2/bias_adjustment.git@v0.5.5",
]

# List of dependencies for running our tests. Not intended to be
# required for one pass algorithms runtime.
tests = [
    "coverage==7.2.*",
    "pylint==3.*",
    "pytest==7.2.*",
    "pytest-cov==4.*",
    "pytest-mock==3.10.*",
]

setup(
    name="one_pass",
    setup_requires=["setuptools-scm>=8.1.0"],
    use_scm_version=True,
    description="One Pass Algorithms",
    author="Katherine Grayson",
    author_email="katherine.grayson@bsc.es",
    url="https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass",
    python_requires=">=3.10",
    packages=find_packages(),
    extras_require={
        "ba": bias_adjustment,
        "tests": tests,
        "all": bias_adjustment + tests,  # later, here we can do tests + docs + something...
    },
    install_requires=requirements,
)
