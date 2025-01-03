# One pass algorithms

This repository holds the code for the implementation of the one_pass algorithms, developed for the DestinationEarth project: 
https://destine.ecmwf.int/ and https://destination-earth.eu/ and licensed under Apache License, Version 2.0. 

These algorithms are intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations.

The algorithms take as input any Python Xarray object (either a Dataset or a DataArray) and will compute the requested statistics based on the user configuration yml file. 

The code is ready to be implemented into a workflow which passes output data from climate models of arbitary chunk (number of time steps) length. 

### Disclaimer

No active development of the one-pass is occuring in this repository - that is occuring in a private repository. This repository contains the tagged version v0.6.2 used for publication. For any questions on development please contact katherine.grayson@bsc.es

## Installation
As there is currently no package-based installation for the `one_pass` the source must be obtained. This can easily be done (on any platform with internet connection) by cloning the repository directly:

```
git clone https://github.com/DestinE-Climate-DT/one_pass.git

```
After cloning, set up the environment using either conda or mamba (in the command below mamba can be replaced by conda if you do not have mamba installed): 

```
cd one_pass
mamba env create -f environment.yml
conda activate env_opa

```
In any platform without interent connection, we have provided installation instructions using Docker in the documentation. 

## Documentation 

All the information on how to install, configure and run the one_pass package is given the documentation, found in the docs folder.

## Support

For all feedback, comments and issues, feel free to open an issue or email me at katherine.grayson@bsc.es. 
 
[![DOI](https://zenodo.org/badge/684685048.svg)](https://doi.org/10.5281/zenodo.14591827)



