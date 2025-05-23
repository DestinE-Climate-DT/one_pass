# One pass algorithms

This repository holds the code for the development and implementation of the one_pass algorithms for the DestinationEarth project. These algorithms are intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations, taking the needs of the use cases into account. The work is contained in workpackage 9 (WP9).

The one_pass algorithms will eventually work with climate data streamted from the Generic State Vector (GSV) interface. The algorithms take as input any Xarray object (either a Dataset or a DataArray) from the GSV interface and will compute the requested statistics. 

### Disclaimer
The `one_pass` package is in a preliminary developement phase. Some features are still not implemented and you may encounter bugs. For feedback and issue reporting, feel free to open an issue in: https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass/-/issues
 
## Installation
As there is currently no package-based installation for the `one_pass` the source must be obtained. This can easily be done (on any platform with internet connection) by cloning the repository directly. We recommend cloning the repository to depth 1 (only taking the latest commit to the main branch), to avoid cloning the larger history:

```
git clone --depth 1 https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git

```
If however you want to work on other branches, clone the whole repository by: 

```
git clone https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git

```
After cloning, set up the environment using either conda or mamba (in the command below mamba can be replaced by conda if you do not have mamba installed): 

```
cd one_pass
mamba env create -f environment.yml
conda activate env_opa

```
In any platform without interent connection, we have provided installation instructions using Docker in the documentation.

Alternatively, the package can be installed via pip and venv, as:

```
git clone https://earth.bsc.es/gitlab/digital-twins/de_340-2/one_pass.git
cd one_pass
python3 -m venv venv
source venv/bin/activate
pip install .[all] # or pip install -e .[all] for an editable installation 
```

### Added in v0.7.0

In minor release v0.7, the integration with [**bias_adjustment**](https://earth.bsc.es/gitlab/digital-twins/de_340-2/bias_adjustment) has been introduced. To make use of it, it needs to be installed manually at the tag [`v0.5.3`](https://earth.bsc.es/gitlab/digital-twins/de_340-2/bias_adjustment/-/tags/v0.5.3) in the virtual environment.

## Documentation 

All the information on how to install, configure and run the one_pass package is given the documentation, found in the docs folder. Here you will find the source code to build the online and pdf version of the documentation. 

To build the online version of the documentation you must clone the repo (see above), make sure you've activated the `env_opa` environment and 

```
cd docs
make html 

```
 
This will build the oneline documentation in the folder `docs/build/html`. To access the documentation you can then click on `index.html` which will generate the webpage docs. If you have built the documentation on a remote platform (i.e. HPC) you can copy the documentation to your local machine via: 

```
scp -r user@ssh_platform:~/path_to_one_pass/one_pass/docs/build/html /path/on/local/machine/to/docs

``` 
For example if you're on Levante `user@remote_platform` should be `username@levante.dkrz.de`. This will copy the documentation on to your local machine and you can click on `index.html` to view. 

To build the pdf version of the docs, again you must clone the repo (see above), make sure you've activated the `env_opa` environment and

```
cd docs
make latexpdf

```
which will make the pdf version of the docs in the folder `docs/build/latex`. It might be necessary to load a module such as `texlive` to enable the build of the pdf.

## Support

For all feedback, comments and issues, feel free to [open an issue](https://earth.bsc.es/gitlab/digital-twins/de_340-2/one_pass/-/issues) or send [me](https://www.bsc.es/alsina-ferrer-ivan) an email.
