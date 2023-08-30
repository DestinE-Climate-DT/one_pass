# One pass algorithms

This repository holds the code for the development and implementation of the one_pass algorithms for the DestinationEarth project. These algorithms are intelligent data reduction techniques to process streamed climate model output data on-the-fly to produce statistical summaries or derived computations, taking the needs of the use cases into account. The work is contained in workpackage 9 (WP9).

The one_pass algorithms will eventually work with climate data streamted from the Generic State Vector (GSV) interface. The algorithms take as input any Xarray object (either a Dataset or a DataArray) from the GSV interface and will compute the requested statistics. 

## Comments on the revision of WP9 deliverable DE340.9.1.1
Most of what I am sharing here are comments that I belive will enhance deliverabe DE340.9.1.1.

First, I must say the code works and I could not find any erros in the equations.

Here is a list of my comments:

1. I've observed that there isn't a clearly defined naming convention for 'ONE PASS'. In the Word document titled "Report on the Draft Implementation of the One-Pass Algorithms for Intelligent Data Reduction", it seems that you've opted to use 'one-pass' instead of 'one_pass'. However, in this repository, 'one_pass' is the term used most of the time. My suggestion would be to prioritize using 'one_pass'. This would make it easier to update the Word document to match the repository's convention. Additionally, since the Python code itself refers to it as 'one_pass,' adopting this naming in the document would enhance overall consistency and uniformity.

2. I encountered the following error while attempting to generate the documentation. I didn't give the error much attention and instead proceeded to read the PDF version. It's possible that the process didn't work as expected because I attempted it in Levante?

```
(env_opa) [b382303@l40051 one_pass]$ cd docs/source
(env_opa) [b382303@l40051 source]$ make html
make: *** No rule to make target 'html'.  Stop.
```

3. As I was going through the documentation PDF, I came across a mistake in the file named `the_config_file.rst`. I took the initiative to fix it myself.

4. As mentioned earlier, throughout the repository, the term 'one_pass' is consistently used. Additionally, I observed that the Documentation employs a naming convention for terms related to Xarray that differs from what I am accustomed to. Here, I've included the comment that I previously left in the Word document: "I would use the naming documentation of Xarray, i.e. `Xarray`, `DataArray` and `Dataset` instead of `xArray`, `dataArray` and `dataSet`".

5. I have a question rather than a comment. Is there an established naming convention for the saved NetCDF files? For instance, would it follow a format like `tp_2020_daily.nc`? If such a convention exists, could you please direct me to where I can find more details about it? This could help to develop better some applications such as `mHM`.

I typically employ tools like black and isort to reformat Python files. However, in your case, the code was already neatly organized, with just a few occasional excess "spaces".

For further remarks or recommendations, I've included them directly in the modifications made to the original files. 

Overall, I didn't come across any substantial errors, just some tweaks.


### Disclaimer
The `one_pass` package is in a preliminary developement phase. Some features are still not implemented and you may encounter bugs. For feedback and issue reporting, feel free to open an issue in: https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass/-/issues
 
## Installation
As there is currently no package-based installation for the `one_pass` the source must be obtained. This can easily be done (on any platform with internet connection) by cloning the repository directly:

```
git clone https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git
cd one_pass
mamba env create -f environment.yml
conda activate env_opa

```
In any platform without interent connection, we have provided installation instructions using Docker in the documentation. 

## Documentation 

All the information on how to install, configure and run the one_pass package is given the documentation, found in the docs folder. Here you will find both a pdf version and the source code to build the online documentation. 

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

For all feedback, comments and issues, feel free to open an issue or email me at katherine.grayson@bsc.es


