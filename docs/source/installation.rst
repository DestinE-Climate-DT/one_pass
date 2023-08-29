Installation
=============

The one_pass library is hosted in Git, at the BSC GitLab Earth Sciences repository, and v0.4 can be found on the `CSC gitHub <https://github.com/DestinE-Climate-DT>`__. The package is avalible through pip or git. We recommend using Mamba, a package manager for conda-forge, for setting up the python environment. 

Pre-requisites
------------------

Before installing one_pass, ensure you have the following pre-requisites installed: 

- `Git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`__

- `Mamba <https://mamba.readthedocs.io/en/latest/>`__ or `Conda <https://docs.conda.io/en/latest/>`__ 

- `Python <https://docs.python.org/3/>`__

Pip
------------

To install one_pass with pip, execute the following: 

.. code-block:: bash 
   
   pip install git+https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git@main

Git
-------------

You can clone the directory directly via Git using: 

.. code-block:: bash

   git clone https://earth.bsc.es/gitlab/digital-twins/de_340/one_pass.git

This will clone the main branch of the repository. 

Environments with Conda/Mamba
-------------------------------

Navigate to the one_pass directory: 

.. code-block:: bash

   cd one_pass

Create a new mamba environment with the required packages using the environment.yml file: 

.. code-block:: bash 
   
   mamba env create -f environment.yml

Finally activate the new environment by running: 

.. code-block:: bash
 
   conda activate env_opa

Installation with Docker
-----------------------------
When working on some HPC platforms it is not possible to directly use python conda environments. In this case we suggest running the one_pass environment inside a Docker container and copying to the HPC system as a Singularity container. To use this method, you first need both Docker (`instructions <https://docs.docker.com/engine/install/>`__) and Singularity (`instructions <https://docs.sylabs.io/guides/3.0/user-guide/installation.html>`__) installed on your **local** machine. 

1. First clone the git reposity to your local machine and navigate into the root directory.

2. Create the docker image using the command

.. code-block:: bash

   docker build -t one_pass:latest .

If you don't have permissions, add ``sudo`` to the beginning of the line. This will create your docker image. 

3. To enter the docker image run 
  
.. code-block:: bash
   
   docker run --rm -ti one_pass:latest /bin/bash

Again, if you don't have permissions, ``sudo`` to the beginning of the line. You are now running your Docker container with the environment ``(env_opa)``. To exit the container use ctrl + d.  

4. The next step is to create a singularity container so that you can move this docker image to any HPC system. To create the singularity file (you must have singularity installed on your local machine) run: 

.. code-block:: bash
  
   singularity build one_pass_singularity.sif docker-daemon://one_pass:latest

Again, if you don't have permissions add ``sudo`` at the beginning of the line.

5. To enter the singularity container:

.. code-block:: bash
    
   singularity shell one_pass_singularity.sif

6. Then to activate the environment: 

.. code-block:: bash

   source /usr/local/bin/_activate_current_env.sh

You now have your singularity container containing the one_pass environment that can be passed to any HPC machine. 

7. To copy the singularity container to LUMI you can run: 

.. code-block:: bash
  
   scp -r one_pass_singularity.sif lumi:.

This will pass your singularity container to LUMI (or any other machine you want). You can then enter the singularity container using step 5 and 6 above. 

Installing environment using containers
--------------------------------------------

The other option on LUMI is to use conda containers, as described `here <https://docs.lumi-supercomputer.eu/software/installing/container-wrapper/>`_. 

