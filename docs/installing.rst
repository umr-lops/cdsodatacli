.. _installing:

************
Installation
************

conda install
#############


.. code-block:: bash

    conda create -n cdsodataclienv
    conda activate cdsodataclienv
    conda install -c conda-forge cdsodatacli

.. note::
    this is also working the same for mamba or micromamba


docker image building
#####################
A dockerfile is provided to build an image with cdsodatacli installed. To build the image, run the following command in the root directory of the repository:

.. code-block:: bash

    docker build -f Dockerfile . -t cdsodatacli:latest


This will create a docker image named `cdsodatacli:latest` with cdsodatacli installed. You can then run a container from this image using the following command:

.. code-block:: bash

    docker run -i -t --rm cdsodatacli:latest /bin/bash
    # or to run directly a command
    docker run -i -t --rm cdsodatacli:latest downloadFromCDS -h



Update cdsodatacli to the latest version
########################################


To be up to date with the development team, it's recommended to update the installation using pip:

.. code-block:: bash

    pip install git+https://github.com/umr-lops/cdsodatacli.git

or

.. code-block:: bash

  git clone https://github.com/umr-lops/cdsodatacli.git
  cd cdsodatacli
  pip install -e .
