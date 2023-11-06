.. _installing:

************
Installation
************

conda install
#############


.. code-block::

    conda create -n cdsodataclienv
    conda activate cdsodataclienv
    conda install -c conda-forge cdsodatacli

.. note::
    this is also working the same for mamba or micromamba

Update xsar_slc to the latest version
#####################################


To be up to date with the development team, it's recommended to update the installation using pip:

.. code-block::

    pip install git+https://github.com/umr-lops/cdsodatacli.git

or

.. code-block::

  git clone https://github.com/umr-lops/cdsodatacli.git
  cd cdsodatacli
  pip install -e .
