.. _usage:


Do a query through CDSE Odata API
=================================
This is an example of how to use the cdsodatacli package to query and download data from the Copernicus Data Space Ecosystem (CDSE) Odata API.
A query or a request on CDSE, only provides meta-data information about the products available, not the data itself.


.. code-block:: python

    queryCDS -h
    # or
    queryCDS --collection SENTINEL-1 --startdate 20230101T00:00:00 --stopdate 20230105T10:10:10 --mode IW --product SLC --querymode seq --geometry "POINT (-5.02 48.4)"


or use the method `cdsodatacli.query.fetch_data()` see `cdsodatacli` API documentation: :ref:`basic_api`.

Download a listing of product
==============================

Once you have a listing of products (for example from a query), you can download the products.
There  are two main ways to download products:


1. Single-threaded download with a given user/login
----------------------------------------------------

For a download with a given login/password, you can use the command line tool `downloadFromCDS`:

.. code-block:: bash

    downloadFromCDS -h

2. Multi-threaded download with multiple users/accounts
-------------------------------------------------------
For a multi-threaded download with multiple users/accounts, you can use the command line tool `downloadMuliThreadMultiUser`:

.. code-block:: bash

    downloadMuliThreadMultiUser -h
