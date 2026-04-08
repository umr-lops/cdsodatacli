.. _usage:


Do a query through CDSE Odata API
=================================
This is an example of how to use the cdsodatacli package to query and download data from the Copernicus Data Space Ecosystem (CDSE) Odata API.
A query or a request on CDSE, only provides meta-data information about the products available, not the data itself.


.. code-block:: bash

    queryCDS -h
    # example of a query
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



Get CDSE Odata IDs for a list of products
=========================================

.. code-block:: bash

    get-odata-ids -h
        usage: get-odata-ids [-h] [--verbose] [--output-listing OUTPUT_LISTING] --input-listing INPUT_LISTING [--email EMAIL] [--password PASSWORD]

        example main

        options:
        -h, --help            show this help message and exit
        --verbose
        --output-listing OUTPUT_LISTING
                                output listing where the SAFE+id will be written [optional, default is input-listing+'.ids']
        --input-listing INPUT_LISTING
                                input SAFE listing path containing only SAFE basenames
        --email EMAIL         email of the CDSE account to use for queries [optional, default None -> use cdsodatacli default behavior]
        --password PASSWORD   password of the CDSE account to use for queries [optional, default None -> use cdsodatacli default behavior]


Get equivalent S1 product types
===============================

When you have a list of Sentinel-1 products, you may want to get the equivalent product types (for example to know which S1 GRD products correspond to a list of S1 SLC products).
You can use the command line tool `match_s1_product_types`:

.. code-block:: bash

    match_s1_prodtypes -h
        usage: match_s1_prodtypes [-h] (--safe SAFE_ID [SAFE_ID ...] | --input-listing FILE) --prodtype TYPE --output FILE [--dev] [--verbose]

        Match Sentinel-1 SAFE products to a given product type via OData.

        options:
        -h, --help            show this help message and exit
        --safe SAFE_ID [SAFE_ID ...]
                                One or more Sentinel-1 SAFE product IDs passed directly on the command line.
        --input-listing FILE  Path to a text file with one SAFE product ID per line (blank lines ignored).
        --prodtype TYPE       Target product type to search for. Choices: GRDH, GRDM, SLC_, OCN_, RAW_
        --output FILE         Output text file to write results to (one match per line).
        --dev                 Enable dev mode with reduced number of SAFE to treat.
        --verbose, -v         Enable DEBUG-level logging.

        Examples:
        # From an inline list, look for OCN products
        python s1_matchup.py --prodtype OCN_ \
            --safe S1A_IW_GRDH_1SDV_20230726T071112_... S1A_IW_GRDH_1SDV_20230726T071137_...

        # From a file (one SAFE ID per line), look for SLC products
        python s1_matchup.py --prodtype SLC_ --input-listing my_products.txt

        # Enable verbose/debug logging
        python s1_matchup.py --prodtype OCN_ --input-listing list.txt --verbose
