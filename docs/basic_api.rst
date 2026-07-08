.. _basic_api:

####################
API Documentation
####################

This section provides the API documentation for the main modules of `cdsodatacli`.

..
    to document functions, add them to __all__ in ../cdsodatacli/__init__.py


cdsodatacli
===========

.. automodule:: cdsodatacli
    :members:


cdsodatacli.query
=================

Main module for querying the CDSE OData catalog.

**Functions:**

.. automodule:: cdsodatacli.query
    :members: fetch_data, query_client, normalize_gdf, create_urls, remove_duplicates, multy_to_poly, sea_percent


cdsodatacli.get_products_metadata
=================================

Module for retrieving metadata from known SAFE names or Product IDs.

**Functions:**

.. automodule:: cdsodatacli.get_products_metadata
    :members: main, validate_safe_name, create_urls_from_safe_names, create_urls_from_product_ids, fetch_all_batches, fetch_metadata_for_batch, post_process_data


cdsodatacli.download
====================

Module for downloading products from CDSE.

**Functions:**

.. automodule:: cdsodatacli.download
    :members: download_list_product_multithread_v4, cds_s3_download_one_product, filter_product_already_present


cdsodatacli.utils
=================

Utility functions for configuration, file operations, and data conversion.

**Functions:**

.. automodule:: cdsodatacli.utils
    :members: get_conf, check_safe_in_outputdir, check_safe_in_spool, WhichArchiveDir, check_safe_in_archive


cdsodatacli.session
===================

Session management for S3 downloads.

.. automodule:: cdsodatacli.session
    :members: get_sessions_download_available_s3


cdsodatacli.s3_temporary_access_token
=====================================

Functions for obtaining temporary S3 access tokens.

.. automodule:: cdsodatacli.s3_temporary_access_token
    :members: _get_fresh_s3_client


cdsodatacli.rate_limiter
========================

Rate limiting implementation for API calls.

.. automodule:: cdsodatacli.rate_limiter
    :members: RateLimiter


cdsodatacli.retry
=================

Retry logic with exponential backoff.

.. automodule:: cdsodatacli.retry
    :members: retry_with_backoff


cdsodatacli.product_parser
==========================

Parser for SAFE product names (Sentinel-1, Sentinel-2, Sentinel-3).

.. automodule:: cdsodatacli.product_parser
    :members: ExplodeSAFE


cdsodatacli.s3_path
===================

Utilities for S3 path handling.

.. automodule:: cdsodatacli.s3_path
    :members: get_s3path_from_safe


cdsodatacli.fetch_access_token
==============================

Authentication and token fetching.

.. automodule:: cdsodatacli.fetch_access_token
    :members: get_access_token


Example: Using fetch_data() for spatio-temporal queries
=======================================================

.. code-block:: python

    import cdsodatacli.query as query
    import geopandas as gpd
    from shapely import wkt
    import datetime

    # Create query parameters
    gdf = gpd.GeoDataFrame(
        {
            "start_datetime": [datetime.datetime(2023, 1, 1, 0, 0, 0)],
            "end_datetime": [datetime.datetime(2023, 1, 5, 10, 10, 10)],
            "geometry": [wkt.loads("POINT (-5.02 48.4)")],
            "collection": ["SENTINEL-1"],
            "sensormode": ["IW"],
            "producttype": ["SLC"],
            "id_query": ["my_query_id"],
        }
    )

    # Fetch metadata
    results = query.fetch_data(gdf=gdf, top=1000, cache_dir="./cache", querymode="seq")


Example: Using get-metadata-from-product-names
==============================================

.. code-block:: python

    from cdsodatacli.get_products_metadata import main
    import argparse

    # Command line usage is preferred:
    # get-metadata-from-product-names --input-listing safes.txt --output results.csv


Example: Downloading products
=============================

.. code-block:: python

    from cdsodatacli.download import download_list_product_multithread_v4

    # Download products from a listing
    results = download_list_product_multithread_v4(
        listing_path="products.csv", output_dir="./downloads", max_workers=4
    )
