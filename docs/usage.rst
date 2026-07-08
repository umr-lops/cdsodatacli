.. _usage:

Usage
=====

This section describes the available command-line tools provided by `cdsodatacli` for interacting with the Copernicus Data Space Ecosystem (CDSE) OData API.

List of Entrypoints
-------------------

The package provides several CLI tools:

1. **queryCDS** - Spatio-temporal product search
2. **get-metadata-from-product-names** - Get metadata from SAFE names or Product IDs
3. **match_s1_prodtypes** - Match Sentinel-1 product types
4. **get-odata-ids** - Get OData IDs from SAFE names
5. **downloadMultiThreadMultiUserS3** - Multi-threaded S3 download
6. **countProductsLocally** - Count local products

---

1. queryCDS - Spatio-temporal product search
============================================

Search for products using spatial and temporal filters (geometry, date range, product type, etc.).

**Features:**
- Spatial filtering (Point, Polygon, global)
- Temporal range with automatic time slicing (--batch-size)
- Product type filtering (GRD, SLC, OCN, RAW)
- Mode filtering (IW, EW, SM, WV)
- Automatic pagination with `$skip` (up to 10000 products)
- Checkpoint support for interrupted queries
- Cache support for repeated queries
- Sequential or multithreaded mode

**Examples:**

.. code-block:: bash

    # Basic query with geometry
    queryCDS --collection SENTINEL-1 --startdate 20230101T00:00:00 --stopdate 20230105T10:10:10 --mode IW --product SLC --geometry "POINT (-5.02 48.4)"

    # Global query with product filter and output
    queryCDS --collection SENTINEL-1 --startdate 20260604T00:00:02 --stopdate 20260614T00:01:59 --product GRD --output-safe-listing results.csv

    # With time slicing (3-day windows) to handle large periods
    queryCDS --collection SENTINEL-1 --startdate 20260604T00:00:02 --stopdate 20260614T00:01:59 --product GRD --output-safe-listing results.csv --batch-size 3

    # With checkpoint and cache
    queryCDS --collection SENTINEL-1 --startdate 20260604T00:00:02 --stopdate 20260614T00:01:59 --product GRD --output-safe-listing results.csv --checkpoint-dir ./ckpt --cache-dir ./cache

    # Multithreaded mode
    queryCDS --collection SENTINEL-1 --startdate 20230101T00:00:00 --stopdate 20230105T10:10:10 --querymode multi

**Key Options:**

- `--startdate`, `--stopdate`: Time range (format: YYYYMMDDTHH:MM:SS)
- `--geometry`: WKT geometry (Point or Polygon)
- `--collection`: SENTINEL-1, SENTINEL-2, etc.
- `--product`: GRD, SLC, OCN, RAW
- `--mode`: IW, EW, SM, WV
- `--batch-size`: Days per time slice (default: 14)
- `--checkpoint-dir`: Resume interrupted queries
- `--cache-dir`: Cache OData responses
- `--output-safe-listing`: CSV output file


---

2. get-metadata-from-product-names
==================================

Retrieve complete metadata for a list of known SAFE product names or Product UUIDs.

**Features:**
- Input: text file with one SAFE name or UUID per line
- Automatic detection of SAFE names vs UUIDs
- Batching (15 names per OData query)
- Parallel fetching with configurable workers
- Automatic deduplication and geometry conversion
- Checkpoint support
- Cache support

**Examples:**

.. code-block:: bash

    # From a file with SAFE names
    get-metadata-from-product-names --input-listing safes.txt --output results.csv

    # With checkpoint and cache for large lists
    get-metadata-from-product-names --input-listing big_list.txt --output results.csv --checkpoint-dir ./ckpt --cache-dir ./cache

    # With more parallel workers
    get-metadata-from-product-names --input-listing safes.txt --output results.csv --max-workers 8

    # With custom batch size
    get-metadata-from-product-names --input-listing safes.txt --output results.csv --batch-size 30

**Key Options:**

- `--input-listing`: Text file with one SAFE name or UUID per line
- `--output`: CSV output file
- `--batch-size`: Products per OData query (default: 15)
- `--max-workers`: Parallel threads (default: 4)
- `--checkpoint-dir`: Resume interrupted runs
- `--cache-dir`: Cache OData responses


---

3. match_s1_prodtypes - Match Sentinel-1 product types
======================================================

Find matching products of different types (e.g., find OCN products for a list of GRD products).

**Examples:**

.. code-block:: bash

    # Match OCN products from a list of GRD SAFE names
    match_s1_prodtypes --safe S1A_IW_GRDH_1SDV_20230726T071112_... --prodtype OCN_ --output matches.txt

    # From a file with SAFE names
    match_s1_prodtypes --input-listing grd_list.txt --prodtype SLC_ --output matches.txt

    # With checkpoint for large lists
    match_s1_prodtypes --input-listing big_list.txt --prodtype OCN_ --output matches.txt --checkpoint-dir ./ckpt

**Key Options:**

- `--safe`: List of SAFE names
- `--input-listing`: File with one SAFE name per line
- `--prodtype`: Target product type (GRDH, GRDM, SLC_, OCN_, RAW_)
- `--checkpoint-dir`: Resume interrupted runs


---

4. get-odata-ids - Get OData IDs from SAFE names
================================================

Extract OData product IDs from a list of SAFE names.

**Examples:**

.. code-block:: bash

    get-odata-ids --input-listing safes.txt --output ids.csv


---

5. downloadMultiThreadMultiUserS3 - Multi-threaded S3 download
==============================================================

Download products from CDSE S3 storage with multi-threading and multi-account support.

**Examples:**

.. code-block:: bash

    downloadMultiThreadMultiUserS3 --input-listing products.csv --output-dir ./downloads

**Options:**

- `--verbose`: Enable verbose output
- `--forcedownload`: Force download even if product exists locally
- `--logingroup`: Group of CDSE accounts in localconfig.yml (default: logins)
- `--listing`: Path to the listing file with product IDs and SAFE names
- `--outputdir`: Output directory for downloaded products
- `--cdsodatacli_conf_file`: Path to config file
- `--download-backend`: Choose between 'zipper' or 's3endpoint'

**Performance Comparison (2026 tests):**

+------------------+------------+--------------------------------------+---------------------------+
| Products         | backend    | average download speed (Mo/s)        | total duration (seconds)  |
+==================+============+======================================+===========================+
| 2 IW OCN         | s3endpoint | 10.4 Mo/s (stdev: 0.5 Mo/s)          | 10.0                      |
+------------------+------------+--------------------------------------+---------------------------+
| 3 IW SLC         | s3endpoint | 32.1 Mo/s (stdev: 15.1 Mo/s)         | 524.29                    |
+------------------+------------+--------------------------------------+---------------------------+
| 2 IW OCN         | zipper     | 5.9 Mo/s (stdev: 1.3 Mo/s)           | 23.04                     |
+------------------+------------+--------------------------------------+---------------------------+
| 3 IW SLC         | zipper     | 4.2 Mo/s (stdev: 2.5 Mo/s)           | 2896.93                   |
+------------------+------------+--------------------------------------+---------------------------+

.. note::
    S3 backend is up to 5X faster than zipper for some cases!
    With 's3endpoint' backend, products are uncompressed after download.


---

6. countProductsLocally - Count local products
==============================================

Count products present in a local directory vs expected from a listing.

**Examples:**

.. code-block:: bash

    countProductsLocally --listing products.csv --directory ./downloads


---

Programmatic Usage
===================

You can also use the package programmatically. See :ref:`basic_api` for the API documentation.

Key functions:
- `cdsodatacli.query.fetch_data()`: Spatio-temporal product search
- `cdsodatacli.get_products_metadata.main()`: Get metadata from SAFE names
- `cdsodatacli.download.download_products()`: Download products

Example:

.. code-block:: python

    import cdsodatacli.query as query

    # Create a GeoDataFrame with query parameters
    gdf = query.create_query_gdf(
        start_datetime="2023-01-01T00:00:00",
        end_datetime="2023-01-05T10:10:10",
        geometry="POINT (-5.02 48.4)",
        collection="SENTINEL-1",
        sensormode="IW",
        producttype="SLC"
    )

    # Fetch metadata
    results = query.fetch_data(gdf, top=1000)


---

Configuration
=============

Create a configuration file `localconfig.yml` in your working directory or in the package location:

.. code-block:: yaml

    example_group_of_logins:
      user@email.com:
        cdse-psswd: your-password
        s3-access-key: your-s3-key
        s3-secret: your-s3-secret
    URL_identity: https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
    URL_download: https://zipper.dataspace.copernicus.eu/odata/v1/Products(%s)/$value
    spools:
      default: "./my_spool"
    archives:
      default: "./my_archive"
    s3_endpoint: "https://eodata.dataspace.copernicus.eu"
    s3_bucket: "eodata"