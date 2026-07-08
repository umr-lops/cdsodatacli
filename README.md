# cdsodatacli

<div align="center">

[![Build status](https://github.com/umr-lops/cdsodatacli/workflows/build/badge.svg?branch=main&event=push)](https://github.com/umr-lops/cdsodatacli/actions?query=workflow%3Abuild)
[![Python Version](https://img.shields.io/pypi/pyversions/cdsodatacli.svg)](https://pypi.org/project/cdsodatacli/)
[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/umr-lops/cdsodatacli/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/umr-lops/cdsodatacli/blob/main/.pre-commit-config.yaml)
[![License](https://img.shields.io/github/license/umr-lops/cdsodatacli)](https://github.com/umr-lops/cdsodatacli/blob/main/LICENSE)
![Coverage Report](assets/images/coverage.svg)

**OData client for Copernicus Data Space Ecosystem (CDSE) catalog**

</div>

---

## Installation

```bash
pip install -U cdsodatacli
```

---

## CLI Entrypoints

The package provides several command-line tools:

### 1. `queryCDS` - Spatio-temporal product search

Search for products using spatial and temporal filters (geometry, date range, product type, etc.).

**Features:**
- Spatial filtering (Point, Polygon, global)
- Temporal range with automatic time slicing (`--batch-size`)
- Product type filtering (GRD, SLC, OCN, RAW)
- Mode filtering (IW, EW, SM, WV)
- Automatic pagination with `$$skip` (up to 10000 products)
- Checkpoint support for interrupted queries
- Cache support for repeated queries
- Sequential or multithreaded mode

**Examples:**

```bash
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
```

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

### 2. `get-metadata-from-product-names` - Get metadata from SAFE names or Product IDs

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

```bash
# From a file with SAFE names
get-metadata-from-product-names --input-listing safes.txt --output results.csv

# With checkpoint and cache for large lists
get-metadata-from-product-names --input-listing big_list.txt --output results.csv --checkpoint-dir ./ckpt --cache-dir ./cache

# With more parallel workers
get-metadata-from-product-names --input-listing safes.txt --output results.csv --max-workers 8

# With custom batch size
get-metadata-from-product-names --input-listing safes.txt --output results.csv --batch-size 30
```

**Key Options:**
- `--input-listing`: Text file with one SAFE name or UUID per line
- `--output`: CSV output file
- `--batch-size`: Products per OData query (default: 15)
- `--max-workers`: Parallel threads (default: 4)
- `--checkpoint-dir`: Resume interrupted runs
- `--cache-dir`: Cache OData responses

---

### 3. `match_s1_prodtypes` - Match Sentinel-1 product types

Find matching products of different types (e.g., find OCN products for a list of GRD products).

**Examples:**

```bash
# Match OCN products from a list of GRD SAFE names
match_s1_prodtypes --safe S1A_IW_GRDH_1SDV_20230726T071112_... --prodtype OCN_ --output matches.txt

# From a file with SAFE names
match_s1_prodtypes --input-listing grd_list.txt --prodtype SLC_ --output matches.txt

# With checkpoint for large lists
match_s1_prodtypes --input-listing big_list.txt --prodtype OCN_ --output matches.txt --checkpoint-dir ./ckpt
```

**Key Options:**
- `--safe`: List of SAFE names
- `--input-listing`: File with one SAFE name per line
- `--prodtype`: Target product type (GRDH, GRDM, SLC_, OCN_, RAW_)
- `--checkpoint-dir`: Resume interrupted runs

---

### 4. `get-odata-ids` - Get OData IDs from SAFE names

Extract OData product IDs from a list of SAFE names.

**Examples:**

```bash
get-odata-ids --input-listing safes.txt --output ids.csv
```

---

### 5. `downloadMultiThreadMultiUserS3` - Multi-threaded S3 download

Download products from CDSE S3 storage with multi-threading and multi-account support.

**Examples:**

```bash
downloadMultiThreadMultiUserS3 --input-listing products.csv --output-dir ./downloads
```

---

### 6. `countProductsLocally` - Count local products

Count products present in a local directory vs expected from a listing.

**Examples:**

```bash
countProductsLocally --listing products.csv --directory ./downloads
```

---

## Configuration

Create a configuration file `localconfig.yml` in your working directory or in the package location:

```yaml
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
```

---

## Project Structure

```
.
├── cdsodatacli              # Main package directory
│   ├── __init__.py
│   ├── config.yml           # Default configuration
│   ├── download.py          # Download logic
│   ├── fetch_access_token.py
│   ├── get_products_metadata.py  # Metadata from SAFE names
│   ├── product_parser.py    # SAFE name parsing
│   ├── query.py             # Query logic (spatio-temporal)
│   ├── rate_limiter.py      # Rate limiting
│   ├── retry.py             # Retry with backoff
│   ├── s3_path.py
│   ├── s3_temporary_access_token.py
│   ├── session.py
│   ├── utils.py
│   └── scripts/             # Helper scripts
│       ├── match_s1_product_types.py
│       ├── get_ids_listing_safe_iterative.py
│       ├── download_multithread_one_account_deamon.py
│       └── ...
├── tests/                   # Unit and integration tests
└── docs/                    # Documentation
```

---

## License

[![License](https://img.shields.io/github/license/umr-lops/cdsodatacli)](https://github.com/umr-lops/cdsodatacli/blob/main/LICENSE)

This project is licensed under the terms of the `MIT` license.

---

## Citation

```bibtex
@misc{cdsodatacli,
  author = {umr-lops},
  title = {OData client for Copernicus Data Space Ecosystem},
  year = {2023},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/umr-lops/cdsodatacli}}
}
```

---

## Credits

This project was generated with [`python-package-template`](https://github.com/TezRomacH/python-package-template)