# cdsodatacli

<div align="center">

[![Build status](https://github.com/umr-lops/cdsodatacli/workflows/build/badge.svg?branch=main&event=push)](https://github.com/umr-lops/cdsodatacli/actions?query=workflow%3Abuild)
[![Python Version](https://img.shields.io/pypi/pyversions/cdsodatacli.svg)](https://pypi.org/project/cdsodatacli/)
[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/umr-lops/cdsodatacli/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/umr-lops/cdsodatacli/blob/main/.pre-commit-config.yaml)
[![Semantic Versions](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg)](https://github.com/umr-lops/cdsodatacli/releases)
[![License](https://img.shields.io/github/license/umr-lops/cdsodatacli)](https://github.com/umr-lops/cdsodatacli/blob/main/LICENSE)
![Coverage Report](assets/images/coverage.svg)

odata client for Copernicus Data Space catalog

</div>

## Very first steps

### step 1: create a config file

- create a copy of the config.yml called localconfig.yml where `cdsodatacli` is installed in your python environement.

```bash
cp config.yml localconfig.yml
```

- edit the localconfig.yml to set your own path for output directories and CDSE accounts (including S3 credentials)

```bash
 vi  localconfig.yml

```

content of the configuration file to adapt is typically like this:

```yaml
example_group_of_logins:
  dummy-email@foobar.com:
    cdse-psswd: a-passord
    s3-access-key: a-s3-key
    s3-secret: a-s3-secret
URL_identity: https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
URL_download: https://zipper.dataspace.copernicus.eu/odata/v1/Products(%s)/$value
spools:
  default: "./my_spool"
pre_spool: "./sentinel1_cdse_pre_spool"
archives:
  default: "./my_archive"
test_default_output_directory: "./my_tests"
s3_endpoint: "https://eodata.dataspace.copernicus.eu"
s3_bucket: "eodata"
s3_region: "default"
list_sar_unit_private_data: []
```

### step 2: do a query on CDSE Odata API

```bash
queryCDS -h
# or
queryCDS --collection SENTINEL-1 --startdate 20230101T00:00:00 --stopdate 20230105T10:10:10 --mode IW --product SLC --querymode seq --geometry "POINT (-5.02 48.4)"
```

or use the method `cdsodatacli.query.fetch_data()`

### step 3: download a listing of product

```bash
downloadFromCDS -h
```

## Installation

```bash
pip install -U cdsodatacli

```

## Project Structure

```
.
├── assets
│   └── images
│       └── coverage.svg
├── cdsodatacli              # Main package directory
│   ├── __init__.py
│   ├── config.yml          # Default configuration
│   ├── download.py         # Download logic
│   ├── example.py
│   ├── fetch_access_token.py
│   ├── localconfig.yml     # Local configuration (to be created by user)
│   ├── product_parser.py
│   ├── query.py            # Query logic
│   ├── s3_path.py
│   ├── s3_temporary_access_token.py
│   ├── session.py
│   ├── utils.py
│   └── scripts             # Helper and utility scripts
│       ├── check_curl_query_with_specific_account.py
│       ├── convert_json_odata_to_txt_listing.py
│       ├── fetch_product_S1_worldwide.py
│       └── ...
├── docs                    # Documentation source
│   ├── examples
│   └── usage.rst
├── tests                   # Unit and integration tests
│   ├── API_OData_status_code_tests.py
│   ├── test_authentication.py
│   └── ...
├── ci                      # Continuous Integration configs
├── cdsodatacli_apptainer.def
├── Dockerfile
├── pyproject.toml
└── README.md
```

## 🛡 License

[![License](https://img.shields.io/github/license/umr-lops/cdsodatacli)](https://github.com/umr-lops/cdsodatacli/blob/main/LICENSE)

This project is licensed under the terms of the `MIT` license. See [LICENSE](https://github.com/umr-lops/cdsodatacli/blob/main/LICENSE) for more details.

## 📃 Citation

```bibtex
@misc{cdsodatacli,
  author = {umr-lops},
  title = {odata client for Copernicus Data Space catalog},
  year = {2023},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/umr-lops/cdsodatacli}}
}
```

## Credits [![🚀 Your next Python package needs a bleeding-edge project structure.](https://img.shields.io/badge/python--package--template-%F0%9F%9A%80-brightgreen)](https://github.com/TezRomacH/python-package-template)

This project was generated with [`python-package-template`](https://github.com/TezRomacH/python-package-template)
