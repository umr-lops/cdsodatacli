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
  
* create a copy of the config.yml  called localconfig.yml where `cdsodatacli` is installed in your python environement.
```bash
cp config.yml localconfig.yml 
``` 
 * edit the localconfig.yml to set your own path for output directories and CDSE accounts
```bash
 vi  localconfig.yml
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
