#!/usr/bin/env bash

if command -v micromamba >/dev/null; then
  conda=micromamba
elif command -v mamba >/dev/null; then
  conda=mamba
else
  conda=conda
fi
conda remove -y --force cytoolz numpy xarray construct toolz fsspec python-dateutil pandas
python -m pip install \
  -i https://pypi.anaconda.org/scientific-python-nightly-wheels/simple \
  --no-deps \
  --pre \
  --upgrade \
  numpy \
  pandas \
  geopandas
python -m pip install --upgrade \
  git+https://github.com/construct/construct \
  git+https://github.com/pytoolz/toolz \
  git+https://github.com/fsspec/filesystem_spec \
  git+https://github.com/dateutil/dateutil
