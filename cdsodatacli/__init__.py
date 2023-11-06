# type: ignore[attr-defined]
"""odata client for Copernicus Data Space catalog"""

import sys
from cdsodatacli import *
from cdsodatacli.query import fetch_data


if sys.version_info >= (3, 8):
    from importlib import metadata as importlib_metadata
else:
    import importlib_metadata


def get_version() -> str:
    try:
        return importlib_metadata.version(__name__)
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


version: str = get_version()
__version__ = get_version()
