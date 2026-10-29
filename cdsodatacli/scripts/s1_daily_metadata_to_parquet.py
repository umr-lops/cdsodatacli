#!/usr/bin/env python3
"""
Script to fetch daily Sentinel-1 metadata from CDSE and save as parquet files.

This script queries the Copernicus Data Space Ecosystem (CDSE) OData API
for all Sentinel-1 products (all product types, all polarizations) over a
given date range, excluding COG products. Only products in the operational
modes WV, IW, EW, S1-S6 are kept; other modes (e.g. N1-N6, EN, AI) are
dropped before saving. Results are saved as daily parquet files for
efficient storage and processing.

Usage:
    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data

    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data --config-file /path/to/config.yml --log-level DEBUG

    # Skip existing files (default)
    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data

    # Overwrite existing files
    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data --overwrite

    # Also store the WV imagettes (measurement file names and acquisition times) of
    # the WV OCN and WV SLC products. Existing files get the new column too.
    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data --wv-imagettes
"""

import argparse
import datetime
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import quote
import traceback
import geopandas as gpd
import pandas as pd
import shapely
from tqdm import tqdm

from cdsodatacli.fetch_access_token import get_access_token
from cdsodatacli.query import _GLOBAL_RATE_LIMITER, _fetch_with_retry, fetch_data
from cdsodatacli.utils import get_conf

logger = logging.getLogger(__name__)

# Constants
WORLDPOLYGON = shapely.wkt.loads("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))")

# Operational modes kept in the daily parquet files (see issue #150).
# Modes N1-N6 and EN/AI are dropped as they are not used downstream.
S1_MODES_TO_KEEP = ["WV", "IW", "EW", "S1", "S2", "S3", "S4", "S5", "S6"]

# WV OCN and WV SLC products hold one measurement file per imagette. The Nodes
# listing returns the file names (not the content) without downloading the product.
WV_IMAGETTE_PRODUCT_RE = re.compile(r"^S1[ABCD]_WV_(OCN__2S|SLC__1S)")
NODES_URL = (
    "https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})"
    "/Nodes({safe_name})/Nodes(measurement)/Nodes"
)
NODES_TIMEOUT_SECONDS = 30
WV_IMAGETTES_COLUMN = "WVImagettes"
# e.g. s1a-wv1-ocn-vv-20240918t060833-20240918t060836-055715-06ce05-001.nc
IMAGETTE_NAME_RE = re.compile(
    r"^s1[abcd]-wv[12]-(?:ocn|slc)-(?:vv|hh)-(?P<start>\d{8}t\d{6})-(?P<end>\d{8}t\d{6})"
    r"-\d{6}-[0-9a-f]{6}-(?P<index>\d{3})\.(?:nc|tiff)$"
)


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Fetch daily Sentinel-1 metadata from CDSE and save as parquet files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch data for January 2026
  fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data

  # Fetch data for a single day with custom config
  fetch_s1_daily_metadata --start-date 2026-01-15 --stop-date 2026-01-15 --outputdir ./data --config-file localconfig.yml

  # Enable debug logging
  fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-07 --outputdir ./data --log-level DEBUG

  # Overwrite existing parquet files
  fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data --overwrite
        """,
    )

    parser.add_argument(
        "--start-date",
        required=True,
        type=str,
        help="Start date in YYYY-MM-DD format (inclusive).",
    )

    parser.add_argument(
        "--stop-date",
        required=True,
        type=str,
        help="Stop date in YYYY-MM-DD format (inclusive).",
    )

    parser.add_argument(
        "--outputdir",
        required=True,
        type=str,
        help="Output directory where parquet files will be saved.",
    )

    parser.add_argument(
        "--config-file",
        type=str,
        default=None,
        help="Path to cdsodatacli configuration file (localconfig.yml).",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO).",
    )

    parser.add_argument(
        "--email",
        type=str,
        default=None,
        help="CDSE account email (overrides config file).",
    )

    parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="CDSE account password (overrides config file).",
    )

    parser.add_argument(
        "--cache-dir",
        type=str,
        default=None,
        help="Cache directory for OData responses.",
    )

    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=None,
        help="Checkpoint directory for resuming interrupted runs.",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=7,
        help="Number of days per time slice for splitting queries (default: 7).",
    )

    parser.add_argument(
        "--querymode",
        type=str,
        default="seq",
        choices=["seq", "multi"],
        help="Query mode: sequential or multithreaded (default: seq).",
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help=(
            "Maximum parallel workers for multithreaded mode and for the imagette "
            "listings (default: 4)."
        ),
    )

    parser.add_argument(
        "--wv-imagettes",
        action="store_true",
        default=False,
        help=(
            "Add the column WVImagettes: for each WV OCN and WV SLC product, the "
            "measurement file names with their acquisition start and end, listed "
            "through the OData Nodes endpoint (no download). Parquet files that "
            "already exist get the column without a new catalogue query."
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing parquet files. If False, skip days that already have a parquet file (default: False).",
    )

    return parser.parse_args()


def setup_logging(level: str = "INFO") -> None:
    """
    Configure logging for the script.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def validate_dates(
    start_date: str, stop_date: str
) -> Tuple[datetime.date, datetime.date]:
    """
    Validate and parse date strings.

    Args:
        start_date: Start date in YYYY-MM-DD format.
        stop_date: Stop date in YYYY-MM-DD format.

    Returns:
        tuple: (start_date_obj, stop_date_obj).

    Raises:
        ValueError: If dates are invalid or start_date > stop_date.
    """
    try:
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        stop = datetime.datetime.strptime(stop_date, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")

    if start > stop:
        raise ValueError(
            f"start-date ({start_date}) must be before stop-date ({stop_date})"
        )

    today = datetime.datetime.now(datetime.UTC).date()
    if stop > today:
        logger.warning(
            f"Stop date {stop_date} is in the future. Limiting to today ({today})."
        )
        stop = today

    return start, stop


def generate_daily_dates(
    start_date: datetime.date, stop_date: datetime.date
) -> list[datetime.date]:
    """
    Generate a list of daily dates between start_date and stop_date (inclusive).

    Args:
        start_date: Start date.
        stop_date: Stop date.

    Returns:
        list: List of datetime.date objects for each day.
    """
    dates = []
    current = start_date
    while current <= stop_date:
        dates.append(current)
        current += datetime.timedelta(days=1)
    return dates


def get_parquet_filepath(date: datetime.date, output_dir: Path) -> Path:
    """
    Get the parquet file path for a given date.

    Args:
        date: Date.
        output_dir: Output directory.

    Returns:
        Path: Path to the parquet file.
    """
    filename = f"s1_{date.strftime('%Y-%m-%d')}.parquet"
    return output_dir / filename


def create_query_gdf(date: datetime.date) -> gpd.GeoDataFrame:
    """
    Create a GeoDataFrame for a single day query.

    Args:
        date: Date to query.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with query parameters.
    """
    start_datetime = datetime.datetime.combine(
        date, datetime.time.min, tzinfo=datetime.UTC
    )
    end_datetime = datetime.datetime.combine(
        date, datetime.time.max, tzinfo=datetime.UTC
    )

    id_query = f"s1_daily_{date.strftime('%Y%m%d')}"

    gdf = gpd.GeoDataFrame(
        {
            "start_datetime": [start_datetime],
            "end_datetime": [end_datetime],
            "geometry": [WORLDPOLYGON],
            "collection": ["SENTINEL-1"],
            "name": [None],
            "sensormode": [None],
            "producttype": [None],
            "Attributes": [None],
            "id_query": [id_query],
        }
    )

    return gdf


def extract_operational_mode(attributes) -> Optional[str]:
    """
    Extract the operationalMode value from a product Attributes entry.

    Args:
        attributes: Product Attributes (list of dicts, JSON string, or None).

    Returns:
        Optional[str]: The operational mode (e.g. "IW") or None if not found.
    """
    if attributes is None:
        return None
    if isinstance(attributes, str):
        try:
            attributes = json.loads(attributes)
        except (ValueError, TypeError):
            return None
    if not isinstance(attributes, list):
        return None
    for attr in attributes:
        if isinstance(attr, dict) and attr.get("Name") == "operationalMode":
            return attr.get("Value")
    return None


def filter_by_operational_mode(
    df: pd.DataFrame,
    modes_to_keep: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Keep only rows whose operationalMode is in the allowed list.

    Rows without a resolvable operationalMode are dropped.

    Args:
        df: DataFrame containing an "Attributes" column.
        modes_to_keep: List of allowed operational modes.
            Defaults to S1_MODES_TO_KEEP.

    Returns:
        pd.DataFrame: Filtered DataFrame.
    """
    if df is None or df.empty or "Attributes" not in df.columns:
        return df

    if modes_to_keep is None:
        modes_to_keep = S1_MODES_TO_KEEP

    modes = df["Attributes"].apply(extract_operational_mode)
    mask = modes.isin(modes_to_keep)
    n_dropped = int((~mask).sum())
    if n_dropped > 0:
        dropped_modes = modes[~mask].dropna().unique().tolist()
        logger.info(
            f"Dropped {n_dropped} products with modes not in "
            f"{modes_to_keep}: {dropped_modes}"
        )
    return df[mask]


def is_wv_imagette_product(name: str) -> bool:
    """
    Tell whether a product is a WV OCN or WV SLC product.

    These products hold one measurement file per imagette. Other product types
    (RAW, IW, EW...) do not.

    Args:
        name: Product name, e.g. S1A_WV_OCN__2SSV_20240918T060833_..._E53B.SAFE.

    Returns:
        bool: True for WV OCN and WV SLC products.

    Examples:
        >>> is_wv_imagette_product(
        ...     "S1A_WV_OCN__2SSV_20240918T060833_20240918T064359_055715_06CE05_E53B.SAFE"
        ... )
        True
        >>> is_wv_imagette_product(
        ...     "S1A_WV_RAW__0SSV_20240918T060833_20240918T064359_055715_06CE05_AAAA.SAFE"
        ... )
        False
    """
    return bool(WV_IMAGETTE_PRODUCT_RE.match(name or ""))


def parse_imagette_names(names: list[str]) -> list[dict]:
    """
    Turn the measurement file names of a WV product into imagette records.

    The acquisition start and end of each imagette are written in its file name,
    e.g. s1a-wv1-ocn-vv-20240918t060833-20240918t060836-055715-06ce05-001.nc.

    Args:
        names: Measurement file names listed for one product.

    Returns:
        list[dict]: One record per file, sorted by imagette index, with keys
            Name, index and ContentDate (Start and End, UTC ISO 8601).

    Raises:
        ValueError: If a name does not follow the WV measurement naming, or if
            two files share the same index.

    Examples:
        >>> parse_imagette_names(
        ...     ["s1a-wv2-ocn-vv-20240918t060847-20240918t060850-055715-06ce05-002.nc"]
        ... )[0]["ContentDate"]
        {'Start': '2024-09-18T06:08:47Z', 'End': '2024-09-18T06:08:50Z'}
    """
    imagettes = []
    for name in names:
        match = IMAGETTE_NAME_RE.match(name)
        if match is None:
            raise ValueError(f"Unexpected WV measurement file name: {name}")
        start, end = (
            datetime.datetime.strptime(token, "%Y%m%dt%H%M%S").strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            for token in match.group("start", "end")
        )
        imagettes.append(
            {
                "Name": name,
                "index": int(match.group("index")),
                "ContentDate": {"Start": start, "End": end},
            }
        )
    imagettes.sort(key=lambda item: item["index"])
    indices = [item["index"] for item in imagettes]
    if len(indices) != len(set(indices)):
        raise ValueError("Duplicate imagette index in the measurement files")
    return imagettes


def fetch_imagette_names(
    product_id: str, safe_name: str, headers: Optional[dict] = None
) -> list[str]:
    """
    List the measurement file names of one product, without downloading it.

    This uses the OData Nodes listing, which returns names only. It shares the
    global rate limiter of the catalogue queries.

    Args:
        product_id: CDSE product Id (UUID).
        safe_name: Product name ending with .SAFE.
        headers: Optional authentication headers.

    Returns:
        list[str]: Names of the files in the measurement folder.
    """
    url = NODES_URL.format(product_id=product_id, safe_name=quote(safe_name))
    _GLOBAL_RATE_LIMITER.wait_if_needed()
    json_data = _fetch_with_retry(url, headers, NODES_TIMEOUT_SECONDS)
    return [node["Name"] for node in json_data["result"]]


def get_wv_imagettes_json(
    product_id: str, safe_name: str, headers: Optional[dict] = None
) -> Optional[str]:
    """
    Get the imagette records of one WV product as a JSON string.

    Args:
        product_id: CDSE product Id (UUID).
        safe_name: Product name ending with .SAFE.
        headers: Optional authentication headers.

    Returns:
        Optional[str]: JSON list of imagette records, or None if the listing or
            the parsing failed (the error is logged).
    """
    try:
        names = fetch_imagette_names(product_id, safe_name, headers)
        return json.dumps(parse_imagette_names(names))
    except Exception as e:
        logger.error(f"Could not get the imagettes of {safe_name}: {e}")
        return None


def add_wv_imagettes_column(
    df: pd.DataFrame, headers: Optional[dict] = None, max_workers: int = 4
) -> pd.DataFrame:
    """
    Add the imagette records of the WV OCN and WV SLC products to a DataFrame.

    The column WVImagettes holds a JSON list per product. It stays empty (None) for
    other products and for products whose listing failed. Rows that already have
    a value are not queried again, so the function can be run twice to retry the
    failures.

    Args:
        df: DataFrame with the columns Id and Name.
        headers: Optional authentication headers.
        max_workers: Number of parallel listing requests.

    Returns:
        pd.DataFrame: Copy of df with the WVImagettes column.
    """
    df = df.copy().reset_index(drop=True)
    if WV_IMAGETTES_COLUMN not in df.columns:
        df[WV_IMAGETTES_COLUMN] = None
    todo = df.index[
        df["Name"].apply(is_wv_imagette_product) & df[WV_IMAGETTES_COLUMN].isna()
    ]
    if len(todo) == 0:
        return df
    logger.info(f"Listing the imagettes of {len(todo)} WV products")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = list(
            pool.map(
                lambda i: get_wv_imagettes_json(
                    df.at[i, "Id"], df.at[i, "Name"], headers
                ),
                todo,
            )
        )
    df.loc[todo, WV_IMAGETTES_COLUMN] = results
    n_failed = sum(result is None for result in results)
    if n_failed:
        logger.warning(f"{n_failed}/{len(todo)} imagette listings failed")
    return df


def enrich_existing_parquet(
    filepath: Path,
    headers: Optional[dict] = None,
    max_workers: int = 4,
) -> Optional[int]:
    """
    Add the WVImagettes column to a daily parquet file that already exists.

    The file is rewritten through a temporary file only if something changed.

    Args:
        filepath: Daily parquet file.
        headers: Optional authentication headers.
        max_workers: Number of parallel listing requests.

    Returns:
        Optional[int]: Number of products in the file if it was rewritten, None
            if there was nothing to add.
    """
    df = pd.read_parquet(filepath)
    before = df[WV_IMAGETTES_COLUMN].isna().sum() if WV_IMAGETTES_COLUMN in df else None
    enriched = add_wv_imagettes_column(df, headers=headers, max_workers=max_workers)
    if before is not None and enriched[WV_IMAGETTES_COLUMN].isna().sum() == before:
        return None
    tmp_path = filepath.with_suffix(".parquet.tmp")
    enriched.to_parquet(tmp_path, index=False, engine="pyarrow")
    tmp_path.replace(filepath)
    logger.info(f"Added the WV imagettes to {filepath}")
    return len(enriched)


def convert_complex_columns_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all columns to string representation for parquet serialization.
    Uses json.dumps() for dicts/lists to ensure valid JSON format with double quotes.
    """

    def to_str(value):
        """Convert any value to a string representation."""
        if value is None:
            return None
        # For dicts and lists, use json.dumps to get double quotes
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value)
            except Exception:
                return str(value)
        # For shapely geometries
        if hasattr(value, "to_wkt"):
            try:
                return value.to_wkt()
            except Exception:
                return str(value)
        # For datetime
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        return str(value)

    df_copy = df.copy()

    # Convert all columns to string
    for col in df_copy.columns:
        try:
            df_copy[col] = df_copy[col].apply(to_str)
        except Exception as e:
            logger.warning(f"Could not convert column '{col}': {e}")
            try:
                df_copy[col] = df_copy[col].astype(str)
            except Exception:
                df_copy = df_copy.drop(columns=[col])
                logger.warning(f"Dropped column '{col}'")

    return df_copy


def save_to_parquet(
    df: pd.DataFrame,
    date: datetime.date,
    output_dir: Path,
) -> Path:
    """
    Save a DataFrame to a parquet file.

    Args:
        df: DataFrame to save.
        date: Date for the filename.
        output_dir: Output directory.

    Returns:
        Path: Path to the saved file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    filepath = get_parquet_filepath(date, output_dir)

    # Convert complex columns
    df_clean = convert_complex_columns_to_string(df)

    # Ensure all object columns are strings
    for col in df_clean.select_dtypes(include=["object"]).columns:
        try:
            # Test if conversion works
            df_clean[col] = df_clean[col].astype(str)
        except Exception as e:
            logger.warning(f"Could not convert column {col} to string: {e}")
            # Try to handle numpy arrays specifically
            try:
                df_clean[col] = df_clean[col].apply(
                    lambda x: str(x) if x is not None and not pd.isna(x) else None
                )
            except Exception as e2:
                logger.error(f"Failed to convert column {col}: {e2}")
                # Drop the problematic column
                df_clean = df_clean.drop(columns=[col])
                logger.warning(f"Dropped column {col} due to conversion issues")

    try:
        df_clean.to_parquet(filepath, index=False, engine="pyarrow")
    except Exception as e:
        logger.warning(f"Failed to save with pyarrow: {e}. Trying fastparquet...")
        try:
            df_clean.to_parquet(filepath, index=False, engine="fastparquet")
        except Exception as e2:
            logger.error(f"Failed to save with fastparquet: {e2}")
            # Final fallback: convert everything to string
            for col in df_clean.select_dtypes(include=["object"]).columns:
                df_clean[col] = df_clean[col].astype(str)
            df_clean.to_parquet(filepath, index=False, engine="pyarrow")

    logger.info(f"Saved {len(df_clean)} products to {filepath}")
    return filepath


def process_day(
    date: datetime.date,
    output_dir: Path,
    overwrite: bool = False,
    email: Optional[str] = None,
    password: Optional[str] = None,
    cache_dir: Optional[str] = None,
    checkpoint_dir: Optional[str] = None,
    batch_size: int = 7,
    querymode: str = "seq",
    display_tqdm: bool = True,
    wv_imagettes: bool = False,
    max_workers: int = 4,
) -> Tuple[datetime.date, int, float]:
    """
    Process a single day: query CDSE and save results.

    Args:
        date: Date to process.
        output_dir: Output directory.
        overwrite: If False, skip if parquet file already exists.
        email: CDSE account email.
        password: CDSE account password.
        cache_dir: Cache directory.
        checkpoint_dir: Checkpoint directory.
        batch_size: Days per time slice.
        querymode: Query mode ('seq' or 'multi').
        display_tqdm: Whether to display progress bar.
        wv_imagettes: Add the WVImagettes column (WV OCN and WV SLC measurement files).
        max_workers: Parallel workers for the imagette listings.

    Returns:
        tuple: (date, number_of_products, elapsed_time_seconds).
    """
    filepath = get_parquet_filepath(date, output_dir)
    headers = get_access_token(email, password) if wv_imagettes and email else None

    # Check if file already exists
    if not overwrite and filepath.exists():
        if wv_imagettes:
            start_time = datetime.datetime.now()
            n_products = enrich_existing_parquet(
                filepath, headers=headers, max_workers=max_workers
            )
            if n_products is not None:
                elapsed = (datetime.datetime.now() - start_time).total_seconds()
                return date, n_products, elapsed
        logger.info(
            f"Skipping {date.strftime('%Y-%m-%d')} - parquet file already exists "
            f"(use --overwrite to force re-download)"
        )
        return date, -2, 0.0

    logger.info(f"Processing {date.strftime('%Y-%m-%d')}...")

    gdf = create_query_gdf(date)
    top = 1000
    start_time = datetime.datetime.now()

    try:
        result_df = fetch_data(
            gdf=gdf,
            timedelta_slice=datetime.timedelta(days=batch_size),
            min_sea_percent=None,
            top=top,
            cache_dir=cache_dir,
            querymode=querymode,
            email=email,
            password=password,
            display_tqdm=display_tqdm,
            checkpoint_dir=checkpoint_dir,
        )

        if result_df is None or result_df.empty:
            logger.info(f"No products found for {date.strftime('%Y-%m-%d')}")
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            return date, 0, elapsed

        n_before = len(result_df)
        result_df = filter_by_operational_mode(result_df)
        n_after = len(result_df)
        if n_after == 0:
            logger.info(f"No products in kept modes for {date.strftime('%Y-%m-%d')}")
            elapsed = (datetime.datetime.now() - start_time).total_seconds()
            return date, 0, elapsed

        if n_after < n_before:
            logger.info(
                f"Kept {n_after}/{n_before} products in modes {S1_MODES_TO_KEEP}"
            )

        # --- DEBUG: Log column types before conversion ---
        # logger.debug(f"Columns before conversion: {result_df.columns.tolist()}")
        # for col in result_df.columns:
        #     if result_df[col].dtype == "object":
        #         # Check first non-null value type
        #         for val in result_df[col].head(10):
        #             breakpoint()
        #             if val is not None and not pd.isna(val):
        #                 logger.debug(f"Column '{col}' sample type: {type(val)}")
        #                 if hasattr(val, "shape"):
        #                     logger.debug(f"  - shape: {val.shape}")
        #                 break

        if wv_imagettes:
            result_df = add_wv_imagettes_column(
                result_df, headers=headers, max_workers=max_workers
            )

        save_to_parquet(result_df, date, output_dir)

        elapsed = (datetime.datetime.now() - start_time).total_seconds()
        logger.info(
            f"Finished {date.strftime('%Y-%m-%d')}: {len(result_df)} products in {elapsed:.1f}s"
        )

        return date, len(result_df), elapsed

    except KeyboardInterrupt:
        logger.warning(f"Interrupted during processing of {date.strftime('%Y-%m-%d')}")
        raise
    except Exception as e:
        logger.exception("traceback added : %s", traceback.format_exc())
        logger.error(f"Error processing {date.strftime('%Y-%m-%d')}: {e}")
        elapsed = (datetime.datetime.now() - start_time).total_seconds()
        return date, -1, elapsed


def get_credentials(
    config_file: Optional[str], email: Optional[str], password: Optional[str]
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get credentials from config file or command line.

    Args:
        config_file: Path to config file.
        email: Email from command line.
        password: Password from command line.

    Returns:
        tuple: (email, password).
    """
    if email and password:
        return email, password

    if config_file:
        try:
            config = get_conf(config_file)
            for group_name, group_config in config.items():
                if isinstance(group_config, dict):
                    for user_email, user_config in group_config.items():
                        if "@" in user_email and "cdse-psswd" in user_config:
                            logger.info(f"Using credentials from config: {user_email}")
                            return user_email, user_config["cdse-psswd"]
        except Exception as e:
            logger.warning(f"Failed to load config file: {e}")

    return None, None


def main() -> None:
    """Main entry point for the script."""
    args = parse_args()

    setup_logging(args.log_level)

    logger.info("Starting Sentinel-1 daily metadata fetch")
    logger.info(f"Date range: {args.start_date} to {args.stop_date}")
    logger.info(f"Output directory: {args.outputdir}")
    logger.info(f"Query mode: {args.querymode}")
    logger.info(f"Overwrite existing files: {args.overwrite}")
    logger.info(f"Add WV imagettes: {args.wv_imagettes}")

    try:
        start_date, stop_date = validate_dates(args.start_date, args.stop_date)
        dates = generate_daily_dates(start_date, stop_date)
        logger.info(f"Processing {len(dates)} days")

        output_dir = Path(args.outputdir)
        output_dir.mkdir(parents=True, exist_ok=True)

        email, password = get_credentials(args.config_file, args.email, args.password)

        results = []
        total_products = 0
        total_time = 0.0
        skipped_days = 0

        for date in tqdm(dates, desc="Processing days", unit="day"):
            try:
                date_obj, num_products, elapsed = process_day(
                    date=date,
                    output_dir=output_dir,
                    overwrite=args.overwrite,
                    email=email,
                    password=password,
                    cache_dir=args.cache_dir,
                    checkpoint_dir=args.checkpoint_dir,
                    batch_size=args.batch_size,
                    querymode=args.querymode,
                    display_tqdm=False,
                    wv_imagettes=args.wv_imagettes,
                    max_workers=args.max_workers,
                )

                if num_products == -2:
                    # File already exists and overwrite is False
                    skipped_days += 1
                    results.append((date_obj, -2, elapsed))
                elif num_products >= 0:
                    total_products += num_products
                    total_time += elapsed
                    results.append((date_obj, num_products, elapsed))
                else:
                    logger.error(f"Failed to process {date.strftime('%Y-%m-%d')}")
                    results.append((date_obj, -1, elapsed))

            except KeyboardInterrupt:
                logger.info("Interrupted by user.")
                break
            except Exception as e:
                logger.error(f"Unexpected error for {date.strftime('%Y-%m-%d')}: {e}")
                continue

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)

        processed_days = [r for r in results if r[1] >= 0]
        skipped = [r for r in results if r[1] == -2]
        failed = [r for r in results if r[1] == -1]

        logger.info(f"Total days in range: {len(dates)}")
        logger.info(f"Days processed: {len(processed_days)}")
        logger.info(f"Days skipped (file already exists): {len(skipped)}")
        logger.info(f"Days failed: {len(failed)}")
        logger.info(f"Total products found: {total_products}")
        logger.info(f"Total time: {total_time:.1f}s")

        if len(processed_days) > 0:
            logger.info(
                f"Average products per day: {total_products / len(processed_days):.1f}"
            )
            logger.info(
                f"Average time per day: {total_time / len(processed_days):.1f}s"
            )
            if total_time > 0:
                logger.info(f"Throughput: {total_products / total_time:.1f} products/s")

        if len(failed) > 0:
            logger.warning("Failed days:")
            for date, _, _ in failed[:10]:
                logger.warning(f"  - {date.strftime('%Y-%m-%d')}")
            if len(failed) > 10:
                logger.warning(f"  ... and {len(failed) - 10} more")

        logger.info("=" * 60)
        logger.info(f"Output directory: {output_dir}")
        logger.info("Done.")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
