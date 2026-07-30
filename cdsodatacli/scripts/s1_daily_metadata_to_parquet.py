#!/usr/bin/env python3
"""
Script to fetch daily Sentinel-1 metadata from CDSE and save as parquet files.

This script queries the Copernicus Data Space Ecosystem (CDSE) OData API
for all Sentinel-1 products (all modes, all product types, all polarizations)
over a given date range, excluding COG products. Results are saved as daily
parquet files for efficient storage and processing.

Usage:
    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data

    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data --config-file /path/to/config.yml --log-level DEBUG

    # Skip existing files (default)
    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data

    # Overwrite existing files
    fetch_s1_daily_metadata --start-date 2026-01-01 --stop-date 2026-01-31 --outputdir ./data --overwrite
"""

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Optional, Tuple
import traceback
import geopandas as gpd
import pandas as pd
import shapely
from tqdm import tqdm

from cdsodatacli.query import fetch_data
from cdsodatacli.utils import get_conf

logger = logging.getLogger(__name__)

# Constants
WORLDPOLYGON = shapely.wkt.loads("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))")


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
        help="Maximum parallel workers for multithreaded mode (default: 4).",
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

    Returns:
        tuple: (date, number_of_products, elapsed_time_seconds).
    """
    filepath = get_parquet_filepath(date, output_dir)

    # Check if file already exists
    if not overwrite and filepath.exists():
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
