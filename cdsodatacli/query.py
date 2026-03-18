import datetime
import logging
import os
import json
import hashlib
import requests
import pandas as pd
import argparse
from shapely import wkt
import geopandas as gpd
import shapely
from shapely.ops import unary_union
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import defaultdict
import traceback
import warnings
from geodatasets import get_path
import numpy as np
from cdsodatacli.fetch_access_token import get_access_token


DEFAULT_TOP_ROWS_PER_QUERY = 1000
WORLDPOLYGON = shapely.wkt.loads("POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))")


def time_based_hash(length=7):
    """Generates a short hash based on the current time in milliseconds.

    Args:
        length (int): The desired length of the resulting hex string. Defaults to 7.

    Returns:
        str: A truncated SHA-256 hash string.
    """
    now_ms = str(int(time.time() * 1000))  # milliseconds
    return hashlib.sha256(now_ms.encode()).hexdigest()[:length]


def query_client():
    """Main entry point for the MetaData query command line interface.

    Parses command line arguments, prepares the query GeoDataFrame, and executes
    the data fetching process.

    Returns:
        pd.DataFrame: A DataFrame containing the query results (footprint, Name, Id, etc.).
    """
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    parser = argparse.ArgumentParser(description="MetaData query to CDSE-OData API")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--collection",
        required=False,
        default="SENTINEL-1",
        help="SENTINEL-1 or SENTINEL-2 ... [optional, default=SENTINEL-1]",
    )
    parser.add_argument(
        "--startdate",
        required=True,
        help=" start date of sensor acquisition YYYYMMDDTHH:MM:SS",
    )
    parser.add_argument(
        "--stopdate",
        required=False,
        help=" stop date of sensor acquisition YYYYMMDDTHH:MM:SS [optional, default=now]",
        default=datetime.datetime.utcnow().strftime("%Y%m%dT%H:%M:%S"),
    )
    parser.add_argument(
        "--mode",
        choices=["EW", "IW", "WV", "SM"],
        help="sensor mode, could be EW, IW, WV or SM [optional]",
        default=None,
    )
    parser.add_argument(
        "--product",
        default=None,
        help="product type, could be GRD, SLC, RAW or  OCN or more specifically IW_GRDH_1S_PRIVATE [optional default=None -> no filtering]",
    )
    parser.add_argument(
        "--querymode",
        choices=["seq", "multi"],
        help="query mode: sequential or multithreaded [optional, default=seq]",
        default="seq",
    )
    parser.add_argument(
        "--geometry",
        required=False,
        default=None,
        help=" [optional, default=None -> global query] example: POINT (-5.02 48.4) or  POLYGON ((-12 35, 15 35, 15 58, -12 58, -12 35))",
    )
    parser.add_argument(
        "--id_query",
        required=False,
        default=None,
        help="unique identifier of the query to track it in the output results [optional, default is hash]",
    )
    parser.add_argument(
        "--email", required=False, default=None, help="CDSE account [optional]"
    )
    parser.add_argument(
        "--password", required=False, default=None, help="CDSE password [optional]"
    )
    parser.add_argument(
        "--top",
        required=False,
        default=None,
        help="max rows per query [optional, default=None -> 1000 (max allowed by OData)]",
    )
    parser.add_argument(
        "--output-safe-listing",
        help="output txt file containing the SAFE listing resulting from the query [optional]",
        required=False,
        default=None,
    )
    parser.add_argument(
        "--safename-pattern",
        help="pattern to filter SAFE names (e.g. S1A_IW_GRDH_1SDH_ or S1A or T160214) [optinal, default: None -> no filtering]",
        default=None,
    )
    parser.add_argument(
        "--cache-dir",
        help="path to cache directory to store and re-use previous queries [optional]",
        default=None,
    )
    parser.add_argument(
        "--minimum-sea-percent",
        type=float,
        default=None,
        help="minimum sea percent to filter out products based on their sea percent coverage [optional, default=None -> no filtering]",
    )

    args = parser.parse_args()
    fmt = "%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s"
    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    t0 = time.time()
    sta = datetime.datetime.strptime(args.startdate, "%Y%m%dT%H:%M:%S")
    sto = datetime.datetime.strptime(args.stopdate, "%Y%m%dT%H:%M:%S")
    id_query = time_based_hash() if args.id_query is None else args.id_query
    gdf = gpd.GeoDataFrame(
        {
            "start_datetime": [sta],
            "end_datetime": [sto],
            "geometry": [wkt.loads(args.geometry)],
            "collection": [args.collection],
            "name": [args.safename_pattern],
            "sensormode": [args.mode],
            "producttype": [args.product],
            "Attributes": [None],
            "id_query": [id_query],
        }
    )
    result_query = fetch_data(
        gdf,
        timedelta_slice=datetime.timedelta(days=14),
        min_sea_percent=None,
        top=args.top,
        cache_dir=args.cache_dir,
        querymode=args.querymode,
        email=args.email,
        password=args.password,
    )
    logging.info("time to query : %1.1f sec", time.time() - t0)
    if result_query is not None and result_query.empty is False:
        logging.info("number of product found: %s", len(result_query))
        if args.output_safe_listing is not None:
            os.makedirs(
                os.path.dirname(args.output_safe_listing), 0o0755, exist_ok=True
            )
            print("with Ids")
            result_query[["Id", "Name"]].to_csv(
                args.output_safe_listing, index=False, header=False
            )
            logging.info("SAFE listing saved to : %s", args.output_safe_listing)
            os.chmod(args.output_safe_listing, 0o0644)
    else:
        if args.output_safe_listing is not None:
            os.makedirs(
                os.path.dirname(args.output_safe_listing), 0o0755, exist_ok=True
            )
            logging.info(
                "no product found -> empty listing saved on disk: %s",
                args.output_safe_listing,
            )
            touch(args.output_safe_listing)
            os.chmod(args.output_safe_listing, 0o0644)
    return result_query


def touch(fname, times=None):
    """Sets the access and modification times of a file. Creates it if it doesn't exist.

    Args:
        fname (str): Path to the file.
        times (tuple, optional): A 2-tuple of (atime, mtime). Defaults to current time.
    """
    fhandle = open(fname, "w")
    try:
        os.utime(fname, times)
    finally:
        fhandle.close()


def fetch_data(
    gdf,
    timedelta_slice=None,
    min_sea_percent=None,
    top=None,
    cache_dir=None,
    querymode="seq",
    email=None,
    password=None,
    display_tqdm=False,
):
    """Fetches meta-data of CDSE products based on provided parameters.

    Splits the input GeoDataFrame by the `id_query` column and executes fetching
    for each unique query ID.

    Args:
        gdf (gpd.GeoDataFrame): Geospatial data for the query.

            Mandatory columns:
                - 'start_datetime': Starting date for the query.
                - 'end_datetime': Ending date for the query.
                - 'id_query': Unique identifier of the query.

            Optional columns:
                - 'name': SAFE name pattern.
                - 'collection': e.g., SENTINEL-1.
                - 'sensormode': e.g., IW.
                - 'producttype': e.g., GRD.
                - 'geometry': shapely geometry for area of interest.
                - 'Attributes': Extra OData filters.

        timedelta_slice (datetime.timedelta, optional): Time window size to split queries
            to avoid OData's 1000 product limit.
        min_sea_percent (float, optional): Minimum sea percent to filter products.
        top (int, optional): Max rows per individual OData query.
        cache_dir (str, optional): Path to directory for storing/reusing results.
        querymode (str): 'seq' (sequential) or 'multi' (multithreaded). Defaults to 'seq'.
        email (str, optional): CDSE account email for authentication.
        password (str, optional): CDSE account password.
        display_tqdm (bool): Whether to show a progress bar. Defaults to False.

    Returns:
        pd.DataFrame: Concatenated meta-data results from all queries.
    """
    if email and password:
        headers = get_access_token(email, password)
    collected_data = None
    # split the gdf in subsets based on the query_id
    unique_query_ids = gdf["id_query"].unique()
    pbar = tqdm(
        range(len(unique_query_ids)),
        disable=not display_tqdm,
        desc="individual CDSE queries",
    )
    # for query_id in unique_query_ids:
    cpt = defaultdict(int)
    for qi in pbar:
        query_id = unique_query_ids[qi]
        gdf_subset = gdf[gdf["id_query"] == query_id]
        logging.info(
            f"fetching data for query_id:{query_id} with {len(gdf_subset)} geometries"
        )
        data_subset, cpt = fetch_data_single_query(
            gdf=gdf_subset,
            timedelta_slice=timedelta_slice,
            min_sea_percent=min_sea_percent,
            top=top,
            cache_dir=cache_dir,
            querymode=querymode,
            email=email,
            password=password,
            cpt=cpt,
            headers=headers if email and password else None,
        )
        pbar.set_description("queries: %s" % cpt)
        if collected_data is None:
            collected_data = data_subset
        else:
            collected_data = pd.concat([collected_data, data_subset], ignore_index=True)
    return collected_data


def fetch_data_single_query(
    gdf,
    min_sea_percent=None,
    top=None,
    cache_dir=None,
    querymode="seq",
    timedelta_slice=None,
    email=None,
    password=None,
    cpt=None,
    headers=None,
):
    """Executes fetching logic for a GeoDataFrame subset (single query ID).

    Normalizes the GeoDataFrame, creates URLs, performs the HTTP requests,
    and post-processes the results (deduplication, geometry conversion).

    Args:
        gdf (gpd.GeoDataFrame): Normalized input data.
        min_sea_percent (float, optional): Threshold for sea coverage.
        top (int, optional): Max rows per query.
        cache_dir (str, optional): Path for local caching.
        querymode (str): 'seq' or 'multi'. Defaults to 'seq'.
        timedelta_slice (datetime.timedelta, optional): Time-based slicing window.
        email (str, optional): Auth email.
        password (str, optional): Auth password.
        cpt (collections.defaultdict, optional): Counter for tracking query status.
        headers (dict, optional): Pre-obtained authentication headers. [optional]

    Returns:
        tuple: (pd.DataFrame, dict)
            - pd.DataFrame: Results for the single query.
            - dict: Updated status counters.
    """
    if cpt is None:
        cpt = defaultdict(int)
    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        gdf_norm = normalize_gdf(
            gdf=gdf,
            timedelta_slice=timedelta_slice,
        )
        logging.debug(gdf_norm.keys())
        logging.info(f"Length of input after slicing in time:{len(gdf_norm)}")
        urls_plus_headers = create_urls(
            gdf=gdf_norm, top=top, email=email, password=password, headers=headers
        )
    else:
        urls_plus_headers = {"urls": [], "headers": None}
    if querymode == "seq":
        collected_data, cpt = fetch_data_from_urls_sequential(
            urls_plus_headers=urls_plus_headers, cache_dir=cache_dir, cpt=cpt
        )

    elif querymode == "multi":
        maxworker = 10
        logging.info("maximum // queries : %s", maxworker)
        collected_data, cpt = fetch_data_from_urls_multithread(
            urls_plus_headers=urls_plus_headers,
            cache_dir=cache_dir,
            max_workers=maxworker,
            cpt=cpt,
        )
    if collected_data is not None and collected_data.empty is False:
        data_dedup = remove_duplicates(safes_ori=collected_data)
        logging.info(
            "number of product after removing duplicates: %s", len(data_dedup["Name"])
        )
        full_data = multy_to_poly(collected_data=data_dedup)
        logging.info(
            "number of product after removing multipolygon: %s", len(full_data["Name"])
        )
        if min_sea_percent is not None:
            full_data = sea_percent(
                collected_data=full_data, min_sea_percent=min_sea_percent
            )
            logging.info(
                "number of product after adding sea percent: %s", len(full_data["Name"])
            )
    else:
        full_data = collected_data

    return full_data, cpt


def apply_slicing_time_to_gdf(gdf, timedelta_slice=None):
    """Slices a GeoDataFrame into multiple time windows.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame with 'start_datetime' and 'end_datetime'.
        timedelta_slice (datetime.timedelta, optional): The time window to slice by.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing expanded rows for each time slice.
    """
    gdf_slices = gdf

    # slice
    if timedelta_slice is not None:
        mindate = gdf["start_datetime"].min()
        maxdate = gdf["end_datetime"].max()
        # those index will need to be time expanded
        idx_to_expand = gdf.index[
            (gdf["end_datetime"] - gdf["start_datetime"]) > timedelta_slice
        ]
        # TO make sure that date does not contain future date
        if maxdate > datetime.datetime.now(datetime.UTC):
            maxdate = datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1)

        if (mindate == mindate) and (maxdate == maxdate):  # non nan
            gdf_slices = []
            slice_begin = mindate
            slice_end = slice_begin
            islice = 0
            while slice_end < maxdate:
                islice += 1
                slice_end = slice_begin + timedelta_slice
                # this is time grouping
                gdf_slice = gdf[
                    (gdf["start_datetime"] >= slice_begin)
                    & (gdf["end_datetime"] <= slice_end)
                ]
                for to_expand in idx_to_expand:
                    # missings index in gdf_slice.
                    # check if there is time overlap.
                    latest_start = max(gdf.loc[to_expand].start_datetime, slice_begin)
                    earliest_end = min(gdf.loc[to_expand].end_datetime, slice_end)
                    overlap = earliest_end - latest_start
                    if overlap >= datetime.timedelta(0):
                        gdf_slice = pd.concat(
                            [gdf_slice, gpd.GeoDataFrame(gdf.loc[to_expand]).T]
                        )
                        gdf_slice.loc[to_expand, "start_datetime"] = latest_start
                        gdf_slice.loc[to_expand, "end_datetime"] = earliest_end
                if not gdf_slice.empty:
                    gdf_slices.append(gdf_slice)
                slice_begin = slice_end
    gdf_norm = gpd.GeoDataFrame(
        pd.concat(gdf_slices, ignore_index=False), crs=gdf_slices[0].crs
    )
    return gdf_norm


def normalize_gdf(
    gdf,
    timedelta_slice=None,
):
    """Standardizes input column names and geometry for the fetching logic.

    Ensures dates are in UTC, indices are unique, and geometries are valid.
    Missing geometries are filled with the WORLDPOLYGON.

    Args:
        gdf (gpd.GeoDataFrame): Input query data.

            Mandatory columns:
                - 'start_datetime' or 'startdate': Starting date for the query.
                - 'end_datetime' or 'stopdate': Ending date for the query.
                - 'id_query': Unique identifier of the query.

            Optional columns:
                - 'name' or 'Name': SAFE name pattern.
                - 'collection': e.g., SENTINEL-1.
                - 'sensormode': e.g., IW.
                - 'producttype': e.g., GRD.
                - 'geometry': shapely geometry for area of interest.
                - 'Attributes': Extra OData filters.

        timedelta_slice (datetime.timedelta, optional): Time slicing parameter.

    Returns:
        gpd.GeoDataFrame: The normalized and optionally time-sliced GeoDataFrame.

    Raises:
        IndexError: If the input GeoDataFrame index is not unique.
        ValueError: If invalid geometries are found.
    """
    gdf_norm_sliced = None
    # add the input index as id_original_query if id_query is None
    gdf["id_original_query"] = np.where(
        gdf["id_query"].isnull(), gdf.index, gdf["id_query"]
    )

    start_time = time.time()
    default_timedelta_slice = datetime.timedelta(weeks=1)
    if "startdate" in gdf:
        gdf.rename(columns={"startdate": "start_datetime"}, inplace=True)
    if "stopdate" in gdf:
        gdf.rename(columns={"stopdate": "end_datetime"}, inplace=True)
    if "geofeature" in gdf:
        gdf.rename(columns={"geofeature": "geometry"}, inplace=True)

    if timedelta_slice is None:
        timedelta_slice = default_timedelta_slice
    if gdf is not None:
        if not gdf.index.is_unique:
            raise IndexError(
                "Index must be unique. Duplicate founds : %s"
                % list(gdf.index[gdf.index.duplicated(keep=False)].unique())
            )
        if len(gdf) == 0:
            logging.error("gdf is empty")
            return gdf_norm_sliced
        norm_gdf = gdf.copy()
        norm_gdf.set_geometry("geometry", inplace=True)
    else:
        logging.error("gdf is None")
        return gdf_norm_sliced

    # if there is no geometry, set it to world polygon
    if "geometry" not in norm_gdf:
        norm_gdf["geometry"] = WORLDPOLYGON

    # Fill NaNs/Nones with the world polygon
    norm_gdf["geometry"] = norm_gdf["geometry"].fillna(WORLDPOLYGON)

    # Ensure every entry is a valid shapely geometry object (defensive check)
    is_geom = norm_gdf["geometry"].apply(
        lambda x: isinstance(x, shapely.geometry.base.BaseGeometry)
    )
    norm_gdf.loc[~is_geom, "geometry"] = WORLDPOLYGON

    # convert naives dates to utc
    for date_col in norm_gdf.select_dtypes(include=["datetime64"]).columns:
        try:
            norm_gdf[date_col] = norm_gdf[date_col].dt.tz_localize("UTC")
        except TypeError:
            pass

    # check valid input geometry
    if not all(norm_gdf.is_valid):
        invalid_indices = norm_gdf.index[~norm_gdf.is_valid].tolist()
        logging.error(f"Invalid geometries at indices: {invalid_indices}")
        raise ValueError("Invalid geometries found. Check them with gdf.is_valid")

    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"processing time to normalize the GeoDataFrame :{processing_time}s")
    gdf_norm_sliced = apply_slicing_time_to_gdf(
        gdf=norm_gdf, timedelta_slice=timedelta_slice
    )
    return gdf_norm_sliced


def create_urls(gdf, top=None, email=None, password=None, headers=None):
    """Constructs OData query URLs based on GeoDataFrame attributes.

    Args:
        gdf (gpd.GeoDataFrame): Normalized query data.
        top (int, optional): The `$top` OData parameter. Defaults to 1000.
        email (str, optional): Account email for access token generation.
        password (str, optional): Account password.
        headers (dict, optional): Pre-obtained authentication headers. [optional]

    Returns:
        dict: A dictionary containing:
            - 'urls': List of tuples (id_original_query, url_string).
            - 'headers': Auth headers dict or None.
    """
    start_time = time.time()

    # --- 1. Handle Authentication ---
    if email and password and headers is None:
        logging.info(f"[*] Authenticating for {email}...")
        headers = get_access_token(email, password)
        logging.info("[*] Authentication successful.")
    urlapi = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
    urls = []

    if top is None:
        top = DEFAULT_TOP_ROWS_PER_QUERY

    for row in range(len(gdf)):
        gdf_row = gdf.iloc[row]
        enter_index = gdf["id_original_query"].iloc[row]

        params = {}

        # Geometry processing
        if "geometry" in gdf_row and not pd.isna(gdf_row["geometry"]):
            value = str(gdf_row.geometry)
            geo_type = gdf_row.geometry.geom_type
            coordinates_part = value[value.find("(") + 1 : value.find(")")]

            if geo_type == "Point":
                coordinates_part = coordinates_part.replace(" ", "%20")
                params["OData.CSC.Intersects"] = (
                    f"(area=geography'SRID=4326;POINT({coordinates_part})')"
                )
            elif geo_type == "Polygon":
                params["OData.CSC.Intersects"] = (
                    f"(area=geography'SRID=4326;POLYGON({coordinates_part}))')"
                )

        # OData Filter Mapping
        if "collection" in gdf_row and not pd.isna(gdf_row["collection"]):
            params["Collection/Name eq"] = f" '{gdf_row['collection']}'"

        if "name" in gdf_row and not pd.isna(gdf_row["name"]):
            params["contains"] = f"(Name,'{gdf_row['name']}')"

        if "sensormode" in gdf_row and not pd.isna(gdf_row["sensormode"]):
            params[
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq"
            ] = f" '{gdf_row['sensormode']}')"

        if "producttype" in gdf_row and not pd.isna(gdf_row["producttype"]):
            params[
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq"
            ] = f" '{gdf_row['producttype']}')"

        if "start_datetime" in gdf_row and not pd.isna(gdf_row["start_datetime"]):
            start_dt = gdf_row["start_datetime"].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start gt"] = f" {start_dt}"

        if "end_datetime" in gdf_row and not pd.isna(gdf_row["end_datetime"]):
            end_dt = gdf_row["end_datetime"].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start lt"] = f" {end_dt}"

        if "Attributes" in gdf_row and not pd.isna(gdf_row["Attributes"]):
            attr_str = str(gdf_row["Attributes"]).replace(" ", "")
            attr_name = attr_str.split(",")[0]
            attr_val = attr_str.split(",")[1]
            params["Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq"] = (
                f" '{attr_name}' and att/OData.CSC.DoubleAttribute/Value le {attr_val})"
            )

        # Construction of the final URL
        str_query = " and ".join([f"{key}{val}" for key, val in params.items()])
        url = f"{urlapi}{str_query}&$top={top}&$expand=Attributes"
        urls.append((enter_index, url))

    processing_time = time.time() - start_time
    logging.info("processing time:%1.1fs", processing_time)
    logging.debug(
        "example of generated URL: %s", urls[0][1] if urls else "No URLs generated"
    )
    return {"urls": urls, "headers": headers}


def get_cache_filename(url, cache_dir=None):
    """Generates a filename for caching a URL response based on its MD5 hash.

    Args:
        url (str): The OData URL.
        cache_dir (str, optional): The directory where cache files are stored.

    Returns:
        str: The full path to the JSON cache file.
    """
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    return os.path.join(cache_dir, url_hash + ".json")


def fetch_one_url(url, cpt, index, cache_dir, headers=None):
    """Fetches meta-data for a single OData URL.

    Handles local JSON caching if enabled and updates status counters.

    Args:
        url (str): The CDSE OData query URL.
        cpt (collections.defaultdict): Status counters.
        index (str): Original query identifier.
        cache_dir (str, optional): Path to directory for local caching.
        headers (dict, optional): Authentication headers.

    Returns:
        tuple: (defaultdict, pd.DataFrame)
            - Updated counters.
            - DataFrame containing result rows for this URL.
    """
    timeout = 10  # seconds
    json_data = None
    collected_data = None
    if cache_dir is not None:
        cache_file = get_cache_filename(url, cache_dir)
        if os.path.exists(cache_file):
            cpt["cache_used"] += 1
            logging.debug("cache file exists: %s", cache_file)
            with open(cache_file, "r") as f:
                json_data = json.load(f)
                collected_data = process_data(json_data)
    if json_data is None:
        logging.debug("no cache file -> go for query CDS")
        cpt["urls_tested"] += 1
        try:
            json_data = requests.get(url, headers=headers, timeout=timeout).json()
            cpt["urls_OK"] += 1
        except requests.exceptions.ReadTimeout:
            cpt["urls_timeout"] += 1
        except KeyboardInterrupt:
            raise
        except ValueError:
            cpt["urls_KO"] += 1
            logging.error(
                "impossible to get data from CDS for query: %s: %s",
                url,
                traceback.format_exc(),
            )
        if json_data is not None:
            if "value" in json_data:
                if cache_dir is not None:
                    cache_file = get_cache_filename(url, cache_dir)
                    with open(cache_file, "w") as f:
                        json.dump(json_data, f)
                collected_data = process_data(json_data)

    if collected_data is not None:
        if len(collected_data.index) > 0:
            cpt["product_proposed_by_CDS"] += len(collected_data["Name"])
            collected_data["id_original_query"] = index
            if len(collected_data) == DEFAULT_TOP_ROWS_PER_QUERY:
                logging.warning(
                    "%i products found in a single OData query (max is %s).",
                    len(collected_data),
                    DEFAULT_TOP_ROWS_PER_QUERY,
                )
            if pd.isna(collected_data["Name"]).any():
                raise Exception("Name field contains NaN")
            cpt["answer_append"] += 1
        else:
            cpt["nodata_answer"] += 1
    else:
        cpt["empty_answer"] += 1
    return cpt, collected_data


def fetch_data_from_urls_sequential(urls_plus_headers, cache_dir, cpt=None):
    """Fetches meta-data sequentially from a list of OData URLs.

    Args:
        urls_plus_headers (dict): Dict containing 'urls' (list) and 'headers' (dict).
        cache_dir (str, optional): Path for local caching.
        cpt (collections.defaultdict, optional): Status counters.

    Returns:
        tuple: (pd.DataFrame, defaultdict)
            - Concatenated meta-data results.
            - Updated status counters.
    """
    urls = urls_plus_headers["urls"]
    headers = urls_plus_headers["headers"]
    if cpt is None:
        cpt = defaultdict(int)
    start_time = time.time()
    collected_data_x = []
    collected_data_final = None
    if cache_dir is not None:
        if not os.path.exists(cache_dir):
            logging.info("mkdir cache dir: %s", cache_dir)
            os.makedirs(cache_dir)
    for ii in tqdm(range(len(urls)), disable=True):
        url = urls[ii][1]
        index = urls[ii][0]
        cpt, collected_data = fetch_one_url(
            url, cpt, index=index, cache_dir=cache_dir, headers=headers
        )
        if collected_data is not None:
            if not collected_data.empty:
                collected_data_x.append(collected_data)
    if len(collected_data_x) > 0:
        collected_data_final = pd.concat(collected_data_x)
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info("fetch_data_from_urls time:%1.1fsec", processing_time)
    logging.info("counter: %s", cpt)
    if collected_data_final is not None:
        assert "id_original_query" in collected_data_final
    return collected_data_final, cpt


def fetch_data_from_urls_multithread(
    urls_plus_headers, cache_dir=None, max_workers=50, cpt=None
):
    """Fetches meta-data from OData URLs using a thread pool.

    Args:
        urls_plus_headers (dict): Dict containing 'urls' (list) and 'headers' (dict).
        cache_dir (str, optional): Path for local caching.
        max_workers (int): Max number of threads. Defaults to 50.
        cpt (collections.defaultdict, optional): Status counters.

    Returns:
        tuple: (pd.DataFrame, defaultdict)
            - Concatenated results.
            - Updated status counters.
    """
    collected_data = pd.DataFrame()
    if cpt is None:
        cpt = defaultdict(int)
    urls = urls_plus_headers["urls"]
    headers = urls_plus_headers["headers"]
    with (
        ThreadPoolExecutor(max_workers=max_workers) as executor,
        tqdm(total=len(urls)) as pbar,
    ):
        future_to_url = {
            executor.submit(
                fetch_one_url, url[1], cpt, url[0], cache_dir, headers=headers
            ): (
                url[0],
                url[1],
            )
            for url in urls
        }
        for future in as_completed(future_to_url):
            cpt, df = future.result()
            if df is not None:
                if not df.empty:
                    collected_data = pd.concat([collected_data, df])
            pbar.update(1)
    logging.info("counter: %s", cpt)
    return collected_data, cpt


def process_data(json_data):
    """Converts the raw JSON response from OData into a pandas DataFrame.

    Args:
        json_data (dict): The raw JSON payload from CDSE.

    Returns:
        pd.DataFrame or None: Resulting meta-data rows or None if no data.
    """
    res = None
    if "value" in json_data:
        res = pd.DataFrame.from_dict(json_data["value"])
    else:
        logging.debug("No data found.")
    return res


def remove_duplicates(safes_ori):
    """Removes duplicate SAFE products based on their Name.

    Keeps the entry with the most recent `ModificationDate`.

    Args:
        safes_ori (pd.DataFrame): Input DataFrame containing duplicate rows.

    Returns:
        pd.DataFrame: Deduplicated DataFrame.
    """
    start_time = time.time()
    safes_sort = safes_ori.sort_values("ModificationDate", ascending=False)
    safes_dedup = safes_sort.drop_duplicates(subset=["Name"])
    end_time = time.time()
    processing_time = end_time - start_time
    nb_duplicate = len(safes_ori) - len(safes_dedup)
    logging.info("nb duplicate removed: %s", nb_duplicate)
    logging.info("remove_duplicates processing time:%1.1f sec", processing_time)
    return safes_dedup


def multy_to_poly(collected_data=None):
    """Converts WKT Footprints to Shapely geometries and unifies MultiPolygons.

    Args:
        collected_data (pd.DataFrame): Input meta-data with a 'Footprint' column.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with a clean 'geometry' column.
    """
    start_time = time.time()
    collected_data["geometry"] = (
        collected_data["Footprint"].str.split(";", expand=True)[1].str.strip().str[:-1]
    )
    collected_data["geometry"] = gpd.GeoSeries.from_wkt(collected_data["geometry"])
    collected_data = gpd.GeoDataFrame(
        collected_data, geometry="geometry", crs="EPSG:4326"
    )
    collected_data["geometry"] = collected_data["geometry"].apply(
        lambda geo: unary_union(geo.geoms) if geo.geom_type == "MultiPolygon" else geo
    )
    collected_data = gpd.GeoDataFrame(
        collected_data, geometry="geometry", crs="EPSG:4326"
    )
    collected_data.dropna(subset=["Id"], inplace=True)
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"multi_to_poly processing time:{processing_time}s")
    return collected_data


def sea_percent(collected_data, min_sea_percent=None):
    """Computes the sea coverage percentage for each product footprint.

    Filters the GeoDataFrame to keep only products meeting the minimum threshold.

    Args:
        collected_data (gpd.GeoDataFrame): Resulting meta-data.
        min_sea_percent (float, optional): Threshold (0-100). Defaults to None.

    Returns:
        gpd.GeoDataFrame: Filtered GeoDataFrame with an extra 'sea_percent' column.
    """
    start_time = time.time()
    warnings.simplefilter(action="ignore", category=FutureWarning)
    earth = gpd.read_file(get_path("naturalearth.land")).buffer(0)

    collected_data = (
        collected_data.to_crs(earth.crs)
        if collected_data.crs != earth.crs
        else collected_data
    )

    # try to fix invalid geometries
    collected_data["geometry"] = collected_data["geometry"].apply(
        lambda geom: geom.buffer(0) if not geom.is_valid else geom
    )
    sea_percentage = (
        (
            collected_data.geometry.area
            - collected_data.geometry.intersection(earth.unary_union).area
        )
        / collected_data.geometry.area
    ) * 100
    collected_data["sea_percent"] = sea_percentage
    collected_data = collected_data[collected_data["sea_percent"] >= min_sea_percent]
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"sea_percent processing time:{processing_time}s")
    return collected_data
