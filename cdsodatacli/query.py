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
import pytz
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import defaultdict
import traceback
import warnings
from geodatasets import get_path
import numpy as np
import urllib3
import sys
from cdsodatacli.fetch_access_token import get_access_token


DEFAULT_TOP_ROWS_PER_QUERY = 1000


def query_client():
    """

    Returns
    -------
        result_query (pd.DataFrame): containing columns (footprint, Name, Id, original_query_id, ...)
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
        help="SENTINEL-1 or SENTINEL-2 ...",
    )
    parser.add_argument("--startdate", required=True, help=" YYYYMMDDTHH:MM:SS")
    parser.add_argument(
        "--stopdate",
        required=False,
        help=" YYYYMMDDTHH:MM:SS [optional, default=now]",
        default=datetime.datetime.utcnow().strftime("%Y%m%dT%H:%M:%S"),
    )
    parser.add_argument("--mode", choices=["EW", "IW", "WV", "SM"])
    parser.add_argument(
        "--product",
        help="product type, could be GRD, SLC, RAW or  OCN or more specifically IW_GRDH_1S_PRIVATE",
    )
    parser.add_argument("--querymode", choices=["seq", "multi"])
    parser.add_argument(
        "--geometry",
        required=False,
        default=None,
        help=" [optional, default=None -> global query] example: POINT (-5.02 48.4) or  POLYGON ((-12 35, 15 35, 15 58, -12 58, -12 35))",
    )
    parser.add_argument("--id_query", required=False, default=None)
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
        help="output txt file containing the SAFE listing resulting from the query",
    )
    parser.add_argument(
        "--safename-pattern",
        help="pattern to filter SAFE names (e.g. S1A_IW_GRDH_1SDH_ or S1A or T160214) [optinal, default: None -> no filtering]",
        default=None,
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
            "id_query": [args.id_query],
        }
    )
    result_query = fetch_data(
        gdf,
        date=None,
        dtime=None,
        timedelta_slice=datetime.timedelta(days=14),
        start_datetime=None,
        end_datetime=None,
        min_sea_percent=None,
        fig=None,
        top=args.top,
        cache_dir=None,
        mode=args.querymode,
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
            result_query["Name"].to_csv(
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
):
    """
    Fetches meta-data of CDSE products based on provided parameters.
    GeoDataFrame is splitted based on the id_query column to keep track of each pair query/products

    Args:
        gdf (GeoDataFrame): containing the geospatial data for the query.
            list of the mandatory columns:
                - 'start_datetime' : datetime representing the starting date for the query.
                - 'end_datetime' : datetime representing the ending date for the query.
                - 'id_query' : unique identifier of the query (to split the gdf in subsets)
            list of the optional columns:
                - 'name' : pattern of the SAFE name to filter the query
                - 'collection' : e.g. SENTINEL-1, SENTINEL-2, ...
                - 'sensormode' : e.g. EW, IW, WV, ...
                - 'producttype' : e.g. GRD, SLC, RAW, OCN, ...
                - 'geometry' : shapely geometry (Polygon, MultiPolygon, Point, ..) representing the area of interest for the query.
                - 'Attributes' : additional attributes to filter the query
        timedelta_slice (datetime.timedelta) : optional param to split the queries wrt time in order to avoid missing product because of the 1000 product max returned by Odata
        min_sea_percent (Float): minimum sea percent within product footprint to filter out the data that are below the thresold [None-> no filter based on sea percent].
        top (String): representing the ending publication date for the query.
        cache_dir (String): path to cache directory to store/re-use intermediate query results
        querymode (String): how the queries are send/received to Odata, possibles choices: 'seq' (Sequential) or 'multi' (multithread)
        email (str): CDSE account [optional to be used for PRIVATE data that need authentication]
        password (str): password CDSE account [optional to be used for PRIVATE data that need authentication]
    Return:
        (pd.DataFame): data containing the fetched results.
    """
    collected_data = None
    # split the gdf in subsets based on the query_id
    unique_query_ids = gdf["id_query"].unique()
    for query_id in unique_query_ids:
        gdf_subset = gdf[gdf["id_query"] == query_id]
        logging.info(
            f"fetching data for query_id:{query_id} with {len(gdf_subset)} geometries"
        )
        data_subset = fetch_data_single_query(
            gdf=gdf_subset,
            timedelta_slice=timedelta_slice,
            min_sea_percent=min_sea_percent,
            top=top,
            cache_dir=cache_dir,
            querymode=querymode,
            email=email,
            password=password,
        )
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
):
    """
    Fetches data based on provided parameters.

    Args:
       gdf (GeoDataFrame): containing the geospatial data for the query, it must contain 'start_datetime', 'end_datetime' 'id_query' columns.
       min_sea_percent (Float): minimum sea percent to filter the data [None-> no filter based on sea percent].
       top (String): representing the ending publication date for the query.
       cache_dir (String): path to cache directory to store intermediate results
       querymode (String): how the queries are send/received to Odata, possibles choices: 'seq' (Sequential) or 'multi' (multithread)
       timedelta_slice (datetime.timedelta) : optional param to split the queries wrt time in order to avoid missing product because of the 1000 product max returned by Odata
       email (str): CDSE account [optional to be used for PRIVATE data that need authentication]
       password (str): password CDSE account [optional to be used for PRIVATE data that need authentication]
    Return:
        (pd.DataFame): data containing the fetched results.
    """
    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        gdf_norm = normalize_gdf(
            gdf=gdf,
            # start_datetime=start_datetime,
            # end_datetime=end_datetime,
            # date=date,
            # dtime=dtime,
            timedelta_slice=timedelta_slice,
        )
        # geopd_norm.sort_index(ascending=False)
        logging.debug(gdf_norm.keys())
        logging.info(f"Length of input after slicing in time:{len(gdf_norm)}")
        urls_plus_headers = create_urls(
            gdf=gdf_norm, top=top, email=email, password=password
        )
    else:
        urls_plus_headers = {"urls": [], "headers": None}
    if querymode == "seq":
        collected_data = fetch_data_from_urls_sequential(
            urls_plus_headers=urls_plus_headers, cache_dir=cache_dir
        )
    elif querymode == "multi":
        maxworker = 10
        logging.info("maximum // queries : %s", maxworker)
        collected_data = fetch_data_from_urls_multithread(
            urls_plus_headers=urls_plus_headers,
            cache_dir=cache_dir,
            max_workers=maxworker,
        )

    # Convert all Multipolygon to Polygon and add geometry as new column
    # if 'Footprint' in collected_data:
    if collected_data is not None and collected_data.empty is False:
        # Remove duplicates
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

    return full_data


def apply_slicing_time_to_gdf(gdf, timedelta_slice=None):
    """
    step apply normalization of the gdf to slice it in time based on the timedelta_slice param
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
        if maxdate > datetime.datetime.utcnow().replace(tzinfo=pytz.UTC):
            maxdate = datetime.datetime.utcnow().replace(
                tzinfo=pytz.UTC
            ) + datetime.timedelta(days=1)

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
                # check if some slices needs to be expanded
                # index of gdf_slice that where not grouped
                # not_grouped_index = pd.Index(set(idx_to_expand) - set(gdf_slice.index))
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
    """
    return a normalized gdf list
    start/stop date name will be 'start_datetime' and 'end_datetime'

    Args:
        gdf (GeoDataFrame): containing the geospatial data for the query.
            list of the mandatory columns:
                - 'start_datetime' or 'startdate' : datetime representing the starting date for the query.
                - 'end_datetime' or 'stopdate' : datetime representing the ending date for the query.
                - 'id_query' : unique identifier of the query (to split the gdf in subsets)
            list of the optional columns:
                - 'name' or 'Name': pattern of the SAFE name to filter the query
                - 'collection' : e.g. SENTINEL-1, SENTINEL-2, ...
                - 'sensormode' : e.g. EW, IW, WV, ...
                - 'producttype' : e.g. GRD, SLC, RAW, OCN, ...
                - 'geometry' : shapely geometry (Polygon, MultiPolygon, Point, ..) representing the area of interest for the query.
                - 'Attributes' : additional attributes to filter the query
    Return:
        (GeoDataFrame): normalized gdf and sliced in time if timedelta_slice is not None
    """
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
            return []
        norm_gdf = gdf.copy()
        norm_gdf.set_geometry("geometry", inplace=True)
    else:
        logging.error("gdf is None")
        return None
        # norm_gdf = gpd.GeoDataFrame(
        #     {
        #         "start_datetime": start_datetime,
        #         "end_datetime": end_datetime,
        #         "geometry": Polygon(),
        #     },
        #     geometry="geometry",
        #     index=[0],
        #     crs="EPSG:4326",
        # )
        # # no slicing
        # timedelta_slice = None
    worlpolygon = shapely.wkt.loads(
        "POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))"
    )
    # if there is no geometry, set it to world polygon
    norm_gdf["geometry"].fillna(
        value=worlpolygon, inplace=True
    )  # to replace None by NaN
    # convert naives dates to utc
    for date_col in norm_gdf.select_dtypes(include=["datetime64"]).columns:
        try:
            norm_gdf[date_col] = norm_gdf[date_col].dt.tz_localize("UTC")
            logging.debug("norm_gdf[date_col] %s", type(norm_gdf[date_col].iloc[0]))
            # logger.warning("Assuming UTC date on col %s" % date_col)
        except TypeError:
            # already localized
            pass

    # check valid input geometry
    if not all(norm_gdf.is_valid):
        raise ValueError("Invalid geometries found. Check them with gdf.is_valid")

    # if date in norm_gdf:
    #     if (start_datetime not in norm_gdf) and (end_datetime not in norm_gdf):
    #         norm_gdf["start_datetime"] = norm_gdf[date] - dtime
    #         norm_gdf["end_datetime"] = norm_gdf[date] + dtime
    #     else:
    #         raise ValueError("date keyword conflict with startdate/stopdate")

    # if (start_datetime in norm_gdf) and (start_datetime != "start_datetime"):
    #     norm_gdf["start_datetime"] = norm_gdf[start_datetime]

    # if (end_datetime in norm_gdf) and (end_datetime != "end_datetime"):
    #     norm_gdf["end_datetime"] = norm_gdf[end_datetime]

    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"normalize_gdf processing time:{processing_time}s")
    gdf_norm_sliced = apply_slicing_time_to_gdf(
        gdf=norm_gdf, timedelta_slice=timedelta_slice
    )
    return gdf_norm_sliced


def create_urls(gdf, top=None, email=None, password=None):
    """
    Method to create the list of URLs and authentication headers.

    Parameters
    ----------
        gdf : GeoDataFrame containing the query parameters
        top : int number of max rows per query
        email : str  CDSE account [optional]
        password : str password CDSE account [optional]

    Returns
    -------
        dict : containing
            'urls' : list of tuples (id_original_query, url)
            'headers' : authentication headers (None if no email/password)

    """
    start_time = time.time()

    # --- 1. Handle Authentication ---
    headers = None
    if email and password:
        logging.info(f"[*] Authenticating for {email}...")
        headers = get_access_token(email, password)
        logging.info("[*] Authentication successful.")

    urlapi = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
    urls = []

    if top is None:
        # Assuming DEFAULT_TOP_ROWS_PER_QUERY is defined globally
        top = DEFAULT_TOP_ROWS_PER_QUERY

    for row in range(len(gdf)):
        gdf_row = gdf.iloc[row]
        enter_index = gdf["id_original_query"].iloc[row]

        params = {}

        # Geometry processing
        if "geometry" in gdf_row and not pd.isna(gdf_row["geometry"]):
            value = str(gdf_row.geometry)
            geo_type = gdf_row.geometry.geom_type
            # Extracting coordinates inside parentheses
            # coordinates_part = value[value.find("(") + 1 : value.rfind(")")]
            coordinates_part = value[value.find("(") + 1 : value.find(")")]

            if geo_type == "Point":
                # Clean spaces for URL encoding
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


def get_cache_filename(url, cache_dir=None) -> str:
    """

    Parameters
    ----------
    url (str)
    cache_dir (str) : directory to store cache files [optional, default=None -> no cache]

    Returns
    -------
    cache_file (str)

    """
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    return os.path.join(cache_dir, url_hash + ".json")


def fetch_one_url(url, cpt, index, cache_dir, headers=None):
    """

    Parameters
    ----------
    url (str): CDS OData query URL
    cpt (defaultdict(int)): counters
    index (str): id_query of the original gdf
    cache_dir (str): directory to store cache files [optional, default=None -> no cache]
    headers (dict): authentication headers (None if no email/password) [optional, default=None]

    Returns
    -------
    cpt (defaultdict(int))
    collected_data (pandas.GeoDataframe)

    """
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
    if (
        json_data is None
    ):  # means that cache cannot be used (or user used cache_dir=None or there is no associated json file
        logging.debug("no cache file -> go for query CDS")
        cpt["urls_tested"] += 1
        try:
            json_data = requests.get(url, headers=headers).json()
            cpt["urls_OK"] += 1
        except KeyboardInterrupt:
            raise ("keyboard interrupt")
        except ValueError:
            cpt["urls_KO"] += 1
            logging.error(
                "impossible to get data from CDSfor query: %s: %s",
                url,
                traceback.format_exc(),
            )
        if json_data is not None:
            if "value" in json_data:

                if cache_dir is not None:  # write a cache file
                    cache_file = get_cache_filename(url, cache_dir)
                    with open(cache_file, "w") as f:
                        json.dump(json_data, f)
                collected_data = process_data(json_data)
                # collected_data = pd.DataFrame.from_dict(json_data['value'])
    if collected_data is not None:
        if len(collected_data.index) > 0:
            # collected_data_x.append(collected_data)
            cpt["product_proposed_by_CDS"] += len(collected_data["Name"])
            collected_data["id_original_query"] = index
            if len(collected_data) == DEFAULT_TOP_ROWS_PER_QUERY:
                logging.warning(
                    "%i products found in a single CDSE OData query (maximum is %s): make sure the timedelta_slice parameters is small enough to avoid truncated results",
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


def fetch_data_from_urls_sequential(urls_plus_headers, cache_dir) -> pd.DataFrame:
    """

    Parameters
    ----------
    urls_plus_headers (dict): containing
        'urls' : list of tuples (id_original_query, url)
        'headers' : authentication headers (None if no email/password)
    cache_dir (str)

    Returns
    -------

    """
    urls = urls_plus_headers["urls"]
    headers = urls_plus_headers["headers"]
    cpt = defaultdict(int)
    start_time = time.time()
    collected_data_x = []
    collected_data_final = None
    if cache_dir is not None:
        if not os.path.exists(cache_dir):
            logging.info("mkdir cache dir: %s", cache_dir)
            os.makedirs(cache_dir)
    # with tqdm(total=len(urls)) as pbar:
    for ii in tqdm(range(len(urls)), disable=True):
        # for url in urls:
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
        assert (
            "id_original_query" in collected_data_final
        ), "id_original_query column missing in collected data"
    return collected_data_final


def fetch_data_from_urls_multithread(urls_plus_headers, cache_dir=None, max_workers=50):
    """

    Parameters
    ----------
    urls_plus_headers (dict): containing
        'urls' : list of tuples (id_original_query, url)
        'headers' : authentication headers (None if no email/password)
    cache_dir (str): directory to store cache files [optional, default=None -> no cache]
    max_workers (int): maximum number of parallel threads [optional, default=50]

    Returns
    -------
    collected_data (pandas.GeoDataframe): containing the fetched results.
    """
    collected_data = pd.DataFrame()
    cpt = defaultdict(int)
    urls = urls_plus_headers["urls"]
    headers = urls_plus_headers["headers"]
    with (
        ThreadPoolExecutor(max_workers=max_workers) as executor,
        tqdm(total=len(urls)) as pbar,
    ):
        # url[1] is a CDS Odata query URL
        # url[0] is index of original gdf
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
    return collected_data


# def fetch_url(url):
#    data = requests.get(url).json()
#    return process_data(data)


# def fetch_data_from_urls(urls):
#    with ThreadPoolExecutor(max_workers=10) as executor:
#        data = list(tqdm(executor.map(fetch_url, urls), total=len(urls)))

#    df = pd.concat(data, ignore_index=True)
#    return df


def process_data(json_data):
    """
    Processes the fetched JSON data and returns relevant information.

    :param json_data: JSON data containing the fetched results.
    :return: Processed data for visualization.
    """
    res = None
    if "value" in json_data:
        res = pd.DataFrame.from_dict(json_data["value"])
        # get the code status of the query "@odata.count"
        # if len(res) > 0:
        #     logging.debug("example of data fetched: %s", res.iloc[0].to_dict())
    else:
        logging.debug("No data found.")
        pass
    return res


def remove_duplicates(safes_ori):
    """
    Remove duplicate safe (ie same footprint with same date, but different prodid)
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
    """

    method to compute the sea percentage of each product footprint and filter the products based on a minimum sea percentage threshold.

    Parameters
    ----------
    collected_data pandas.DataFrame: containing a 'geometry' column with product footprints
    min_sea_percent float : minimum threshold of sea percent per footprint to keep image [optional, default=None -> no filtering]

    Returns
    -------
    collected_data pandas.DataFrame: filtered based on min_sea_percent

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


def core_query_logged(
    email=None,
    password=None,
    type=None,
    startdate=None,
    enddate=None,
    unit=None,
    output=None,
    limit=1000,
):
    """
    Core function to query CDSE OData with authentication and keyed arguments.
    this method in complementary to cdsodatacli.query.fetch_data() because here we use authentication
    and we have keyed arguments instead of a gdf input.
    It is used in the context of private data access where authentication is required during IOC periods.
    Current limitations:
         - no spatial filtering

    Args:
        email (str): CDSE account email.
        password (str): CDSE account password.
        type (str): Product type (e.g. WV_SLC__1S_PRIVATE).
        startdate (str): Start date (e.g. 2025-01-01T00:00:00).
        enddate (str): End date (e.g. 2025-01-31T23:59:59).
        unit (str): Satellite Unit Identifier (C or D).
        output (str): Output JSON file path.
        limit (int): Max records to return.
    Returns:
        None


    """

    # --- 1. Authentication ---
    auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    auth_data = {
        "client_id": "cdse-public",
        "username": email,
        "password": password,
        "grant_type": "password",
    }

    logging.info(f"[*] Authenticating for {email}...")
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.post(auth_url, data=auth_data, verify=False)
        response.raise_for_status()
        access_token = response.json().get("access_token")
    except Exception as e:
        logging.error(f"[-] Auth Error: {e}")
        if "response" in locals():
            logging.error(f"Details: {response.text}")
        sys.exit(1)

    # --- 2. OData Query Construction ---
    odata_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

    # Using the specific StringAttribute syntax required by CDSE OData
    if enddate is None:
        add_filter_startdate = [
            f"ContentDate/End ge {startdate}",
        ]
        add_filter_enddate = []
    else:
        add_filter_enddate = [f"ContentDate/End le {enddate}"]
        add_filter_startdate = [
            f"ContentDate/Start ge {startdate}",
        ]
    filters = [
        "Collection/Name eq 'SENTINEL-1'",
        # f"ContentDate/Start ge {startdate}",
        # f"ContentDate/End le {enddate}",
        f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{type}')",
        f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'platformSerialIdentifier' and att/OData.CSC.StringAttribute/Value eq '{unit}')",
    ]
    filters += add_filter_enddate
    filters += add_filter_startdate

    odata_filter = " and ".join(filters)

    params = {
        "$filter": odata_filter,
        "$orderby": "ContentDate/Start asc",
        "$top": limit,
        "$count": "true",
    }

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    logging.info("[*] Querying CDSE OData...")
    logging.debug(f"URL: {odata_url}")
    logging.debug(f"Params: {params}")
    # logging.debug(f"Headers: {headers}")
    try:
        # Requests automatically handles URL encoding of spaces, quotes, and symbols
        search_res = requests.get(
            odata_url, params=params, headers=headers, verify=False
        )
        search_res.raise_for_status()
        data = search_res.json()

        # --- 3. Save Results ---
        with open(output, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        count = len(data.get("value", []))
        logging.info(f"[+] Success! {count} products found.")
        logging.info(f"[+] Results saved to: {output}")

    except Exception as e:
        logging.error(f"[-] Search Error: {e}")
        if "search_res" in locals():
            logging.error(f"Response: {search_res.text}")
        sys.exit(1)

    logging.info("finish")
