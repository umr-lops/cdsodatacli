import datetime
import logging
import os
import json
import hashlib
import requests
import pandas as pd
import argparse
from shapely.geometry import (
    GeometryCollection,
    Polygon,
)
from shapely import wkt
import geopandas as gpd
import shapely
import pytz
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from collections import defaultdict
import traceback
import warnings

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

    parser = argparse.ArgumentParser(description="query-CDSE-OData")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--collection",
        required=False,
        default="SENTINEL-1",
        help="SENTINEL-1 or SENTINEL-2 ...",
    )
    parser.add_argument("--startdate", required=True, help=" YYYYMMDDTHH:MM:SS")
    parser.add_argument("--stopdate", required=True, help=" YYYYMMDDTHH:MM:SS")
    parser.add_argument("--mode", choices=["EW", "IW", "WV", "SM"])
    parser.add_argument("--product", choices=["GRD", "SLC", "RAW", "OCN"])
    parser.add_argument("--querymode", choices=["seq", "multi"])
    parser.add_argument(
        "--geometry",
        required=False,
        default=None,
        help="[optional, default=None -> global query] example: POINT (-5.02 48.4) or  POLYGON ((-12 35, 15 35, 15 58, -12 58, -12 35))",
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
            "name": [None],
            "sensormode": [args.mode],
            "producttype": [args.product],
            "max_cloud_percent": [None],
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
        top=None,
        cache_dir=None,
        mode=args.querymode,
    )
    logging.info('time to query : %1.1f sec', time.time() - t0)
    return result_query


def fetch_data(
    gdf,
    date=None,
    dtime=None,
    timedelta_slice=None,
    start_datetime=None,
    end_datetime=None,
    min_sea_percent=None,
    top=None,
    cache_dir=None,
    mode="seq",
):
    """
    Fetches data based on provided parameters.

    Args: gdf (GeoDataFrame): containing the geospatial data for the query. date (str, optional): Specific date for
    the query. dtime (str, optional): Specific datetime for the query. timedelta_slice (datetime.timedelta,
    optional): Time interval to split queries and avoid missing products due to the 1000 product limit returned by
    OData. start_datetime (String): representing the starting date for the query. end_datetime (String): representing
    the ending date for the query. top (String): representing the ending publication date for the query. mode (
    String): seq ( Sequential) or multi (multithread) timedelta_slice (datetime.timedelta) : optional param to split
    the queries wrt time in order to avoid missing product because of the 1000 product max returned by Odata Return:
    (pd.DataFame): data containing the fetched results.
    """
    collected_data = None
    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        gdf_norm = normalize_gdf(
            gdf=gdf,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            date=date,
            dtime=dtime,
            timedelta_slice=timedelta_slice,
        )
        # geopd_norm.sort_index(ascending=False)
        logging.debug(gdf_norm.keys())
        logging.info(f"Length of input after slicing in time:{len(gdf_norm)}")
        urls = create_urls(gdf=gdf_norm, top=top)
    else:
        urls = []
    if mode == "seq":
        collected_data = fetch_data_from_urls_sequential(urls=urls, cache_dir=cache_dir)
    elif mode == "multi":
        maxworker = 10
        logging.info("maximum // queries : %s", maxworker)
        collected_data = fetch_data_from_urls_multithread(
            urls=urls, cache_dir=cache_dir, max_workers=maxworker
        )

    # Convert all Multipolygon to Polygon and add geometry as new column
    # if 'Footprint' in collected_data:
    if collected_data is not None:
        # Remove duplicates
        data_dedup = remove_duplicates(safes_ori=collected_data)
        logging.info(
            "number of product after removing duplicates: %s", len(data_dedup["Name"])
        )
        full_data = multy_to_poly(collected_data=data_dedup)
        full_data = full_data.sort_values(by='OriginDate')
        full_data.reset_index(drop=True, inplace=True)
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


def gdf_create(
    start_datetime=None,
    end_datetime=None,
    prod_name=None,
    collection=None,
    sensormode=None,
    producttype=None,
    geometry=None,
):
    """
    return a GeoDataFrame Created from users input
    """
    data_in = {
        "start_datetime": [None],
        "end_datetime": [None],
        "prod_name": [None],
        "collection": [None],
        "sensormode": [None],
        "producttype": [None],
        "geometry": [None],
    }

    gdf = gpd.GeoDataFrame(data_in)
    if prod_name is not None:
        gdf["prod_name"] = prod_name
    if collection is not None:
        gdf["collection"] = collection
    if sensormode is not None:
        gdf["sensormode"] = sensormode
    if producttype is not None:
        gdf["producttype"] = producttype
    if geometry is not None:
        gdf["geometry"] = shapely.wkt.loads(geometry)
    if start_datetime is not None:
        gdf["start_datetime"] = datetime.datetime.strptime(
            start_datetime, "%Y-%m-%d %H:%M:%S"
        )
    if end_datetime is not None:
        gdf["end_datetime"] = datetime.datetime.strptime(
            end_datetime, "%Y-%m-%d %H:%M:%S"
        )
    return gdf


def normalize_gdf(
    gdf,
    start_datetime=None,
    end_datetime=None,
    date=None,
    dtime=None,
    timedelta_slice=None,
):
    """return a normalized gdf list
    start/stop date name will be 'start_datetime' and 'end_datetime'
    """
    # add the index of each rows of input gdf
    if ("Name" in gdf
        and not gdf["Name"].empty
        and gdf["Name"] is not None
    ):
        gdf["id_original_query"] = gdf.Name
    else:
        gdf["id_original_query"] = gdf.index
    start_time = time.time()
    # default_cacherefreshrecent = datetime.timedelta(days=7)
    default_timedelta_slice = datetime.timedelta(weeks=1)
    if "name" in gdf:
        gdf.rename(columns={"name": "prod_name"}, inplace=True)
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
        norm_gdf = gpd.GeoDataFrame(
            {
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "geometry": Polygon(),
            },
            geometry="geometry",
            index=[0],
            crs="EPSG:4326",
        )
        # no slicing
        timedelta_slice = None
    worlpolygon = shapely.wkt.loads(
        "POLYGON((-180 -90,180 -90,180 90,-180 90,-180 -90))"
    )
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

    if date in norm_gdf:
        if (start_datetime not in norm_gdf) and (end_datetime not in norm_gdf):
            norm_gdf["start_datetime"] = norm_gdf[date] - dtime
            norm_gdf["end_datetime"] = norm_gdf[date] + dtime
        else:
            raise ValueError("date keyword conflict with startdate/stopdate")

    if (start_datetime in norm_gdf) and (start_datetime != "start_datetime"):
        norm_gdf["start_datetime"] = norm_gdf[start_datetime]

    if (end_datetime in norm_gdf) and (end_datetime != "end_datetime"):
        norm_gdf["end_datetime"] = norm_gdf[end_datetime]

    gdf_slices = norm_gdf

    # slice
    if timedelta_slice is not None:
        mindate = norm_gdf["start_datetime"].min()
        maxdate = norm_gdf["end_datetime"].max()
        # those index will need to be time expanded
        idx_to_expand = norm_gdf.index[
            (norm_gdf["end_datetime"] - norm_gdf["start_datetime"]) > timedelta_slice
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
                gdf_slice = norm_gdf[
                    (norm_gdf["start_datetime"] >= slice_begin)
                    & (norm_gdf["end_datetime"] <= slice_end)
                    ]
                # check if some slices needs to be expanded
                # index of gdf_slice that where not grouped
                # not_grouped_index = pd.Index(set(idx_to_expand) - set(gdf_slice.index))
                for to_expand in idx_to_expand:
                    # missings index in gdf_slice.
                    # check if there is time overlap.
                    latest_start = max(
                        norm_gdf.loc[to_expand].start_datetime, slice_begin
                    )
                    earliest_end = min(norm_gdf.loc[to_expand].end_datetime, slice_end)
                    overlap = earliest_end - latest_start
                    if overlap >= datetime.timedelta(0):
                        new_slice = gpd.GeoDataFrame(gpd.GeoDataFrame(norm_gdf.loc[to_expand]).T, geometry='geometry')
                        new_slice.crs = gdf_slice.crs
                        gdf_slice = pd.concat(
                            [gdf_slice, new_slice]
                        )
                        gdf_slice.loc[to_expand, "start_datetime"] = latest_start
                        gdf_slice.loc[to_expand, "end_datetime"] = earliest_end
                if not gdf_slice.empty:
                    gdf_slices.append(gdf_slice)
                slice_begin = slice_end
    gdf_norm = gpd.GeoDataFrame(
        pd.concat(gdf_slices, ignore_index=True), crs=gdf_slices[0].crs
    )
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"normalize_gdf processing time:{processing_time}s")
    return gdf_norm


def create_urls(gdf, top=None):
    """
    return all url created from input
    """
    start_time = time.time()
    urlapi = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
    urls = []
    if top is None:
        top = DEFAULT_TOP_ROWS_PER_QUERY
    for row in range(len(gdf)):
        gdf_row = gdf.iloc[row]
        # enter_index = gdf.index[row]
        enter_index = gdf["id_original_query"].iloc[row]

        # Taking all given parameters
        params = {}
        if (
            "geometry" in gdf_row
            and not pd.isna(gdf_row["geometry"])
            and gdf_row["geometry"] is not None
        ):
            value = str(gdf_row.geometry)
            geo_type = gdf_row.geometry.geom_type
            coordinates_part = value[value.find("(") + 1: value.find(")")]
            if geo_type == "Point":
                modified_value = f"{coordinates_part}"
                coordinates_part = modified_value.replace(" ", "%20")
                params[
                    "OData.CSC.Intersects"
                ] = f"(area=geography'SRID=4326;POINT({coordinates_part})')"
            elif geo_type == "Polygon":
                params[
                    "OData.CSC.Intersects"
                ] = f"(area=geography'SRID=4326;POLYGON({coordinates_part}))')"

        if "collection" in gdf_row and not pd.isna(gdf_row["collection"]):
            collection = gdf_row["collection"]
            params["Collection/Name eq"] = f" '{collection}'"

        if "prod_name" in gdf_row and not pd.isna(gdf_row["prod_name"]):
            prod_name = gdf_row["prod_name"]
            params["contains"] = f"(Name,'{prod_name}')"

        if "sensormode" in gdf_row and not pd.isna(gdf_row["sensormode"]):
            sensormode = gdf_row["sensormode"]
            params[
                ("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq")
            ] = f" '{sensormode}')"

        if "producttype" in gdf_row and not pd.isna(gdf_row["producttype"]):
            producttype = gdf_row["producttype"]
            params[
                ("Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq")
            ] = f" '{producttype}')"

        if "start_datetime" in gdf_row and not pd.isna(gdf_row["start_datetime"]):
            start_datetime = gdf_row["start_datetime"].strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            params["ContentDate/Start gt"] = f" {start_datetime}"

        if "end_datetime" in gdf_row and not pd.isna(gdf_row["end_datetime"]):
            end_datetime = gdf_row["end_datetime"].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start lt"] = f" {end_datetime}"

        if "max_cloud_percent" in gdf_row and not pd.isna(gdf_row["max_cloud_percent"]):
            max_cloud_percent = float(gdf_row["max_cloud_percent"])
            max_cloud_percent = "{:.2f}".format(max_cloud_percent)
            params[
                ("Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt")
            ] = f" {max_cloud_percent})"

        str_query = " and ".join([f"{key}{value}" for key, value in params.items()])

        str_query = str_query + "&$top=" + str(top)
        url = urlapi + str_query + "&$expand=Attributes"
        urls.append((enter_index, url))
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"create_urls() processing time:%1.1fs", processing_time)
    logging.debug("example of URL created: %s", urls[0])
    return urls


def get_cache_filename(url, cache_dir=None):
    """
    Generates a cache filename based on the provided URL and cache directory.

    Args:
        url (str): The URL for which to generate the cache filename.
        cache_dir (str, optional): Directory path for caching. If not provided, the cache filename will be generated without a specific directory.

    Returns:
        str: The cache filename, incorporating the hash of the URL and the '.json' extension.
    """
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    return os.path.join(cache_dir, url_hash + ".json")


def fetch_one_url(url, cpt, index, cache_dir):
    """

    Parameters
    ----------
    url (str)
    cpt (defaultdict(int))
    index (int)
    cache_dir (str)

    Returns
    -------
    cpt (defaultdict(int))
    collected_data (pandas.GeoDataframe)

    """
    json_data = None
    collected_data = None
    url_ko = None
    if cache_dir is not None:
        cache_file = get_cache_filename(url, cache_dir)
        if os.path.exists(cache_file):
            cpt["cache_used"] += 1
            logging.debug("cache file exists: %s", cache_file)
            with open(cache_file, "r") as f:
                json_data = json.load(f)
                collected_data = process_data(json_data)
                collected_data["id_original_query"] = index
    if (
        json_data is None
    ):  # means that cache cannot be used (or user used cache_dir=None or there is no associated json file
        logging.debug("no cache file -> go for query CDS")
        cpt["urls_tested"] += 1
        try:
            json_data = requests.get(url).json()
            cpt["urls_OK"] += 1
        except KeyboardInterrupt:
            raise "keyboard interrupt"
        except:
            cpt["urls_KO"] += 1
            url_ko = (index, url)
            logging.error(
                "impossible to get data from CDS for query: %s: %s",
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
                                "%i products found in a single CDSE OData query (maximum is %s): make sure the "
                                "timedelta_slice parameters is small enough to avoid truncated results",
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
    return cpt, collected_data, url_ko


def fetch_data_from_urls_sequential(urls, cache_dir) -> pd.DataFrame:
    """

    Parameters
    ----------
    urls (list): list of url to query CDS

    Returns
    -------

    """

    cpt = defaultdict(int)
    start_time = time.time()
    collected_data_x = []
    collected_data_final = None
    all_url_ko = []
    if cache_dir is not None:
        if not os.path.exists(cache_dir):
            logging.info("mkdir cache dir: %s", cache_dir)
            os.makedirs(cache_dir)
    # with tqdm(total=len(urls)) as pbar:
    for ii in tqdm(range(len(urls))):
        # for url in urls:
        index = urls[ii][0]
        url = urls[ii][1]
        cpt, collected_data, url_ko = fetch_one_url(url, cpt, index, cache_dir=cache_dir)
        if url_ko is not None:
            all_url_ko.append(url_ko)
        if collected_data is not None:
            if not collected_data.empty:
                collected_data_x.append(collected_data)
    # processing all failed urls
    if len(all_url_ko) > 0:
        logging.info(f"retry no result urls:%", len(all_url_ko))
        for ik in tqdm(range(len(all_url_ko))):
            index = all_url_ko[ik][0]
            url = all_url_ko[ik][1]
            cpt, collected_data, url_ko = fetch_one_url(url, cpt, index, cache_dir=cache_dir)
            all_url_ko.append(url_ko)
            if collected_data is not None:
                if not collected_data.empty:
                    collected_data_x.append(collected_data)
    if len(collected_data_x) > 0:
        collected_data_final = pd.concat(collected_data_x, ignore_index=True)
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"fetch_data_from_urls time:%1.1fsec", processing_time)
    logging.info("counter: %s", cpt)
    return collected_data_final


def fetch_data_from_urls_multithread(urls, cache_dir=None, max_workers=50):
    """

    Parameters
    ----------
    urls (list)
    cache_dir (str)
    max_workers (int)

    Returns
    -------

    """
    collected_data = pd.DataFrame()
    cpt = defaultdict(int)
    collected_data_x = []
    all_url_ko = []
    if cache_dir is not None:
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(
        total=len(urls)
    ) as pbar:
        # url[1] is a CDS Odata query URL
        # url[0] is index of original gdf
        future_to_url = {
            executor.submit(fetch_one_url, url[1], cpt, url[0], cache_dir): (
                url[1],
                url[0],
            )
            for url in urls
        }
        for future in as_completed(future_to_url):
            cpt, df, url_ko = future.result()
            if url_ko is not None:
                all_url_ko.append(url_ko)
            if df is not None:
                if not df.empty:
                    collected_data_x.append(df)
            pbar.update(1)
    # processing all failed urls
    if len(all_url_ko) > 0:
        logging.info(f"retry no result urls:%", len(all_url_ko))
        for ik in tqdm(range(len(all_url_ko))):
            index = all_url_ko[ik][0]
            url = all_url_ko[ik][1]
            cpt, collected_data, url_ko = fetch_one_url(url, cpt, index, cache_dir=cache_dir)
            all_url_ko.append(url_ko)
            if collected_data is not None:
                if not collected_data.empty:
                    collected_data_x.append(collected_data)
    if len(collected_data_x) > 0:
        collected_data_final = pd.concat(collected_data_x, ignore_index=True)
    logging.info("counter: %s", cpt)
    # logging.info("no result urls: %s", all_url_ko)
    return collected_data_final


def process_data(json_data):
    """
    Processes the fetched JSON data and returns relevant information.

    :param json_data: JSON data containing the fetched results.
    :return: Processed data for visualization.
    """
    res = None
    if "value" in json_data:
        res = pd.DataFrame.from_dict(json_data["value"])
    else:
        print("No data found.")
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
    logging.info(f"remove_duplicates processing time:%1.1f sec", processing_time)
    return safes_dedup


def multy_to_poly(collected_data=None):
    """
    Converts MultiPolygon geometries in the provided GeoDataFrame to Polygons.

    Args:
        collected_data (GeoDataFrame): GeoSpatial DataFrame containing MultiPolygon geometries in the 'Footprint' column.

    Returns:
        GeoDataFrame: Updated GeoDataFrame with 'geometry' column containing Polygons instead of MultiPolygons.
    """
    start_time = time.time()
    collected_data["geometry"] = (
        collected_data["Footprint"].str.split(";", expand=True)[1].str.strip().str[:-1]
    )
    collected_data["geometry"] = gpd.GeoSeries.from_wkt(collected_data["geometry"])
    collected_data = gpd.GeoDataFrame(
        collected_data, geometry="geometry", crs="EPSG:4326"
    )
    collected_data["geometry"] = collected_data["geometry"].buffer(0)
    collected_data = gpd.GeoDataFrame(
        collected_data, geometry="geometry", crs="EPSG:4326"
    )
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"multi_to_poly processing time:{processing_time}s")
    return collected_data


def sea_percent(collected_data, min_sea_percent=None):
    """

    Parameters
    ----------
    collected_data pandas.DataFrame
    min_sea_percent float [optional]

    Returns
    -------

    """
    start_time = time.time()
    warnings.simplefilter(action="ignore", category=FutureWarning)
    earth = GeometryCollection(
        list(gpd.read_file(gpd.datasets.get_path("naturalearth_lowres")).geometry)
    ).buffer(0)
    collected_data.to_crs(crs="EPSG:4326")
    sea_percentage = (
        (
            collected_data.geometry.area
            - collected_data.geometry.intersection(earth).area
        )
        / collected_data.geometry.area
        * 100
    )
    collected_data["sea_percent"] = sea_percentage
    collected_data = collected_data[collected_data["sea_percent"] >= min_sea_percent]
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"sea_percent processing time:{processing_time}s")
    return collected_data
