import datetime
import logging
import os
import json
import hashlib
import requests
import pandas as pd
import argparse
import pdb
from shapely.geometry import (
    GeometryCollection,
    LineString,
    Point,
    Polygon,
    MultiPolygon,
)
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


def fetch_data(
    gdf,
    date=None,
    dtime=None,
    timedelta_slice=None,
    start_datetime=None,
    end_datetime=None,
    min_sea_percent=None,
    fig=None,
    top=None,
    cache_dir=None,
    mode="seq",
):
    """
    Fetches data based on provided parameters.

    Args:
       gdf (GeoDataFrame): containing the geospatial data for the query.
       geometry (list of tuples): representing the geometry.
       collection (String): representing the collection information for filtering the data.
       name (String): representing the name information for filtering the data.
       sensormode (String): representing the mode of the sensor for filtering the data.
       producttype (String): representing the type of product for filtering the data.
       start_datetime (String): representing the starting date for the query.
       end_datetime (String): representing the ending date for the query.
       publication_start (String): representing the starting publication date for the query.
       publication_end (String): representing the ending publication date for the query.
       top (String): representing the ending publication date for the query.
       mode (String): seq ( Sequential) or multi (multithread)
       timedelta_slice (datetime.timedelta) : optional param to split the queries wrt time in order to avoid missing product because of the 1000 product max returned by Odata
    Return:
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
        logging.info(
            "number of product after removing multipolygon: %s", len(full_data["Name"])
        )
        if fig is not None:
            fig(collected_data=full_data)
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
    name=None,
    collection=None,
    sensormode=None,
    producttype=None,
    geometry=None,
    publication_start=None,
    publication_end=None,
):
    data_in = {
        "start_datetime": [None],
        "end_datetime": [None],
        "name": [None],
        "collection": [None],
        "sensormode": [None],
        "producttype": [None],
        "geometry": [None],
        "publication_start": [None],
        "publication_end": [None],
    }

    gdf = gpd.GeoDataFrame(data_in)

    if geometry is not None:
        gdf["geometry"] = shapely.wkt.loads(geometry)
    if collection is not None:
        gdf["collection"] = collection
    if name is not None:
        gdf["name"] = name
    if sensormode is not None:
        gdf["sensormode"] = sensormode
    if producttype is not None:
        gdf["producttype"] = producttype
    if start_datetime is not None:
        gdf["start_datetime"] = datetime.datetime.strptime(
            start_datetime, "%Y-%m-%d %H:%M:%S"
        )
    if end_datetime is not None:
        gdf["end_datetime"] = datetime.datetime.strptime(
            end_datetime, "%Y-%m-%d %H:%M:%S"
        )
    if publication_start is not None:
        gdf["publication_start"] = publication_start
    if publication_end is not None:
        gdf["publication_end"] = publication_end
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
    gdf["id_original_query"] = gdf.index
    start_time = time.time()
    default_cacherefreshrecent = datetime.timedelta(days=7)
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
            logging.debug('norm_gdf[date_col] %s',type(norm_gdf[date_col].iloc[0]))
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
                        gdf_slice = pd.concat(
                            [gdf_slice, gpd.GeoDataFrame(norm_gdf.loc[to_expand]).T]
                        )
                        gdf_slice.loc[to_expand, "start_datetime"] = latest_start
                        gdf_slice.loc[to_expand, "end_datetime"] = earliest_end
                if not gdf_slice.empty:
                    gdf_slices.append(gdf_slice)
                slice_begin = slice_end
    gdf_norm = gpd.GeoDataFrame(
        pd.concat(gdf_slices, ignore_index=False), crs=gdf_slices[0].crs
    )
    end_time = time.time()
    processing_time = end_time - start_time
    logging.info(f"normalize_gdf processing time:{processing_time}s")
    return gdf_norm


def create_urls(gdf, top=None):
    start_time = time.time()
    urlapi = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
    urls = []
    if top is None:
        top = 1000
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
            coordinates_part = value[value.find("(") + 1 : value.find(")")]
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

        if "name" in gdf_row and not pd.isna(gdf_row["name"]):
            name = gdf_row["name"]
            params["contains"] = f"(Name,'{name}')"

        if "sensormode" in gdf_row and not pd.isna(gdf_row["sensormode"]):
            sensormode = gdf_row["sensormode"]
            params[
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq"
            ] = f" '{sensormode}')"

        if "producttype" in gdf_row and not pd.isna(gdf_row["producttype"]):
            producttype = gdf_row["producttype"]
            params[
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq"
            ] = f" '{producttype}')"

        if "start_datetime" in gdf_row and not pd.isna(gdf_row["start_datetime"]):
            start_datetime = gdf_row["start_datetime"].strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
            params["ContentDate/Start gt"] = f" {start_datetime}"

        if "end_datetime" in gdf_row and not pd.isna(gdf_row["end_datetime"]):
            end_datetime = gdf_row["end_datetime"].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start lt"] = f" {end_datetime}"

        if "Attributes" in gdf_row and not pd.isna(gdf_row["Attributes"]):
            Attributes = str(gdf_row["Attributes"]).replace(" ", "")
            Attributes_name = Attributes[0 : Attributes.find(",")]
            Attributes_value = Attributes[Attributes.find(",") + 1 :]
            params[
                "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq"
            ] = f" '{Attributes_name}' and att/OData.CSC.DoubleAttribute/Value le {Attributes_value})"

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
            json_data = requests.get(url).json()
            cpt["urls_OK"] += 1
        except KeyboardInterrupt:
            raise ("keyboard interrupt")
        except:
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
                        if pd.isna(collected_data["Name"]).any():
                            raise Exception("Name field contains NaN")
                        cpt["answer_append"] += 1
                    else:
                        cpt["nodata_answer"] += 1
                else:
                    cpt["empty_answer"] += 1
    return cpt, collected_data


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
    if cache_dir is not None:
        if not os.path.exists(cache_dir):
            logging.info("mkdir cache dir: %s", cache_dir)
            os.makedirs(cache_dir)
    # with tqdm(total=len(urls)) as pbar:
    for ii in tqdm(range(len(urls))):
        # for url in urls:
        url = urls[ii][1]
        index = urls[ii][0]
        cpt, collected_data = fetch_one_url(url, cpt, index, cache_dir=cache_dir)
        if collected_data is not None:
            if not collected_data.empty:
                collected_data_x.append(collected_data)
    if len(collected_data_x) > 0:
        collected_data_final = pd.concat(collected_data_x)
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
    with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(
        total=len(urls)
    ) as pbar:
        # url[1] is a CDS Odata query URL
        # url[0] is index of original gdf
        future_to_url = {
            executor.submit(fetch_one_url, url[1], cpt, url[0], cache_dir): (
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
