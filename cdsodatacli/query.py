import datetime
import logging
import os
import json
import hashlib
import requests
import pandas as pd
import argparse
from shapely.geometry import GeometryCollection, LineString, Point, Polygon, MultiPolygon
import geopandas as gpd
import shapely
from shapely.ops import unary_union
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import pytz
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import warnings
import xarray as xr


def fetch_data(gdf=None, date=None, dtime=None, timedelta_slice=None, start_datetime=None, end_datetime=None, name=None,
               collection=None, sensormode=None, producttype=None, geometry=None,  max_cloud_percent=None, publication_start=None,
               publication_end=None, min_sea_percent=None, figure=False, top=None, cache_dir=None):
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
    Return:
        (pd.DataFame): data containing the fetched results.
    """

    # Create cache directory
    if cache_dir is None:
        cache_dir = 'api_cache'
        os.makedirs(cache_dir, exist_ok=True)
    else:
        cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    if type(gdf) is str:
        file_extension = os.path.splitext(gdf)[1]
        if file_extension == '.nc':
            ds = xr.open_dataset(gdf)
            df = pd.DataFrame()
            df['LONGITUDE'] = ds['LONGITUDE']
            df['LATITUDE'] = ds['LATITUDE']

            geometry = gpd.points_from_xy(df.LONGITUDE, df.LATITUDE)
            gdf = gpd.GeoDataFrame(df, geometry=geometry)

            # Add time column
            gdf['time'] = pd.to_datetime(ds['TIME'])
            gdf['start_datetime'] = gdf.time - datetime.timedelta(hours=1)
            gdf['end_datetime'] = gdf.time - datetime.timedelta(hours=1)

    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        gdf_norm = normalize_gdf(gdf=gdf, start_datetime=start_datetime, end_datetime=end_datetime, date=date,
                                 dtime=dtime,
                                 timedelta_slice=timedelta_slice)
        # geopd_norm.sort_index(ascending=False)
        logging.debug(f"Length of input:{len(gdf_norm)}")
        urls = urls_creat(gdf=gdf_norm, max_cloud_percent=max_cloud_percent, top=top)
        collected_data = fetch_data_from_urls(urls=urls, cache_dir=cache_dir)
        if collected_data.empty:
            return logging.info("No data found.")

        # Remove duplicates
        data_dedup = remove_duplicates(safes=collected_data)
        # Convert all Multipolygon to Polygon and add geometry as new column
        full_data = multi_to_poly(collected_data=data_dedup)
        if min_sea_percent is not None:
            full_data = sea_percent(collected_data=full_data, min_sea_percent=min_sea_percent)

        if figure:
            fig(collected_data=full_data)

    elif gdf is None:
        gdf = gdf_creat(geometry=geometry, collection=collection, name=name, sensormode=sensormode,
                        producttype=producttype, start_datetime=start_datetime, end_datetime=end_datetime,
                        publication_start=publication_start, publication_end=publication_end)

        gdf_norm = normalize_gdf(gdf=gdf, start_datetime=start_datetime, end_datetime=end_datetime, date=date,
                                 dtime=dtime, timedelta_slice=timedelta_slice)
        urls = urls_creat(gdf=gdf_norm, max_cloud_percent=max_cloud_percent, top=top)
        collected_data = fetch_data_from_urls(urls=urls, cache_dir=cache_dir)
        if collected_data.empty:
            return logging.info("No data found.")
        # Remove duplicates
        data_dedup = remove_duplicates(safes=collected_data)
        # Convert all Multipolygon to Polygon and add geometry as new column
        if min_sea_percent is not None:
            full_data = multi_to_poly(collected_data=data_dedup)
            data_dedup = sea_percent(collected_data=full_data, min_sea_percent=min_sea_percent)

        if fig is True:
            full_data = multi_to_poly(collected_data=data_dedup)
            fig(collected_data=full_data)

    full_data = data_dedup.sort_index()

    return full_data


def gdf_creat(start_datetime=None, end_datetime=None, name=None, collection=None, sensormode=None, producttype=None,
              geometry=None,
              publication_start=None, publication_end=None):
    data_in = {
        "start_datetime": [None], "end_datetime": [None], "name": [None], "collection": [None], "sensormode": [None],
        "producttype": [None], "geometry": [None], "publication_start": [None], "publication_end": [None]
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
        gdf["start_datetime"] = datetime.datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
    if end_datetime is not None:
        gdf["end_datetime"] = datetime.datetime.strptime(end_datetime, "%Y-%m-%d %H:%M:%S")
    if publication_start is not None:
        gdf["publication_start"] = publication_start
    if publication_end is not None:
        gdf["publication_end"] = publication_end
    return gdf


def normalize_gdf(gdf, start_datetime=None, end_datetime=None, date=None, dtime=None, timedelta_slice=None):
    """ return a normalized gdf list
    start/stop date name will be 'start_datetime' and 'end_datetime'
    """
    start_time = time.time()
    default_cacherefreshrecent = datetime.timedelta(days=7)
    default_timedelta_slice = datetime.timedelta(weeks=1)
    if 'startdate' in gdf:
        gdf.rename(columns={'startdate': 'start_datetime'}, inplace=True)
    if 'stopdate' in gdf:
        gdf.rename(columns={'stopdate': 'end_datetime'}, inplace=True)
    if 'geofeature' in gdf:
        gdf.rename(columns={'geofeature': 'geometry'}, inplace=True)

    if timedelta_slice is None:
        timedelta_slice = default_timedelta_slice
    if gdf is not None:
        if not gdf.index.is_unique:
            raise IndexError("Index must be unique. Duplicate founds : %s" % list(
                gdf.index[gdf.index.duplicated(keep=False)].unique()))
        if len(gdf) == 0:
            return []
        norm_gdf = gdf.copy()
        norm_gdf.set_geometry('geometry', inplace=True)
    else:
        norm_gdf = gpd.GeoDataFrame({
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'geometry': Polygon()
        }, geometry='geometry', index=[0], crs="EPSG:4326")
        # no slicing
        timedelta_slice = None

    # convert naives dates to utc
    for date_col in norm_gdf.select_dtypes(include=['datetime64']).columns:
        try:
            norm_gdf[date_col] = norm_gdf[date_col].dt.tz_localize('UTC')
            # logger.warning("Assuming UTC date on col %s" % date_col)
        except TypeError:
            # already localized
            pass

    # check valid input geometry
    # if not all(norm_gdf.is_valid):
    #     raise ValueError("Invalid geometries found. Check them with gdf.is_valid")

    if date in norm_gdf:
        if (start_datetime not in norm_gdf) and (end_datetime not in norm_gdf):
            norm_gdf['start_datetime'] = norm_gdf[date] - dtime
            norm_gdf['end_datetime'] = norm_gdf[date] + dtime
        else:
            raise ValueError('date keyword conflict with startdate/stopdate')

    if (start_datetime in norm_gdf) and (start_datetime != 'start_datetime'):
        norm_gdf['start_datetime'] = norm_gdf[start_datetime]

    if (end_datetime in norm_gdf) and (end_datetime != 'end_datetime'):
        norm_gdf['end_datetime'] = norm_gdf[end_datetime]

    gdf_slices = norm_gdf
    # slice
    if timedelta_slice is not None:
        mindate = norm_gdf['start_datetime'].min()
        maxdate = norm_gdf['end_datetime'].max()
        # those index will need to be time expanded
        idx_to_expand = norm_gdf.index[(norm_gdf['end_datetime'] - norm_gdf['start_datetime']) > timedelta_slice]
        # TO make sure that date does not contain future date
        if maxdate > datetime.datetime.utcnow().replace(tzinfo=pytz.UTC):
            maxdate = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC) + datetime.timedelta(days=1)

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
                    (norm_gdf['start_datetime'] >= slice_begin) & (norm_gdf['end_datetime'] <= slice_end)]
                # check if some slices needs to be expanded
                # index of gdf_slice that where not grouped
                # not_grouped_index = pd.Index(set(idx_to_expand) - set(gdf_slice.index))
                for to_expand in idx_to_expand:
                    # missings index in gdf_slice.
                    # check if there is time overlap.
                    latest_start = max(norm_gdf.loc[to_expand].start_datetime, slice_begin)
                    earliest_end = min(norm_gdf.loc[to_expand].end_datetime, slice_end)
                    overlap = (earliest_end - latest_start)
                    if overlap >= datetime.timedelta(0):
                        gdf_slice = pd.concat([gdf_slice, gpd.GeoDataFrame(norm_gdf.loc[to_expand]).T])
                        gdf_slice.loc[to_expand, 'start_datetime'] = latest_start
                        gdf_slice.loc[to_expand, 'end_datetime'] = earliest_end
                if not gdf_slice.empty:
                    gdf_slices.append(gdf_slice)
                slice_begin = slice_end
    gdf_norm = gpd.GeoDataFrame(pd.concat(gdf_slices, ignore_index=False), crs=gdf_slices[0].crs)
    end_time = time.time()
    processing_time = end_time - start_time
    logging.debug(f"normalize_gdf processing time:{processing_time}s")
    return gdf_norm


def urls_creat(gdf, max_cloud_percent=None,  top=None):
    start_time = time.time()
    urlapi = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter='
    urls = []
    if top is None:
        top = 1000
    for row in range(len(gdf)):
        gdf_row = gdf.iloc[row]
        enter_index = gdf.index[row]

        # Taking all given parameters
        params = {}
        if 'geometry' in gdf_row and not pd.isna(gdf_row['geometry']):
            value = str(gdf_row.geometry)
            geo_type = gdf_row.geometry.geom_type
            coordinates_part = value[value.find("(") + 1:value.find(")")]
            if geo_type == "Point":
                modified_value = f"{coordinates_part}"
                coordinates_part = modified_value.replace(" ", "%20")
                params["OData.CSC.Intersects"] = f"(area=geography'SRID=4326;POINT({coordinates_part})')"
            elif geo_type == "Polygon":
                params["OData.CSC.Intersects"] = f"(area=geography'SRID=4326;POLYGON({coordinates_part}))')"

        if 'collection' in gdf_row and not pd.isna(gdf_row['collection']):
            collection = gdf_row['collection']
            params["Collection/Name eq"] = f" '{collection}'"

        if 'name' in gdf_row and not pd.isna(gdf_row['name']):
            name = gdf_row['name']
            params["contains"] = f"(Name,'{name}')"

        if 'sensormode' in gdf_row and not pd.isna(gdf_row['sensormode']):
            sensormode = gdf_row['sensormode']
            params[
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' and att/OData.CSC.StringAttribute/Value eq"] = f" '{sensormode}')"

        if 'producttype' in gdf_row and not pd.isna(gdf_row['producttype']):
            producttype = gdf_row['producttype']
            params[
                "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq"] = f" '{producttype}')"

        if 'start_datetime' in gdf_row and not pd.isna(gdf_row['start_datetime']):
            # start_datetime = gdf_row['start_datetime'] - datetime.timedelta(hours=10)
            start_datetime = gdf_row['start_datetime'].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start gt"] = f" {start_datetime}"

        if 'end_datetime' in gdf_row and not pd.isna(gdf_row['end_datetime']):
            # end_datetime = gdf_row['end_datetime'] + datetime.timedelta(hours=10)
            end_datetime = gdf_row['end_datetime'].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start lt"] = f" {end_datetime}"

        str_query = ' and '.join([f"{key}{value}" for key, value in params.items()])

        if max_cloud_percent is not None:
            logging.debug("#########")
            max_cloud_percent_str = max_cloud_percent
            str_query = (str_query + f" and Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value lt {max_cloud_percent_str})")


        str_query = (str_query + '&$top=' + str(top))
        url = (urlapi + str_query + '&$expand=Attributes')
        urls.append((enter_index, url))
    end_time = time.time()
    processing_time = end_time - start_time
    logging.debug(f"creat_urls processing time:{processing_time}s")
    return urls


def get_cache_filename(url, cache_dir=None):
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    return os.path.join(cache_dir, url_hash + '.json')


def fetch_data_from_url(url, index, cache_dir=None):
    cache_file = get_cache_filename(url, cache_dir)
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            data = json.load(f)
    else:
        response = requests.get(url)
        data = response.json()
        logging.debug(data)
        if 'value' in data and len(data['value']) > 0:
            with open(cache_file, 'w') as f:
                json.dump(data, f)
    # process data
    df = pd.DataFrame.from_dict(data['value'])
    df['input_index'] = index
    return df


def fetch_data_from_urls(urls, cache_dir=None, max_workers=50):
    collected_data = pd.DataFrame()

    with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(total=len(urls)) as pbar:
        future_to_url = {executor.submit(fetch_data_from_url, url, index, cache_dir): (url, index) for index, url in
                         urls}
        for future in as_completed(future_to_url):
            df = future.result()
            collected_data = pd.concat([collected_data, df])
            pbar.update(1)
    return collected_data


def remove_duplicates(safes):
    start_time = time.time()
    safes_sort = safes.sort_values('ModificationDate', ascending=False)
    safes_dedup = safes_sort.drop_duplicates(subset=['Name'])
    end_time = time.time()
    processing_time = end_time - start_time
    logging.debug(f"remove_duplicates time: {processing_time}s")
    return safes_dedup


def multi_to_poly(collected_data=None):
    start_time = time.time()
    collected_data['geometry'] = collected_data['Footprint'].str.split(';', expand=True)[1].str.strip().str[:-1]
    collected_data['geometry'] = gpd.GeoSeries.from_wkt(collected_data['geometry'])
    collected_data = gpd.GeoDataFrame(collected_data, geometry='geometry', crs="EPSG:4326")
    collected_data['geometry'] = collected_data['geometry'].apply(
        lambda geo: unary_union(geo.geoms) if geo.geom_type == 'MultiPolygon' else geo)
    collected_data = gpd.GeoDataFrame(collected_data, geometry='geometry', crs="EPSG:4326")
    collected_data.dropna(subset=['Id'], inplace=True)
    end_time = time.time()
    processing_time = end_time - start_time
    logging.debug(f"multi_to_poly processing time: {processing_time}s")
    return collected_data


def sea_percent(collected_data, min_sea_percent=None):
    start_time = time.time()
    warnings.filterwarnings("ignore", category=UserWarning)
    earth = GeometryCollection(list(gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')).geometry)).buffer(0)

    sea_percentage = (collected_data.geometry.area - collected_data.geometry.intersection(
        earth).area) / collected_data.geometry.area * 100
    collected_data['sea_percent'] = sea_percentage
    collected_data = collected_data[collected_data['sea_percent'] >= min_sea_percent]
    end_time = time.time()
    processing_time = end_time - start_time
    logging.debug(f"sea_percent processing time:{processing_time}s")
    return collected_data


def fig(collected_data=None):
    start_time = time.time()
    plt.figure(dpi=120, figsize=(10, 8))
    ax = plt.axes(projection=ccrs.PlateCarree())
    for ppi, pp in enumerate(collected_data['geometry']):
        if pp is None:
            continue
        elif ppi == 0:
            plt.plot(*pp.exterior.xy, 'r', label='S1 footprint: %s' % len(collected_data['geometry']))
        else:
            plt.plot(*pp.exterior.xy, 'r', label=None)
            pass
    ax.coastlines(antialiased=True)
    plt.legend()
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
                      linewidth=1, color='gray', alpha=0.5, linestyle='--')
    ext = ax.get_extent()
    ax.set_extent((ext[0] - 3, ext[1] + 3, ext[2] - 0.5, ext[3] + 4))
    plt.title('World map with footprints')
    plt.show()
    end_time = time.time()
    processing_time = end_time - start_time
    logging.debug(f"fig processing time:{processing_time}s")
