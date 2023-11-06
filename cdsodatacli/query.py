import datetime
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
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import warnings


def fetch_data(gdf=None, date=None, dtime=None, timedelta_slice=None, start_datetime=None, end_datetime=None, name=None,
               collection=None, sensormode=None, producttype=None, geometry=None, publication_start=None,
               publication_end=None, min_sea_percent=None, fig=None, top=None):
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

    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        gdf_norm = normalize_gdf(gdf=gdf, start_datetime=start_datetime, end_datetime=end_datetime, date=date,
                                 dtime=dtime,
                                 timedelta_slice=timedelta_slice)
        # geopd_norm.sort_index(ascending=False)
        print(gdf_norm)
        print(f"Length of input:{len(gdf_norm)}")
        urls = urls_creat(gdf=gdf_norm, top=top)

    collected_data = fetch_data_from_urls(urls=urls)
    # Remove duplicates
    data_dedup = remove_duplicates(safes_ori=collected_data)

    # Convert all Multipolygon to Polygon and add geometry as new column
    full_data = multy_to_poly(collected_data=data_dedup)

    if min_sea_percent is not None:
        full_data = sea_percent(collected_data=full_data, min_sea_percent=min_sea_percent)

    if fig is not None:
        fig(collected_data=full_data)

    elif gdf is None:
        gdf = gdf_creat(geometry=geometry, collection=collection, name=name, sensormode=sensormode,
                        producttype=producttype, start_datetime=start_datetime, end_datetime=end_datetime,
                        publication_start=publication_start, publication_end=publication_end)

        gdf_norm = normalize_gdf(gdf=gdf, start_datetime=start_datetime, end_datetime=end_datetime, date=date,
                                 dtime=dtime, timedelta_slice=timedelta_slice)
        print(gdf_norm)
        urls = urls_creat(gdf=gdf_norm, top=top)
        collected_data = fetch_data_from_urls(urls=urls)
        # Remove duplicates
        data_dedup = remove_duplicates(safes_ori=collected_data)

        # Convert all Multipolygon to Polygon and add geometry as new column
        full_data = multy_to_poly(collected_data=data_dedup)

        if min_sea_percent is not None:
            full_data = sea_percent(collected_data=full_data, min_sea_percent=min_sea_percent)

        if fig is not None:
            fig(collected_data=full_data)

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
    if not all(norm_gdf.is_valid):
        raise ValueError("Invalid geometries found. Check them with gdf.is_valid")

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
    print(f"normalize_gdf processing time:{processing_time}s")
    return gdf_norm


def urls_creat(gdf, top=None):
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
            start_datetime = gdf_row['start_datetime'].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start gt"] = f" {start_datetime}"

        if 'end_datetime' in gdf_row and not pd.isna(gdf_row['end_datetime']):
            end_datetime = gdf_row['end_datetime'].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start lt"] = f" {end_datetime}"

        if 'Attributes' in gdf_row and not pd.isna(gdf_row['Attributes']):
            Attributes = str(gdf_row['Attributes']).replace(" ", "")
            Attributes_name = Attributes[0:Attributes.find(",")]
            Attributes_value = Attributes[Attributes.find(",") + 1:]
            params[
                "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq"] = f" '{Attributes_name}' and att/OData.CSC.DoubleAttribute/Value le {Attributes_value})"

        str_query = ' and '.join([f"{key}{value}" for key, value in params.items()])

        str_query = (str_query + '&$top=' + str(top))
        url = (urlapi + str_query + '&$expand=Attributes')
        urls.append(url)

    end_time = time.time()
    processing_time = end_time - start_time
    print(f"creat_urls processing time:{processing_time}s")
    return urls


def fetch_data_from_urls(urls):
    start_time = time.time()
    collected_data = []

    with tqdm(total=len(urls)) as pbar:
        for url in urls:
            json_data = requests.get(url).json()
            if 'value' in json_data:
                collected_data = pd.DataFrame.from_dict(json_data['value'])
                pbar.update(1)

    collected_data = pd.DataFrame(collected_data)
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"fetch_data_from_urls time:{processing_time}s")
    return collected_data


#def fetch_url(url):
#    data = requests.get(url).json()
#    return process_data(data)


#def fetch_data_from_urls(urls):
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
    if 'value' in json_data:
        res = pd.DataFrame.from_dict(json_data['value'])
    else:
        print("No data found.")
        pass
    return res


def remove_duplicates(safes_ori, keep_list=[]):
    """
    Remove duplicate safe (ie same footprint with same date, but different prodid)
    """
    start_time = time.time()
    safes = safes_ori.copy()
    if not safes.empty:
        # remove duplicate safes
        # add a temporary col with filename radic
        safes['__filename_radic'] = [f for f in safes['Name']]
        # print(safes['__filename_radic'])

        uniques_radic = safes['__filename_radic'].unique()  # list all unique name

        for filename_radic in uniques_radic:
            sames_safes = safes[safes['__filename_radic'] == filename_radic]
            # print(len(sames_safes['Name'].unique()))
            if len(sames_safes['Name']) > 1:
                force_keep = list(set(sames_safes['Name']).intersection(keep_list))
                to_keep = sames_safes[
                    'ModificationDate'].max()
                if force_keep:
                    _to_keep = sames_safes[sames_safes['Name'] == force_keep[0]]['ModificationDate'].iloc[0]
                    if _to_keep != to_keep:
                        to_keep = _to_keep
                safes = safes[(safes['ModificationDate'] == to_keep) | (safes['__filename_radic'] != filename_radic)]
        safes = safes.drop_duplicates(subset=['Name'])
        safes.drop('__filename_radic', axis=1, inplace=True)
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"remove_duplicates processing time:{processing_time}s")
    return safes


def multy_to_poly(collected_data=None):
    start_time = time.time()
    collected_data['geometry'] = collected_data['Footprint'].str.split(';', expand=True)[1].str.strip().str[:-1]
    collected_data['geometry'] = gpd.GeoSeries.from_wkt(collected_data['geometry'])
    collected_data = gpd.GeoDataFrame(collected_data, geometry='geometry', crs="EPSG:4326")
    place = 0;
    for geo in collected_data['geometry']:
        single_poly = geo
        if geo.geom_type == 'MultiPolygon':
            # geo = geo.buffer(10)
            polys = geo.geoms
            single_poly = unary_union(polys)
        collected_data.loc[place, 'geometry'] = single_poly
        place += 1
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"multi_to_poly processing time:{processing_time}s")
    return collected_data


def sea_percent(collected_data, min_sea_percent=None):
    start_time = time.time()
    warnings.simplefilter(action='ignore', category=FutureWarning)
    earth = GeometryCollection(list(gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')).geometry)).buffer(0)
    sea_percent = (collected_data.geometry.area - collected_data.geometry.intersection(
        earth).area) / collected_data.geometry.area * 100
    collected_data['sea_percent'] = sea_percent
    collected_data = collected_data[collected_data['sea_percent'] >= min_sea_percent]
    end_time = time.time()
    processing_time = end_time - start_time
    print(f"sea_percent processing time:{processing_time}s")
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
    print(f"fig processing time:{processing_time}s")


# Test
gdf = gpd.GeoDataFrame({
        "start_datetime" : [ datetime.datetime(2021,10,2,0) ,  datetime.datetime(2021,10,13,0) ],
        "end_datetime"   : [ datetime.datetime(2022,10,2,21),  datetime.datetime(2022,10,13,18) ],
        "geometry"   : [ shapely.wkt.loads("POINT (-7.5 53)"),  shapely.wkt.loads("POLYGON ((-12 35, -5 35, -5 45, -12 45, -12 35))")],
        "collection"  : [ "SENTINEL-1", "SENTINEL-2"],
        "name"        : [ None, None],
        "sensormode"  : [ None, None],
        "producttype" : [ None, None],
        "Attributes"  : [ None, None],
    })

# collected_data_norm = fetch_data(gdf, min_sea_percent=20, fig=fig)
# print(collected_data_norm)




















