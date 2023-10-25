import datetime
import requests
import pandas as pd
import argparse
import shapely
from shapely.ops import unary_union
from shapely.geometry import GeometryCollection, LineString, Point, Polygon, MultiPolygon
import geopandas as gpd


def fetch_data(gdf=None, geometry=None, collection=None, name=None, sensormode=None, producttype=None,
               start_datetime=None, end_datetime=None, publication_start=None,
               publication_end=None, top=None):
    """
    Fetches data based on provided parameters.
    :param gdf: GeoDataFrame containing the geospatial data for the query.
    :param geometry: List of tuples representing the geometry.
    :param collection: String representing the collection information for filtering the data.
    :param name: String representing the name information for filtering the data.
    :param sensormode: String representing the type of product for filtering the data.
    :param producttype: String representing the mode of the sensor for filtering the data.
    :param start_datetime: String representing the starting date for the query.
    :param end_datetime: String representing the ending date for the query.
    :param publication_start: String representing the starting publication date for the query.
    :param publication_end: String representing the ending publication date for the query.
    :param top: String representing the ending publication date for the query.
    :return: pdDataFame data containing the fetched results.
    """
    urlapi = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter='
    if gdf is not None and isinstance(gdf, gpd.GeoDataFrame):
        collected_data = fetch_data_by_gdf(gdf)
    else:

        # Taking all given parameters
        params = {}

        if geometry is not None:
            geo = determine_geometry_type(geometry)
            # Shapely form
            if geo == "Unknown":
                shape = None
            elif geo == "Polygon":
                shape = Polygon(geometry)
            elif geo == "Line":
                shape = LineString(geometry)
            elif geo == "Point":
                shape = Point(geometry)
            else:
                shape = None

            # To avoid the space between type and coordinates
            if shape:
                value = shape.wkt
                geo_type = value.split('(')[0].strip()
                coordinates_part = value[value.find("(") + 1:value.find(")")]
                if geo == "Point":
                    modified_value = f"{coordinates_part}"
                    coordinates_part = modified_value.replace(" ", "%20")
                elif geo == "Polygon":
                    coordinates_part = f"{coordinates_part})"
            else:
                print("No geometry input or invalid geometry type")
            params["OData.CSC.Intersects"] = f"(area=geography'SRID=4326;{geo_type}({coordinates_part})')"

        if collection is not None:
            params["Collection/Name eq"] = f" '{collection}'"

        if name is not None:
            params["contains"] = f"(Name,'{name}')"

        if sensormode is not None:
            params["contains"] = f"(Name,'{sensormode}')"

        if producttype is not None:
            params["contains"] = f"(Name,'{producttype}')"

        if start_datetime is not None:
            params["ContentDate/Start gt"] = f" {start_datetime}"

        if end_datetime is not None:
            params["ContentDate/Start lt"] = f" {end_datetime}"

        if publication_start is not None:
            params["PublicationDate gt"] = f" {publication_start}"

        if publication_end is not None:
            params["PublicationDate lt"] = f" {publication_end}"

        str_query = ' and '.join([f"{key}{value}" for key, value in params.items()])
        if top is not None:
            top = str(top)
            str_query = (str_query + '&$top=' + top)
        # print(str_query)
        json_data = requests.get(urlapi + str_query).json()
        collected_data = process_data(json_data)
    # print('json\nn',json)
    return collected_data


def determine_geometry_type(array):
    """
    Determines the type of geometry based on the input array.

    :param array: List of tuples representing the geometry.
    :return: Type of geometry ('Unknown', 'Point', 'Line', 'Polygon').
    """
    dimensions = len(array)
    if dimensions == 0:
        return "Unknown"
    elif dimensions == 1 and len(array[0]) == 2:
        return "Point"
    elif dimensions >= 3 and all(len(item) == 2 for item in array):
        if array[0] == array[-1]:
            return "Polygon"
        else:
            return "Line"
    else:
        return "Unknown"


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
    return res


def fetch_data_by_gdf(gdf):
    urlapi = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter='
    collected_data = pd.DataFrame()
    collected_data_sea_ok = pd.DataFrame()
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
            params["contains"] = f"(Name,'{sensormode}')"

        if 'producttype' in gdf_row and not pd.isna(gdf_row['producttype']):
            producttype = gdf_row['producttype']
            params["contains"] = f"(Name,'{producttype}')"

        if 'start_time' in gdf_row and not pd.isna(gdf_row['start_time']):
            start_datetime = gdf_row['start_time'].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start gt"] = f" {start_datetime}"

        if 'end_time' in gdf_row and not pd.isna(gdf_row['end_time']):
            end_datetime = gdf_row['end_time'].strftime("%Y-%m-%dT%H:%M:%S.000Z")
            params["ContentDate/Start lt"] = f" {end_datetime}"

        if 'Attributes' in gdf_row and not pd.isna(gdf_row['Attributes']):
            Attributes = str(gdf_row['Attributes']).replace(" ", "")
            Attributes_name = Attributes[0:Attributes.find(",")]
            Attributes_value = Attributes[Attributes.find(",") + 1:]
            params[
                "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq"] = f" '{Attributes_name}' and att/OData.CSC.DoubleAttribute/Value le {Attributes_value})"

        str_query = ' and '.join([f"{key}{value}" for key, value in params.items()])
        top_value = {}
        if 'top' in gdf_row and not pd.isna(gdf_row['top']):
            top = str(gdf_row['top'])
            str_query = (str_query + '&$top=' + top)
        json_data = requests.get(urlapi + str_query + '&$expand=Attributes').json()
        data = process_data(json_data)
        data['Enter_index'] = enter_index
        collected_data = pd.concat([collected_data, data], ignore_index=True)

        if 'min_sea_percent' in gdf_row and not pd.isna(gdf_row['min_sea_percent']):
            min_sea_percent = float(gdf_row['min_sea_percent'])
            earth = GeometryCollection(
                list(gpd.read_file(gpd.datasets.get_path('naturalearth_lowres')).geometry)).buffer(0)
            for i in range(len(collected_data)):
                if collected_data.iloc[i].GeoFootprint["type"] == "MultiPolygon":
                    geo_type = collected_data.iloc[i].GeoFootprint["type"]
                    geo_coord = collected_data.iloc[i].GeoFootprint["coordinates"]
                    geofootprint = MultiPolygon(geo_coord)
                    shp_geo = unary_union(geofootprint)
                else:
                    footprint = collected_data.iloc[i].Footprint
                    geo = footprint[footprint.find(";") + 1:footprint.find(")") + 2]
                    shp_geo = shapely.wkt.loads(geo)
                sea_percent = (shp_geo.area - shp_geo.intersection(earth).area) / shp_geo.area * 100
                # print(sea_percent)
                if sea_percent >= min_sea_percent:
                    data_sea_ok = collected_data.iloc[i:i + 1]
                    collected_data_sea_ok = pd.concat([collected_data_sea_ok, data_sea_ok], ignore_index=True)
            collected_data = collected_data_sea_ok
    return collected_data


# Test
gdf = gpd.GeoDataFrame({
    "start_time": [datetime.datetime(2021, 7, 4, 0)],
    "end_time": [datetime.datetime(2021, 7, 5, 23, 59, 59)],
    "collection": ["SENTINEL-1"],
    "name": [None],
    "sensormode": [None],
    "producttype": [None],
    "Attributes": [None],
    "geometry": [None],
    "top": [100],
    "min_sea_percent": [90]
})

collected_data = fetch_data(gdf)
print(collected_data)

