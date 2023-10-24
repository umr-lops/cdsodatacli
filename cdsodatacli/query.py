import pdb
import requests
import datetime
from collections import OrderedDict
import pandas as pd
import argparse
from shapely.geometry import LineString, Point, Polygon


def fetch_data(gdf=None, geometry=None, collection=None, name=None, start_datetime=None, end_datetime=None, publication_start=None,
               publication_end=None):
    """
    Fetches data based on provided parameters.

    :param gdf:
    :param geometry: List of tuples representing the geometry.
    :param collection: String representing the collection information for filtering the data.
    :param name: String representing the name information for filtering the data.
    :param start_datetime: String representing the starting date for the query.
    :param end_datetime: String representing the ending date for the query.
    :param publication_start: String representing the starting publication date for the query.
    :param publication_end: String representing the ending publication date for the query.
    :return: JSON data containing the fetched results.
    """
    urlapi = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter='
    if gdf is not None:
        collected_data = fetch_data_by_gdf(gdf)
    else:
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

        # Taking all given parameters
        params = {}

        if geometry:
            params["OData.CSC.Intersects"] = f"(area=geography'SRID=4326;{geo_type}({coordinates_part})')"
        if collection:
            params["Collection/Name eq"] = f" '{collection}'"
        if name:
            params["contains"] = f"(Name,'{name}')"
        if start_datetime:
            params["ContentDate/Start gt"] = f" {start_datetime}"
        if end_datetime:
            params["ContentDate/Start lt"] = f" {end_datetime}"
        if publication_start:
            params["PublicationDate gt"] = f" {publication_start}"
        if publication_end:
            params["PublicationDate lt"] = f" {publication_end}"

        str_query = ' and '.join([f"{key}{value}" for key, value in params.items()])
        json_data = requests.get(urlapi + str_query).json()
        collected_data = process_data(json_data)
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
    visu = None
    if 'value' in json_data:
        res = pd.DataFrame.from_dict(json_data['value'])
        columns_to_print = ['Id', 'Name', 'S3Path', 'GeoFootprint']
        visu = res[columns_to_print].head(3)
    else:
        print("No data found.")
    return visu


def fetch_data_by_gdf(gdf):
    urlapi = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter='
    collected_data = pd.DataFrame()
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
        json_data = requests.get(urlapi + str_query).json()
        # print(json_data)
        data = process_data(json_data)
        data['Enter_index'] = enter_index
        collected_data = pd.concat([collected_data, data], ignore_index=True)
    return collected_data


