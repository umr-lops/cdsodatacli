import pdb
import requests
import datetime
from collections import OrderedDict
import pandas as pd
import argparse
from shapely.geometry import LineString, Point, Polygon


def fetch_data(geometry=None, product=None, name=None, start_datetime=None, end_datetime=None, publication_start=None,
               publication_end=None):
    """
    Fetches data based on provided parameters.

    :param geometry: List of tuples representing the geometry.
    :param product: String representing the product information for filtering the data.D
    :param name: String representing the name information for filtering the data.
    :param start_datetime: String representing the starting date for the query.
    :param end_datetime: String representing the ending date for the query.
    :param publication_start: String representing the starting publication date for the query.
    :param publication_end: String representing the ending publication date for the query.
    :return: JSON data containing the fetched results.
    """
    urlapi = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter='
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
    if product:
        params["Collection/Name eq"] = f" '{product}'"
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
    # print('json\nn',json)
    return json_data


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
    for line in range(len(gdf)):
        gdf_line = gdf.iloc[line]

        if 'geometry' in gdf_line and gdf_line['geometry'] is not None:
            value = str(gdf_line.geometry)
            geo_type = gdf_line.geometry.geom_type
            coordinates_part = value[value.find("(") + 1:value.find(")")]
            if geo_type == "Point":
                modified_value = f"{coordinates_part}"
                coordinates_part = modified_value.replace(" ", "%20")
                coordinates_part = f"POINT({coordinates_part})"
            elif geo_type == "Polygon":
                coordinates_part = f"POLYGON({coordinates_part}))"

            if 'start_time' in gdf_line and not pd.isna(gdf_line['start_time']):
                print(1)
                start_datetime = gdf_line['start_time'].strftime("%Y-%m-%dT%H:%M:%S.0Z")
                end_datetime = gdf_line['end_time'].strftime("%Y-%m-%dT%H:%M:%S.0Z")
                str_query = f"OData.CSC.Intersects(area=geography'SRID=4326;{coordinates_part}') and ContentDate/Start gt {start_datetime} and ContentDate/Start lt {end_datetime}"
                json_data = requests.get(urlapi + str_query).json()
                data = process_data(json_data)
                collected_data = pd.concat([collected_data, data], ignore_index=True)
            else:
                print(2)
                str_query = f"OData.CSC.Intersects(area=geography'SRID=4326;{coordinates_part}')"
                json_data = requests.get(urlapi + str_query).json()
                data = process_data(json_data)
                collected_data = pd.concat([collected_data, data], ignore_index=True)
    return collected_data

