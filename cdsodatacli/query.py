import pdb
import requests
import datetime
from collections import OrderedDict
import pandas as pd
import argparse
from shapely.geometry import LineString, Point, Polygon


def fetch_data(geometry=None, product=None, name=None, start_datetime=None, end_datetime=None, publication_start=None,
               publication_end=None):
    print(geometry)

    urlapi = 'https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter='
    geo = determine_geometry_type(geometry)
    print(geo)

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
            print(coordinates_part)
            modified_value = f"{coordinates_part}"
            coordinates_part = modified_value.replace(" ", "%")
            print(coordinates_part)
        elif geo == "Polygon":
            coordinates_part = f"{coordinates_part})"
    else:
        print("Invalid geometry type")

    # Taking all given parameters
    params = {}

    if geometry:
        params["OData.CSC.Intersects"] = f"(area=geography'SRID=4326;{geo_type}({coordinates_part})')"
    if product:
        params["Collection/Name eq"] = f"'{product}'"
    if name:
        params["contains(Name,'S1A')"] = f"'(Name,'{name}')'"
    if start_datetime:
        params["ContentDate/Start gt"] = f" {start_datetime}"
    if end_datetime:
        params["ContentDate/Start lt"] = f" {end_datetime}"
    if publication_start:
        params["PublicationDate gt"] = f" {publication_start}"
    if publication_end:
        params["PublicationDate lt"] = f" {publication_end}"

    str_query = ' and '.join([f"{key}{value}" for key, value in params.items()])
    str = urlapi + str_query
    print(str)
    json_data = requests.get(urlapi + str_query).json()
    # print('json\nn',json)
    return json_data


def determine_geometry_type(array):
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
    res = None
    visu = None
    if 'value' in json_data:
        res = pd.DataFrame.from_dict(json_data['value'])
        columns_to_print = ['Id', 'Name', 'S3Path', 'GeoFootprint']
        visu = res[columns_to_print].head(3)
    else:
        print("No data found.")
    return visu
