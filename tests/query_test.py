"""Tests for fetch_data function."""
import pdb

import pytest
import shapely
import requests
import pandas as pd
import cdsodatacli.query as qr
import geopandas as gpd
import numpy as np
import logging
# logging.basicConfig(level=logging.DEBUG)
# Test for Query Collection of Products
name_json = requests.get("https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq 'SENTINEL-2' and ContentDate/Start gt 2022-05-03T00:00:00.000Z and ContentDate/Start lt 2022-05-03T00:11:00.000Z&$top=1000").json()
name_df = pd.DataFrame.from_dict(name_json['value'])
name_df = name_df[name_df['Name'].str.contains('_OPER_')==False]
gdf = gpd.GeoDataFrame({
            "start_datetime" : [ np.datetime64('2022-05-03 00:00:00') ],
            "end_datetime"   : [ np.datetime64('2022-05-03 00:11:00')],
            "geometry"   : [ None],
            "collection"  : [ "SENTINEL-2"],
            "name"        : [ None],
            "sensormode"  : [ None],
            "producttype" : [ None],
            "Attributes"  : [ None],
        })
query_name_dfd = qr.fetch_data(gdf=gdf, top=1000)

# Test Query by Geographic Criteria
geographic_json = requests.get("https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=OData.CSC.Intersects(area=geography'SRID=4326;POLYGON((12.655118166047592 47.44667197521409,21.39065656328509 48.347694733853245,28.334291357162826 41.877123516783655,17.47086198383573 40.35854475076158,12.655118166047592 47.44667197521409))') and ContentDate/Start gt 2022-05-20T00:00:00.000Z and ContentDate/Start lt 2022-05-21T00:00:00.000Z&$top=1000").json()
geographic_df = pd.DataFrame.from_dict(geographic_json['value'])
gdf = gpd.GeoDataFrame({
            "start_datetime" : [ np.datetime64('2022-05-20 00:00:00') ],
            "end_datetime"   : [ np.datetime64('2022-05-21 00:00:00')],
            "geometry"   : [ shapely.wkt.loads('POLYGON((12.655118166047592 47.44667197521409,21.39065656328509 48.347694733853245,28.334291357162826 41.877123516783655,17.47086198383573 40.35854475076158,12.655118166047592 47.44667197521409))')],
            "collection"  : [ None],
            "name"        : [ None],
            "sensormode"  : [ None],
            "producttype" : [ None],
            "Attributes"  : [ None],
        })
query_geographic_name = qr.fetch_data(gdf=gdf, top=1000)

# Test Query by attributes
cloudCover_json = requests.get("https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' and att/OData.CSC.DoubleAttribute/Value le 40.00) and ContentDate/Start gt 2022-01-01T00:00:00.000Z and ContentDate/Start lt 2022-01-01T01:00:00.000Z&$top=1000").json()
cloudCover_df = pd.DataFrame.from_dict(cloudCover_json['value'])
gdf = gpd.GeoDataFrame({
            "start_datetime" : [ np.datetime64('2022-01-01 00:00:00') ],
            "end_datetime"   : [ np.datetime64('2022-01-01 01:00:00')],
            "geometry"   : [ None],
            "collection"  : [ None],
            "name"        : [ None],
            "sensormode"  : [ None],
            "producttype" : [ None],
            "Attributes"  : [ 'cloudCover,40'],
        })
query_cloudCover_df = qr.fetch_data(gdf=gdf, top=1000)
@pytest.mark.parametrize(
    ("expected", "result"),
    [
        (name_df,query_name_dfd),
        (geographic_df,query_geographic_name),
        (cloudCover_df,query_cloudCover_df),

    ]
)
def test_queries(expected,result):
    # assert expected.equals(result)
    assert all(item in list(expected['Name']) for item in list(result['Name']))
    assert all(item in list(result['Name']) for item in list(expected['Name']))

# for uu in range(3):
# test_queries(name_df,query_name_dfd)
# test_queries(geographic_df,query_geographic_name)
# test_queries(cloudCover_df,query_cloudCover_df)
# def test_queryname(query_name_dfd=query_name_dfd, name_df=name_df):
#     """Example test with parametrization."""
#     assert all(item in list(query_name_dfd['Name']) for item in list(name_df['Name']))
#     # logging.info('test_queryname: OK')
#
#
# def test_querygeographic(query_geographic_name=query_geographic_name, geographic_df=geographic_df):
#     """Example test with parametrization."""
#     assert all(item in list(query_geographic_name['Name']) for item in list(geographic_df['Name']))
#
#
# def test_querycloudcover(query_cloudCover_df=query_cloudCover_df, cloudCover_df=cloudCover_df):
#     """Example test with parametrization."""
#     assert all(item in list(query_cloudCover_df['Name']) for item in list(cloudCover_df['Name']))
