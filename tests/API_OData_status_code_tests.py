import pytest
import requests
import cdsodatacli.query as qr

@pytest.mark.parametrize(
    ("input_query","expected_result"),
    [
        ("https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinl2/search.json",404),
        ("https://catalogue.dataspace.copernicus.eu/resto/api/collections/search.json?productsType=S2MSI1C",400),
        ("https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel2/search.json?productType=S2MSI1C&startDat=2023-06-11&completionDte=2023-06-22",400),
        ("https://catalogue.dataspace.copernicus.eu/resto/api/collections/search.json?startDate=2021-07-01T00:00:00Z&completionDate=2021-07-31T23:59:59Z&maxRecords=2001",400),
        ("https://catalogue.dataspace.copernicus.eu/resto/api/collections/search.json?orbitNumber=ascending",400)

    ]
)
def test_status(input_query,expected_result):
    response = requests.get(input_query)
    assert response.status_code==expected_result
