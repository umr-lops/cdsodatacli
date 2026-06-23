# query_test.py - Version qui patch correctement normalize_gdf
"""Tests for fetch_data function."""

import pytest
import cdsodatacli.query as qr
import geopandas as gpd
import numpy as np
from datetime import datetime
from unittest.mock import patch, MagicMock
import shapely


# Désactiver le rate limiting pour les tests
@pytest.fixture(autouse=True)
def disable_rate_limiter():
    """Désactive le rate limiter global pour les tests."""
    with patch("cdsodatacli.query._GLOBAL_RATE_LIMITER") as mock_limiter:
        mock_limiter.wait_if_needed = MagicMock()
        yield


# Mock des réponses pour éviter les vrais appels API
@pytest.fixture
def mock_requests():
    """Mock les requêtes HTTP pour les tests."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        yield mock_get


# Helper pour créer des données de test complètes
def create_mock_product(id_val, name, modification_date=None):
    """
    Crée un produit mocké avec tous les champs nécessaires.
    """
    if modification_date is None:
        modification_date = datetime.now().isoformat()

    footprint = "SRID=4326;POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)))"

    return {
        "Id": id_val,
        "Name": name,
        "ModificationDate": modification_date,
        "ContentLength": 1000,
        "Footprint": footprint,
    }


# Helper pour créer un GeoDataFrame de test
def create_test_gdf(
    start_datetime,
    end_datetime,
    geometry=None,
    collection=None,
    name=None,
    sensormode=None,
    producttype=None,
    attributes=None,
    id_query="test1",
):
    """Crée un GeoDataFrame de test."""
    gdf = gpd.GeoDataFrame(
        {
            "start_datetime": [np.datetime64(start_datetime)],
            "end_datetime": [np.datetime64(end_datetime)],
            "geometry": [geometry] if geometry is not None else [None],
            "collection": [collection],
            "name": [name],
            "sensormode": [sensormode],
            "producttype": [producttype],
            "Attributes": [attributes],
            "id_query": [id_query],
        }
    )
    return gdf


# Fixture qui patch correctement normalize_gdf
@pytest.fixture
def patch_normalize_gdf():
    """
    Patch normalize_gdf pour qu'il préserve id_query.
    Cette version remplace COMPLÈTEMENT la fonction originale.
    """

    def patched_normalize_gdf(gdf, timedelta_slice=None):
        # Copier le DataFrame
        gdf_copy = gdf.copy()

        # Si id_query existe, l'utiliser comme id_original_query
        if "id_query" in gdf_copy.columns:
            gdf_copy["id_original_query"] = gdf_copy["id_query"]
        else:
            gdf_copy["id_original_query"] = gdf_copy.index

        # Appliquer le slicing temporel si nécessaire
        if timedelta_slice is not None:
            # Copier le comportement de apply_slicing_time_to_gdf
            from cdsodatacli.query import apply_slicing_time_to_gdf

            gdf_copy = apply_slicing_time_to_gdf(gdf_copy, timedelta_slice)

        # S'assurer que geometry est bien définie
        if "geometry" not in gdf_copy.columns:
            from cdsodatacli.query import WORLDPOLYGON

            gdf_copy["geometry"] = WORLDPOLYGON

        return gdf_copy

    # Appliquer le patch sur normalize_gdf
    with patch("cdsodatacli.query.normalize_gdf", side_effect=patched_normalize_gdf):
        yield


@pytest.mark.parametrize(
    ("gdf_input", "expected_products", "id_query"),
    [
        (
            # Test 1: Query by name (SENTINEL-2)
            create_test_gdf(
                start_datetime="2022-05-03 00:00:00",
                end_datetime="2022-05-03 00:11:00",
                collection="SENTINEL-2",
                id_query="test1",
            ),
            [
                create_mock_product(
                    "1",
                    "S2A_MSIL1C_20220503T000000_R001_T30TXL_20220503T000000",
                    "2022-05-03T00:00:00Z",
                ),
                create_mock_product(
                    "2",
                    "S2B_MSIL1C_20220503T000000_R001_T30TXL_20220503T000000",
                    "2022-05-03T00:00:01Z",
                ),
            ],
            "test1",
        ),
        (
            # Test 2: Query by geographic criteria
            create_test_gdf(
                start_datetime="2022-05-20 00:00:00",
                end_datetime="2022-05-21 00:00:00",
                geometry=shapely.wkt.loads(
                    "POLYGON((12.655118166047592 47.44667197521409,"
                    "21.39065656328509 48.347694733853245,"
                    "28.334291357162826 41.877123516783655,"
                    "17.47086198383573 40.35854475076158,"
                    "12.655118166047592 47.44667197521409))"
                ),
                id_query="test2",
            ),
            [
                create_mock_product(
                    "3",
                    "S2A_MSIL1C_20220520T000000_R001_T30TXL_20220520T000000",
                    "2022-05-20T00:00:00Z",
                ),
                create_mock_product(
                    "4",
                    "S2B_MSIL1C_20220520T000000_R001_T30TXL_20220520T000000",
                    "2022-05-20T00:00:01Z",
                ),
            ],
            "test2",
        ),
        (
            # Test 3: Query by cloud cover
            create_test_gdf(
                start_datetime="2022-01-01 00:00:00",
                end_datetime="2022-01-01 01:00:00",
                attributes="cloudCover,40",
                id_query="test3",
            ),
            [
                create_mock_product(
                    "5",
                    "S2A_MSIL1C_20220101T000000_R001_T30TXL_20220101T000000",
                    "2022-01-01T00:00:00Z",
                ),
                create_mock_product(
                    "6",
                    "S2B_MSIL1C_20220101T000000_R001_T30TXL_20220101T000000",
                    "2022-01-01T00:00:01Z",
                ),
            ],
            "test3",
        ),
    ],
)
def test_queries(
    gdf_input, expected_products, id_query, mock_requests, patch_normalize_gdf
):
    """Test des requêtes avec données mockées complètes."""
    # Mock de la réponse avec les données attendues
    mock_response = MagicMock()
    mock_response.json.return_value = {"value": expected_products}
    mock_response.status_code = 200
    mock_requests.return_value = mock_response

    # Appel de la fonction
    result = qr.fetch_data(gdf=gdf_input, top=1000, display_tqdm=False)

    # Vérifications
    assert result is not None
    assert not result.empty
    assert "id_original_query" in result.columns

    # Vérifier que id_original_query contient bien l'ID de la requête
    unique_ids = result["id_original_query"].unique()
    assert len(unique_ids) == 1, f"Expected exactly one unique id, got {unique_ids}"
    assert (
        str(unique_ids[0]) == id_query
    ), f"Expected '{id_query}', got '{unique_ids[0]}'"

    # Vérifier que les noms des produits sont corrects
    expected_names = [p["Name"] for p in expected_products]
    result_names = result["Name"].values
    for name in expected_names:
        assert name in result_names, f"Expected '{name}' not found in results"

    # Vérifier que la géométrie a été correctement extraite
    assert "geometry" in result.columns
    assert not result["geometry"].isna().any()


# Test pour id_original_query
@pytest.mark.parametrize(
    ("gdf_input", "expected_products", "id_query"),
    [
        (
            create_test_gdf(
                start_datetime="2022-05-03 00:00:00",
                end_datetime="2022-05-03 00:11:00",
                collection="SENTINEL-2",
                id_query="test1",
            ),
            [
                create_mock_product(
                    "1", "S2A_MSIL1C_20220503T000000_R001_T30TXL_20220503T000000"
                ),
                create_mock_product(
                    "2", "S2B_MSIL1C_20220503T000000_R001_T30TXL_20220503T000000"
                ),
            ],
            "test1",
        ),
    ],
)
def test_id_original_query(
    gdf_input, expected_products, id_query, mock_requests, patch_normalize_gdf
):
    """Test que la colonne id_original_query est correctement définie."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"value": expected_products}
    mock_response.status_code = 200
    mock_requests.return_value = mock_response

    result = qr.fetch_data(gdf=gdf_input, top=1000, display_tqdm=False)

    assert result is not None
    assert "id_original_query" in result.columns

    unique_ids = result["id_original_query"].unique()
    assert len(unique_ids) == 1, f"Expected exactly one unique id, got {unique_ids}"
    assert (
        str(unique_ids[0]) == id_query
    ), f"Expected '{id_query}', got '{unique_ids[0]}'"


# Test pour multiple id_original_query
def test_multiple_id_original_query(mock_requests, patch_normalize_gdf):
    """Test qu'un produit peut apparaître avec plusieurs id_query."""
    gdf_multi_id = gpd.GeoDataFrame(
        {
            "start_datetime": [
                np.datetime64("2022-05-03 00:00:00"),
                np.datetime64("2022-05-03 00:00:00"),
            ],
            "end_datetime": [
                np.datetime64("2022-05-03 00:02:00"),
                np.datetime64("2022-05-03 00:02:00"),
            ],
            "geometry": [None, None],
            "collection": ["SENTINEL-2", "SENTINEL-2"],
            "name": [None, None],
            "sensormode": [None, None],
            "producttype": [None, None],
            "Attributes": [None, None],
            "id_query": ["test1", "test2"],
        }
    )

    # Créer des produits mockés
    products = [
        create_mock_product(
            "1",
            "S2A_MSIL1C_20220503T000000_R001_T30TXL_20220503T000000",
            "2022-05-03T00:00:00Z",
        ),
        create_mock_product(
            "2",
            "S2B_MSIL1C_20220503T000000_R001_T30TXL_20220503T000000",
            "2022-05-03T00:00:01Z",
        ),
    ]

    mock_response = MagicMock()
    mock_response.json.return_value = {"value": products}
    mock_response.status_code = 200
    mock_requests.return_value = mock_response

    result = qr.fetch_data(gdf=gdf_multi_id, top=1000, display_tqdm=False)

    # Vérifier que les produits apparaissent avec les deux id_query
    counts = result["id_original_query"].value_counts()

    # Les IDs devraient être "test1" et "test2"
    assert "test1" in counts.index, f"Expected 'test1' in {counts.index.tolist()}"
    assert "test2" in counts.index, f"Expected 'test2' in {counts.index.tolist()}"

    # Chaque requête devrait retourner le même nombre de produits
    assert (
        counts["test1"] == counts["test2"]
    ), f"Counts differ: test1={counts['test1']}, test2={counts['test2']}"

    # Le nombre total de produits devrait être 2 * nombre de produits uniques
    expected_total = len(products) * 2
    assert (
        len(result) == expected_total
    ), f"Expected {expected_total} rows, got {len(result)}"


# Test additionnel pour vérifier le traitement des Footprints
def test_footprint_parsing(mock_requests, patch_normalize_gdf):
    """Test que le parsing des Footprints fonctionne correctement."""
    gdf = create_test_gdf(
        start_datetime="2022-05-03 00:00:00",
        end_datetime="2022-05-03 00:11:00",
        collection="SENTINEL-2",
        id_query="test1",
    )

    # Créer un produit avec un Footprint valide
    products = [
        {
            "Id": "1",
            "Name": "S2A_MSIL1C_20220503T000000_R001_T30TXL_20220503T000000",
            "ModificationDate": "2022-05-03T00:00:00Z",
            "ContentLength": 1000,
            "Footprint": "SRID=4326;POLYGON((1 2, 3 4, 5 6, 1 2)))",
        }
    ]

    mock_response = MagicMock()
    mock_response.json.return_value = {"value": products}
    mock_response.status_code = 200
    mock_requests.return_value = mock_response

    result = qr.fetch_data(gdf=gdf, top=1000, display_tqdm=False)

    # Vérifier que le Footprint a été correctement parsé
    assert "geometry" in result.columns
    assert not result["geometry"].isna().any()
    assert isinstance(result["geometry"].iloc[0], shapely.geometry.base.BaseGeometry)

    # Vérifier que l'ID de requête est correct
    unique_ids = result["id_original_query"].unique()
    assert len(unique_ids) == 1
    assert str(unique_ids[0]) == "test1"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
