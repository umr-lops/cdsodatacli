# test_match_s1_product_types.py - Version corrigée

import pytest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
from collections import defaultdict
import logging

from cdsodatacli.scripts.match_s1_product_types import (
    parse_start_time,
    closest_in_time,
    find_product_for_safe,
    load_listing,
    entrypoint,
)


@pytest.fixture
def logger():
    return logging.getLogger("test_logger")


@pytest.fixture
def real_safe_id():
    return "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_049591_05F692_E123.SAFE"


@pytest.fixture
def mock_explode_safe():
    """Fixture qui retourne un mock de ExplodeSAFE avec attributs par défaut."""

    def _mock(**kwargs):
        inst = MagicMock()
        inst.startdate = kwargs.get("startdate", datetime(2023, 7, 26, 7, 11, 12))
        inst.mission_data_take = kwargs.get("mission_data_take", "05F692")
        inst.absolute_orbit_number = kwargs.get("absolute_orbit_number", 49591)
        inst.level = kwargs.get("level", "1")
        inst.polarisation = kwargs.get("polarisation", "DV")
        return inst

    return _mock


# Tests pour parse_start_time (inchangé)
@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_parse_start_time_valid(mock_explode):
    mock_inst = MagicMock()
    mock_inst.startdate = datetime(2023, 7, 26, 7, 11, 12)
    mock_explode.return_value = mock_inst
    dt = parse_start_time("S1A_ANY_STRING")
    assert dt == datetime(2023, 7, 26, 7, 11, 12)


@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_parse_start_time_invalid(mock_explode):
    mock_explode.side_effect = ValueError("Invalid")
    assert parse_start_time("INVALID_NAME") is None


# Test pour closest_in_time (utilise ExplodeSAFE maintenant)
def test_closest_in_time():
    ref_dt = datetime(2023, 1, 1, 12, 0, 0)

    def explode_side_effect(name):
        inst = MagicMock()
        if name == "P1":
            inst.startdate = datetime(2023, 1, 1, 12, 0, 10)
        elif name == "P2":
            inst.startdate = datetime(2023, 1, 1, 12, 0, 2)
        else:
            inst.startdate = None
        return inst

    with patch(
        "cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE",
        side_effect=explode_side_effect,
    ):
        best, delta = closest_in_time(ref_dt, [{"Name": "P1"}, {"Name": "P2"}])
        assert delta == 2
        assert best["Name"] == "P2"


# Tests pour find_product_for_safe (avec les nouveaux attributs)
@patch("requests.get")
@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_find_product_success_exact(
    mock_explode_class, mock_get, real_safe_id, logger, mock_explode_safe
):
    """Test de matching exact pour OCN (niveau 2)."""
    # Simuler l'instance ExplodeSAFE pour source_id
    mock_inst = mock_explode_safe(
        startdate=datetime(2023, 7, 26, 7, 11, 12),
        mission_data_take="05F692",
        absolute_orbit_number=49591,
        level="1",  # Source en niveau 1
        polarisation="DV",
    )
    mock_explode_class.return_value = mock_inst

    # Simuler la réponse OData - OCN est niveau 2
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "value": [
            {
                "Id": "u1",
                "Name": "S1A_IW_OCN__2SDV_20230726T071112_20230726T071137_049591_05F692_E123.SAFE",
                "ContentLength": 100,
            }
        ]
    }

    delta_dist = defaultdict(int)
    res = find_product_for_safe(real_safe_id, "OCN_", logger, delta_dist)
    assert res["match_method"] == "exact_timestamp"

    # Vérifier que le filtre a bien utilisé l'orbite et le datatake
    args, kwargs = mock_get.call_args
    filter_str = kwargs["params"]["$filter"]
    assert "49591_05F692" in filter_str

    # CORRECTION : Pour OCN, le niveau est 2, donc polarisation complète = 2SDV
    assert "2SDV" in filter_str, f"Expected '2SDV' in filter, got: {filter_str}"


@patch("requests.get")
@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_find_product_success_exact_grds(
    mock_explode_class, mock_get, real_safe_id, logger, mock_explode_safe
):
    """Test de matching exact pour GRDH (niveau 1)."""
    # Simuler l'instance ExplodeSAFE pour source_id
    mock_inst = mock_explode_safe(
        startdate=datetime(2023, 7, 26, 7, 11, 12),
        mission_data_take="05F692",
        absolute_orbit_number=49591,
        level="1",
        polarisation="DV",
    )
    mock_explode_class.return_value = mock_inst

    # Simuler la réponse OData pour GRDH
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "value": [
            {
                "Id": "u1",
                "Name": "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_049591_05F692_E123.SAFE",
                "ContentLength": 100,
            }
        ]
    }

    delta_dist = defaultdict(int)
    res = find_product_for_safe(real_safe_id, "GRDH", logger, delta_dist)
    assert res["match_method"] == "exact_timestamp"

    # Vérifier le filtre
    args, kwargs = mock_get.call_args
    filter_str = kwargs["params"]["$filter"]
    assert "49591_05F692" in filter_str
    # Pour GRDH, le niveau est 1, donc 1SDV
    assert "1SDV" in filter_str, f"Expected '1SDV' in filter, got: {filter_str}"


@patch("requests.get")
@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_find_product_success_closest(
    mock_explode_class, mock_get, real_safe_id, logger, mock_explode_safe
):
    """Test de matching par plus proche voisin."""
    # Source
    src_inst = mock_explode_safe(
        startdate=datetime(2023, 7, 26, 7, 11, 12),
        mission_data_take="05F692",
        absolute_orbit_number=49591,
        level="1",
        polarisation="DV",
    )

    # Pour les candidats, on va créer un mock qui retourne des startdate différents selon le nom
    def explode_side_effect(name):
        if name == real_safe_id:
            return src_inst
        # Pour les produits dans la réponse OData
        if "GRDH" in name:
            inst = MagicMock()
            inst.startdate = datetime(2023, 7, 26, 7, 11, 15)  # 3s delta
            return inst
        return MagicMock(startdate=None)

    mock_explode_class.side_effect = explode_side_effect

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "value": [
            {
                "Id": "u2",
                "Name": "S1A_IW_GRDH_1SDV_20230726T071115_...",
                "ContentLength": 100,
            }
        ]
    }

    delta_dist = defaultdict(int)
    with patch("cdsodatacli.scripts.match_s1_product_types.MAX_DELTA_SECONDS", 10):
        res = find_product_for_safe(real_safe_id, "GRDH", logger, delta_dist)
    assert res["match_method"] == "closest_in_time"
    assert delta_dist[3] == 1


@patch("requests.get")
@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_find_product_not_found(
    mock_explode_class, mock_get, real_safe_id, logger, mock_explode_safe
):
    mock_inst = mock_explode_safe()
    mock_explode_class.return_value = mock_inst

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"value": []}
    res = find_product_for_safe(real_safe_id, "OCN_", logger, defaultdict(int))
    assert res.get("status") == "not_found"
    assert "No OCN_ product found" in res["note"]


@patch("requests.get")
@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_find_product_delta_exceeds_threshold(
    mock_explode_class, mock_get, real_safe_id, logger, mock_explode_safe
):
    # Source
    src_inst = mock_explode_safe(startdate=datetime(2023, 7, 26, 7, 11, 12))

    # Candidat avec 20s de différence
    def explode_side_effect(name):
        if name == real_safe_id:
            return src_inst
        inst = MagicMock()
        inst.startdate = datetime(2023, 7, 26, 7, 11, 32)  # 20s
        return inst

    mock_explode_class.side_effect = explode_side_effect
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "value": [{"Id": "u3", "Name": "candidate", "ContentLength": 100}]
    }

    delta_dist = defaultdict(int)
    # MAX_DELTA_SECONDS = 8 (valeur par défaut)
    res = find_product_for_safe(real_safe_id, "GRDH", logger, delta_dist)
    assert res["status"] == "not_found"
    assert "20s away" in res["note"]


@patch("requests.get")
@patch("cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE")
def test_find_product_for_slc(
    mock_explode_class, mock_get, real_safe_id, logger, mock_explode_safe
):
    """Test spécifique pour SLC_ (niveau 1 également)."""
    mock_inst = mock_explode_safe(
        startdate=datetime(2023, 7, 26, 7, 11, 12),
        mission_data_take="05F692",
        absolute_orbit_number=49591,
        level="1",
        polarisation="DV",
    )
    mock_explode_class.return_value = mock_inst

    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "value": [
            {
                "Id": "u1",
                "Name": "S1A_IW_SLC__1SDV_20230726T071112_20230726T071137_049591_05F692_E123.SAFE",
                "ContentLength": 100,
            }
        ]
    }

    delta_dist = defaultdict(int)
    res = find_product_for_safe(real_safe_id, "SLC_", logger, delta_dist)
    assert res["match_method"] == "exact_timestamp"

    # Vérifier le filtre
    args, kwargs = mock_get.call_args
    filter_str = kwargs["params"]["$filter"]
    # SLC_ est aussi niveau 1
    assert "1SDV" in filter_str, f"Expected '1SDV' in filter, got: {filter_str}"


def test_load_listing(logger):
    with (
        patch("pathlib.Path.read_text", return_value="P1\nP2\n\nP3"),
        patch("pathlib.Path.exists", return_value=True),
    ):
        assert load_listing("d.txt", logger) == ["P1", "P2", "P3"]


@patch("cdsodatacli.scripts.match_s1_product_types.find_product_for_safe")
@patch("pathlib.Path.open", new_callable=mock_open)
def test_entrypoint_logic(mock_file, mock_find, logger):
    mock_find.return_value = {
        "source_id": "i",
        "target_name": "o",
        "match_method": "e",
        "size_mb": 1024.0,
    }
    entrypoint(["i"], "OCN_", "o.txt", logger)
    mock_file().write.assert_any_call("o\n")
