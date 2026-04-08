import pytest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
from collections import defaultdict
import logging

# Import from the script
from cdsodatacli.scripts.match_s1_product_types import (
    parse_start_time,
    closest_in_time,
    find_product_for_safe,
    load_listing,
    entrypoint
)

@pytest.fixture
def logger():
    return logging.getLogger("test_logger")

@pytest.fixture
def real_safe_id():
    return "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_049591_05F692_E123.SAFE"

@patch('cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE')
def test_parse_start_time_valid(mock_explode):
    mock_inst = MagicMock()
    mock_inst.startdate = datetime(2023, 7, 26, 7, 11, 12)
    mock_explode.return_value = mock_inst
    dt = parse_start_time("S1A_ANY_STRING")
    assert dt == datetime(2023, 7, 26, 7, 11, 12)

@patch('cdsodatacli.scripts.match_s1_product_types.ExplodeSAFE')
def test_parse_start_time_invalid(mock_explode):
    mock_explode.side_effect = ValueError("Invalid")
    assert parse_start_time("INVALID_NAME") is None

def test_closest_in_time():
    ref_dt = datetime(2023, 1, 1, 12, 0, 0)
    def side_effect_logic(name):
        if name == "P1": return datetime(2023, 1, 1, 12, 0, 10)
        if name == "P2": return datetime(2023, 1, 1, 12, 0, 2)
        return None
    with patch('cdsodatacli.scripts.match_s1_product_types.parse_start_time', side_effect=side_effect_logic):
        best, delta = closest_in_time(ref_dt, [{"Name": "P1"}, {"Name": "P2"}])
        assert delta == 2
        assert best["Name"] == "P2"

@patch('requests.get')
@patch('cdsodatacli.scripts.match_s1_product_types.parse_start_time')
def test_find_product_success_exact(mock_parse, mock_get, real_safe_id, logger):
    mock_parse.return_value = datetime(2023, 7, 26, 7, 11, 12)
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "value": [{"Id": "u1", "Name": "S1A_IW_OCN__1SDV_20230726T071112_...", "ContentLength": 100}]
    }
    res = find_product_for_safe(real_safe_id, "OCN_", logger, defaultdict(int))
    assert res["match_method"] == "exact_timestamp"

@patch('requests.get')
@patch('cdsodatacli.scripts.match_s1_product_types.parse_start_time')
def test_find_product_success_closest(mock_parse, mock_get, real_safe_id, logger):
    def side_effect_logic(name):
        if "GRDH" in name: return datetime(2023, 7, 26, 7, 11, 12)
        return datetime(2023, 7, 26, 7, 11, 15) # 3s delta
    mock_parse.side_effect = side_effect_logic
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "value": [{"Id": "u2", "Name": "S1A_IW_OCN__1SDV_20230726T071115_...", "ContentLength": 100}]
    }
    delta_dist = defaultdict(int)
    res = find_product_for_safe(real_safe_id, "OCN_", logger, delta_dist)
    assert res["match_method"] == "closest_in_time"
    assert delta_dist[3] == 1

@patch('requests.get')
def test_find_product_not_found(mock_get, real_safe_id, logger):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"value": []}
    res = find_product_for_safe(real_safe_id, "OCN_", logger, defaultdict(int))
    assert res.get("status") == "not_found"

def test_load_listing(logger):
    with patch("pathlib.Path.read_text", return_value="P1\nP2"), \
         patch("pathlib.Path.exists", return_value=True):
        assert load_listing("d.txt", logger) == ["P1", "P2"]

@patch('cdsodatacli.scripts.match_s1_product_types.find_product_for_safe')
@patch('pathlib.Path.open', new_callable=mock_open)
def test_entrypoint_logic(mock_file, mock_find, logger):
    mock_find.return_value = {"source_id": "i", "target_name": "o", "match_method": "e",
                               "size_mb": 1024.0}
    entrypoint(["i"], "OCN_", "o.txt", logger)
    mock_file().write.assert_any_call("o\n")