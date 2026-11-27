"""Tests of the parquet backend of match_s1_product_types (no network access)."""

import datetime
import logging
from collections import defaultdict
from pathlib import Path

import pandas as pd
import pytest

from cdsodatacli.scripts import match_s1_product_types as m

SLC = "S1A_IW_SLC__1SDV_20230726T071112_20230726T071139_049591_05F692_AAAA.SAFE"
GRD_EXACT = "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_049591_05F692_E123.SAFE"
# its stop time equals the SLC start time: must not be taken as an exact match
GRD_PREVIOUS = (
    "S1A_IW_GRDH_1SDV_20230726T071047_20230726T071112_049591_05F692_B456.SAFE"
)
GRD_COG = "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_049591_05F692_C789_COG.SAFE"
GRD_OTHER_DATATAKE = (
    "S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_049591_05F693_D012.SAFE"
)
SLC_CLOSEST = "S1A_IW_SLC__1SDV_20230726T080000_20230726T080027_049591_05F699_AAAB.SAFE"
GRD_CLOSEST = "S1A_IW_GRDH_1SDV_20230726T080002_20230726T080027_049591_05F699_E124.SAFE"
SLC_FAR = "S1A_IW_SLC__1SDV_20230726T090000_20230726T090027_049592_05F69A_AAAC.SAFE"
GRD_FAR = "S1A_IW_GRDH_1SDV_20230726T090030_20230726T090055_049592_05F69A_E125.SAFE"
SLC_NONE = "S1A_IW_SLC__1SDV_20230726T100000_20230726T100027_049592_05F69B_AAAD.SAFE"
# product close to midnight: its GRD is stored in the file of the next day
SLC_MIDNIGHT = (
    "S1B_IW_SLC__1SDH_20230726T235958_20230727T000025_049600_05F6A0_AAAE.SAFE"
)
GRD_MIDNIGHT = (
    "S1B_IW_GRDH_1SDH_20230727T000001_20230727T000026_049600_05F6A0_E126.SAFE"
)
OCN_EXACT = "S1A_IW_OCN__2SDV_20230726T071112_20230726T071137_049591_05F692_F001.SAFE"
# two versions of the same GRD: the most recently published one is expected
SLC_DUP = "S1A_IW_SLC__1SDV_20230726T110000_20230726T110027_049593_05F6B0_AAAF.SAFE"
GRD_DUP_OLD = "S1A_IW_GRDH_1SDV_20230726T110000_20230726T110025_049593_05F6B0_0001.SAFE"
GRD_DUP_NEW = "S1A_IW_GRDH_1SDV_20230726T110000_20230726T110025_049593_05F6B0_FFFF.SAFE"


def _row(name, pubdate="2023-07-26T12:00:00.000000Z"):
    return {
        "Id": f"id-{name[-9:-5]}",
        "Name": name,
        "ContentLength": str(2 * 1024**2),
        "PublicationDate": pubdate,
        "Attributes": "[]",
    }


@pytest.fixture
def logger():
    return logging.getLogger("test_match_parquet")


@pytest.fixture
def parquet_dir(tmp_path) -> Path:
    day1 = [
        _row(n)
        for n in [
            SLC,
            GRD_EXACT,
            GRD_PREVIOUS,
            GRD_COG,
            GRD_OTHER_DATATAKE,
            SLC_CLOSEST,
            GRD_CLOSEST,
            SLC_FAR,
            GRD_FAR,
            SLC_NONE,
            SLC_MIDNIGHT,
            OCN_EXACT,
            SLC_DUP,
        ]
    ]
    day1.append(_row(GRD_DUP_OLD, "2023-07-26T12:00:00.000000Z"))
    day1.append(_row(GRD_DUP_NEW, "2023-07-26T15:00:00.000000Z"))
    pd.DataFrame(day1).to_parquet(tmp_path / "s1_2023-07-26.parquet", index=False)
    pd.DataFrame([_row(GRD_MIDNIGHT)]).to_parquet(
        tmp_path / "s1_2023-07-27.parquet", index=False
    )
    return tmp_path


def _by_source(results):
    return {r["source_id"]: r for r in results}


def test_target_level():
    assert m.target_level("OCN_") == "2"
    assert m.target_level("RAW_") == "0"
    assert m.target_level("GRDH") == "1"
    assert m.target_level("SLC_") == "1"


def test_parse_safe_names():
    fields = m.parse_safe_names(pd.Series([GRD_EXACT, "not_a_safe"], dtype=object))
    assert fields.loc[0, "platform"] == "S1A"
    assert fields.loc[0, "product"] == "GRDH"
    assert fields.loc[0, "orbit"] == "049591"
    assert fields.loc[0, "datatake"] == "05F692"
    assert fields.loc[0, "polarisation"] == "DV"
    assert fields.loc[0, "start_dt"] == pd.Timestamp("2023-07-26T07:11:12")
    assert pd.isna(fields.loc[1, "start_dt"])


def test_daily_parquet_path():
    path = m.daily_parquet_path(Path("/data"), datetime.date(2023, 7, 26))
    assert path == Path("/data/s1_2023-07-26.parquet")


def test_find_products_in_parquet_grdh(parquet_dir, logger):
    sources = [SLC, SLC_CLOSEST, SLC_FAR, SLC_NONE, SLC_MIDNIGHT, SLC_DUP, "bad"]
    delta_dist = defaultdict(int)
    results = m.find_products_in_parquet(
        sources, "GRDH", parquet_dir, logger, delta_dist
    )
    # one result per source, in the input order
    assert [r["source_id"] for r in results] == sources
    res = _by_source(results)

    # exact start time, not the GRD whose stop time equals the SLC start,
    # COG and other datatake ignored
    assert res[SLC]["target_name"] == GRD_EXACT
    assert res[SLC]["match_method"] == "exact_timestamp"
    assert res[SLC]["target_id"] == "id-E123"
    assert res[SLC]["size_mb"] == 2.0
    assert res[SLC]["download_url"].endswith("Products(id-E123)/$value")

    assert res[SLC_CLOSEST]["target_name"] == GRD_CLOSEST
    assert res[SLC_CLOSEST]["match_method"] == "closest_in_time"

    assert res[SLC_FAR]["status"] == "not_found"
    assert "30s away" in res[SLC_FAR]["note"]

    assert res[SLC_NONE]["status"] == "not_found"
    assert "No GRDH product found for DataTake 05F69B" in res[SLC_NONE]["note"]

    # target stored in the daily file of the next day
    assert res[SLC_MIDNIGHT]["target_name"] == GRD_MIDNIGHT
    assert res[SLC_MIDNIGHT]["match_method"] == "closest_in_time"

    assert res[SLC_DUP]["target_name"] == GRD_DUP_NEW

    assert res["bad"]["status"] == "error"

    assert delta_dist[0] == 2
    assert delta_dist[2] == 1
    assert delta_dist[3] == 1
    assert delta_dist[30] == 1


def test_find_products_in_parquet_ocn(parquet_dir, logger):
    results = m.find_products_in_parquet(
        [SLC, SLC_CLOSEST], "OCN_", parquet_dir, logger, defaultdict(int)
    )
    res = _by_source(results)
    assert res[SLC]["target_name"] == OCN_EXACT
    assert res[SLC_CLOSEST]["status"] == "not_found"


def test_find_products_in_parquet_reverse_direction(parquet_dir, logger):
    """GRD -> SLC works the same way."""
    results = m.find_products_in_parquet(
        [GRD_EXACT, GRD_MIDNIGHT], "SLC_", parquet_dir, logger, defaultdict(int)
    )
    res = _by_source(results)
    assert res[GRD_EXACT]["target_name"] == SLC
    assert res[GRD_MIDNIGHT]["target_name"] == SLC_MIDNIGHT


def test_find_products_in_parquet_missing_day(tmp_path, logger):
    results = m.find_products_in_parquet(
        [SLC], "GRDH", tmp_path, logger, defaultdict(int)
    )
    assert results[0]["status"] == "error"
    assert "Missing daily parquet file(s): 2023-07-26" in results[0]["note"]


def test_get_parquet_dir_from_config(tmp_path):
    cfg = tmp_path / "conf.yml"
    cfg.write_text(f"s1_daily_metadata_parquet_dir: {tmp_path}\n")
    assert m.get_parquet_dir(str(cfg)) == tmp_path


def test_get_parquet_dir_missing_key(tmp_path):
    cfg = tmp_path / "conf.yml"
    cfg.write_text("other_key: 1\n")
    with pytest.raises(KeyError, match="s1_daily_metadata_parquet_dir"):
        m.get_parquet_dir(str(cfg))


def test_entrypoint_parquet_backend(parquet_dir, tmp_path, logger):
    cfg = tmp_path / "conf.yml"
    cfg.write_text(f"s1_daily_metadata_parquet_dir: {parquet_dir}\n")
    output = tmp_path / "out.txt"
    results = m.entrypoint(
        [SLC, SLC_NONE],
        "GRDH",
        str(output),
        logger,
        checkpoint_dir=str(tmp_path / "ckpt"),
        backend="parquet",
        config_file=str(cfg),
    )
    assert len(results) == 2
    lines = output.read_text().splitlines()
    assert lines[0] == GRD_EXACT
    assert lines[1].startswith(f"# NOT_FOUND: {SLC_NONE}")
    checkpoint = tmp_path / "ckpt" / "out_checkpoint.txt"
    assert set(checkpoint.read_text().split()) == {SLC, SLC_NONE}


def test_entrypoint_parquet_dir_not_found(tmp_path, logger):
    with pytest.raises(FileNotFoundError):
        m.entrypoint(
            [SLC],
            "GRDH",
            str(tmp_path / "out.txt"),
            logger,
            backend="parquet",
            parquet_dir=tmp_path / "does_not_exist",
        )


def test_entrypoint_invalid_backend(tmp_path, logger):
    with pytest.raises(ValueError, match="backend"):
        m.entrypoint([SLC], "GRDH", str(tmp_path / "o.txt"), logger, backend="x")
