import pytest
from unittest.mock import patch, MagicMock
from collections import defaultdict
from cdsodatacli.scripts.match_s1_product_types import (
    find_product_for_safe,
    ExplodeSAFE,
)

# Paires (source, target) extraites de vos listings
PAIRS = [
    (
        "S1A_IW_SLC__1SDV_20191031T160736_20191031T160803_029705_036271_B837.SAFE",
        "S1A_IW_GRDH_1SDV_20191031T160737_20191031T160802_029705_036271_C7BE.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20191031T160800_20191031T160828_029705_036271_BC70.SAFE",
        "S1A_IW_GRDH_1SDV_20191031T160802_20191031T160827_029705_036271_733E.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20191031T160825_20191031T160853_029705_036271_195C.SAFE",
        "S1A_IW_GRDH_1SDV_20191031T160821_20191031T160846_029705_036271_7D13.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20191031T160851_20191031T160920_029705_036271_E63C.SAFE",
        "S1A_IW_GRDH_1SDV_20191031T160852_20191031T160920_029705_036271_F602.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20200228T160732_20200228T160759_031455_039F26_DAA5.SAFE",
        "S1A_IW_GRDH_1SDV_20200228T160733_20200228T160758_031455_039F26_E5E1.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20200228T160757_20200228T160824_031455_039F26_7DF8.SAFE",
        "S1A_IW_GRDH_1SDV_20200228T160758_20200228T160823_031455_039F26_259B.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20200228T160822_20200228T160849_031455_039F26_6A8A.SAFE",
        "S1A_IW_GRDH_1SDV_20200228T160823_20200228T160848_031455_039F26_5CE5.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20200228T160847_20200228T160916_031455_039F26_62FE.SAFE",
        "S1A_IW_GRDH_1SDV_20200228T160848_20200228T160916_031455_039F26_D7BD.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20200327T153538_20200327T153605_031863_03AD7B_A07C.SAFE",
        "S1A_IW_GRDH_1SDV_20200327T153539_20200327T153604_031863_03AD7B_57B4.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20201130T160831_20201130T160858_035480_0425C7_F191.SAFE",
        "S1A_IW_GRDH_1SDV_20201130T160832_20201130T160857_035480_0425C7_120E.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20221213T152659_20221213T152726_046315_058C05_70F1.SAFE",
        "S1A_IW_GRDH_1SDV_20221213T152700_20221213T152725_046315_058C05_9A15.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20221213T152724_20221213T152751_046315_058C05_9AE8.SAFE",
        "S1A_IW_GRDH_1SDV_20221213T152725_20221213T152750_046315_058C05_BB90.SAFE",
    ),
    (
        "S1A_IW_SLC__1SDV_20221213T152749_20221213T152816_046315_058C05_E3DA.SAFE",
        "S1A_IW_GRDH_1SDV_20221213T152750_20221213T152815_046315_058C05_BC6E.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDV_20191209T154254_20191209T154321_019290_0246C2_4647.SAFE",
        "S1B_IW_GRDH_1SDV_20191209T154229_20191209T154254_019290_0246C2_1CB1.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDV_20200316T152658_20200316T152723_020719_027475_9AE4.SAFE",
        "S1B_IW_GRDH_1SDV_20200316T152659_20200316T152723_020719_027475_5A1E.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDH_20191209T154458_20191209T154525_019290_0246C3_B217.SAFE",
        "S1B_IW_GRDH_1SDH_20191209T154459_20191209T154524_019290_0246C3_D0C6.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDH_20191209T154432_20191209T154500_019290_0246C3_4B44.SAFE",
        "S1B_IW_GRDH_1SDH_20191209T154434_20191209T154459_019290_0246C3_4C9C.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDH_20191209T154405_20191209T154434_019290_0246C3_C609.SAFE",
        "S1B_IW_GRDH_1SDH_20191209T154405_20191209T154434_019290_0246C3_4F49.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDV_20191209T154343_20191209T154358_019290_0246C2_4284.SAFE",
        "S1B_IW_GRDH_1SDV_20191209T154344_20191209T154358_019290_0246C2_015C.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDV_20191209T154319_20191209T154346_019290_0246C2_1A5F.SAFE",
        "S1B_IW_GRDH_1SDV_20191209T154254_20191209T154319_019290_0246C2_B84F.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDV_20191209T154254_20191209T154321_019290_0246C2_4647.SAFE",
        "S1B_IW_GRDH_1SDV_20191209T154229_20191209T154254_019290_0246C2_1CB1.SAFE",
    ),
    (
        "S1B_IW_SLC__1SDV_20191209T154228_20191209T154256_019290_0246C2_8013.SAFE",
        "S1B_IW_GRDH_1SDV_20191209T154229_20191209T154254_019290_0246C2_1CB1.SAFE",
    ),
]


@pytest.fixture
def logger():
    import logging

    return logging.getLogger("test")


@pytest.mark.parametrize("source_id, expected_target", PAIRS)
@patch("requests.get")
def test_slc_to_grdh_match(mock_get, source_id, expected_target, logger):
    with patch("cdsodatacli.scripts.match_s1_product_types.MAX_DELTA_SECONDS", 3600):
        # Extraire les infos du source pour vérifier le filtre
        src_inst = ExplodeSAFE(source_id)

        # Réponse mock : un seul produit, celui attendu
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {
                    "Id": "mock_id",
                    "Name": expected_target,
                    "ContentLength": 1000,
                }
            ]
        }
        mock_get.return_value = mock_response

        delta_dist = defaultdict(int)
        result = find_product_for_safe(source_id, "GRDH", logger, delta_dist)

        # Vérifier que le résultat est un succès (présence de target_name)
        assert "target_name" in result, f"Expected success but got {result}"
        assert result["target_name"] == expected_target

        # Vérifier la construction du filtre OData
        args, kwargs = mock_get.call_args
        filter_str = kwargs["params"]["$filter"]
        assert (
            f"{src_inst.absolute_orbit_number}_{src_inst.mission_data_take}"
            in filter_str
        )
        assert f"_{src_inst.level}S{src_inst.polarisation}" in filter_str
        assert "not contains(Name,'_COG')" in filter_str
