import pytest
import pandas as pd
import datetime
from unittest.mock import patch, MagicMock, mock_open
from collections import defaultdict

import cdsodatacli.download as dl


@pytest.fixture
def mock_conf():
    """Matches the provided YAML structure."""
    return {
        "URL_identity": "https://identity.fake/token",
        "URL_download": "https://zipper.fake/odata/v1/Products(%s)/$value",
        "spools": {"default": "/tmp/my_spool"},
        "pre_spool": "/tmp/pre_spool",
        "archives": {"default": "/tmp/my_archive", "sentinel1": "/tmp/archive/s1"},
        "token_directory": "/tmp/tokens",
        "active_session_directory": "/tmp/sessions",
        "logins": [{"user1@email.fr": "passwd1"}],
        "default_login": {"user1@email.fr": "passwd1"},
    }


# 1. Test filtering logic
def test_filter_product_already_present(mock_conf):
    df = pd.DataFrame(
        {
            "safe": [
                "S1A_IW_GRDH_1SDV_20240101T120000_20240101T120025_052000_064000_1234",  # ARCHIVE
                "S1A_IW_GRDH_1SDV_20240102T120000_20240102T120025_052001_064001_1235",  # SPOOL
                "S1A_IW_GRDH_1SDV_20240103T120000_20240103T120025_052002_064002_1236",  # MISSING
            ],
            "id": ["id1", "id2", "id3"],
        }
    )
    cpt = defaultdict(int)

    s1_archive = "S1A_IW_GRDH_1SDV_20240101T120000_20240101T120025_052000_064000_1234"
    s1_spool = "S1A_IW_GRDH_1SDV_20240102T120000_20240102T120025_052001_064001_1235"
    s1_missing = "S1A_IW_GRDH_1SDV_20240103T120000_20240103T120025_052002_064002_1236"

    archive_map = {
        s1_archive: (True, "/home/archive/S1A_IW_GRDH_1SDV_20240101T120000.zip"),
        s1_spool: (False, None),
        s1_missing: (False, None),
    }
    spool_map = {
        s1_archive: False,
        s1_spool: True,
        s1_missing: False,
    }

    # Change the patch targets from utils to download
    with (
        patch(
            "cdsodatacli.download.check_safe_in_archive",
            side_effect=lambda safename, conf: archive_map.get(safename, (False, None)),
        ) as mock_archive,
        patch(
            "cdsodatacli.download.check_safe_in_spool",
            side_effect=lambda safename, conf: spool_map.get(safename, False),
        ),
        patch("cdsodatacli.download.check_safe_in_outputdir", return_value=False),
    ):

        df_to_dl, updated_cpt = dl.filter_product_already_present(
            cpt, df, "/tmp/out", mock_conf
        )

        assert mock_archive.call_count == 3
        assert len(df_to_dl) == 1
        assert df_to_dl["safe"].iloc[0] == s1_missing
        assert updated_cpt["preproc-archived_product"] == 1
        assert updated_cpt["preproc-in_spool_product"] == 1


# 2. Test core download function
@patch("cdsodatacli.download.get_conf")
@patch("os.path.exists", return_value=True)  # <-- ajout
@patch("shutil.copy2")  # <-- remplace subprocess.check_output
@patch("os.remove")
@patch("os.chmod")
def test_CDS_Odata_download_one_product_v2_success(
    mock_chmod, mock_remove, mock_copy2, mock_exists, mock_get_conf, mock_conf, tmp_path
):
    mock_get_conf.return_value = mock_conf
    session = MagicMock()
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.reason = "OK"
    mock_response.headers = {"content-length": "2000000"}
    mock_response.iter_content.return_value = [b"chunk1"]
    session.get.return_value = mock_response

    output_path = str(tmp_path / "test.zip")

    with patch("builtins.open", mock_open()):
        speed, meaning, name, sem = dl.CDS_Odata_download_one_product_v2(
            session, {}, "http://url", output_path, "sem_file"
        )

    assert meaning == "OK"
    assert speed > 0
    mock_copy2.assert_called_once()  # vérifie que le move a bien eu lieu
    mock_chmod.assert_called_once()


# 3. Test Metadata generation
def test_add_missing_cdse_hash_ids_in_listing(tmp_path):
    listing = tmp_path / "list.txt"
    listing.write_text("S1A_IW_GRDH_1SDV_20220503T000000.SAFE")

    mock_exploded = MagicMock()
    mock_exploded.startdate = pd.Timestamp("2022-05-03")
    mock_exploded.enddate = pd.Timestamp("2022-05-03")
    mock_exploded.mode = "IW"
    mock_exploded.product = "GRDH"

    mock_query_result = pd.DataFrame(
        {"Id": ["uuid-123"], "Name": ["S1A_IW_GRDH_1SDV_20220503T000000.SAFE"]}
    )

    with (
        patch("cdsodatacli.download.ExplodeSAFE", return_value=mock_exploded),
        patch("cdsodatacli.download.fetch_data", return_value=mock_query_result),
    ):

        res = dl.add_missing_cdse_hash_ids_in_listing(str(listing))
        assert not res.empty
        assert res["id"].iloc[0] == "uuid-123"


# 4. Sequential Download
@patch("cdsodatacli.download.get_conf")
@patch("cdsodatacli.download.get_sessions_download_available")
@patch("cdsodatacli.download.CDS_Odata_download_one_product_v2")
@patch("cdsodatacli.download.get_bearer_access_token")
def test_download_list_product_sequential(
    mock_token, mock_dl_one, mock_sessions, mock_get_conf, mock_conf
):
    mock_get_conf.return_value = mock_conf
    fake_sem = "/tmp/type_group_status_user1_20240101t120000.txt"

    # Mocking token refresh
    mock_token.return_value = ("token", datetime.datetime.now(), "user1", fake_sem)

    mock_sessions.return_value = pd.DataFrame(
        {
            "url": ["url1"],
            "session": [MagicMock()],
            "session_semaphore": [fake_sem],
            "header": [{"Auth": "Bearer"}],
            "token_semaphore": [fake_sem],
            "output_path": ["/tmp/out.zip"],
            "safe": ["SAFE1"],
        }
    )

    mock_dl_one.return_value = (10.0, "OK", "SAFE1", fake_sem)

    with (
        patch("cdsodatacli.download.remove_semaphore_token_file"),
        patch("cdsodatacli.download.remove_semaphore_session_file"),
        patch("cdsodatacli.download.filter_product_already_present") as mock_filter,
    ):

        mock_filter.return_value = (
            pd.DataFrame(
                {
                    "safe": ["SAFE1"],
                    "id": ["id1"],
                    "outputpath": ["/tmp/SAFE1.zip"],
                    "urls": ["url1"],
                }
            ),
            defaultdict(int),
        )

        res = dl.download_list_product_sequential(["id1"], ["SAFE1"], "/tmp/out")
        assert res["status"].iloc[0] == 1


# 5. Multi-thread Orchestrator
@patch("cdsodatacli.download.get_conf")
@patch("cdsodatacli.download.get_sessions_download_available")
@patch("cdsodatacli.download.CDS_Odata_download_one_product_v2")
def test_download_list_product_multithread_v2(
    mock_dl_one, mock_sessions, mock_get_conf, mock_conf
):
    mock_get_conf.return_value = mock_conf
    fake_sem = "/tmp/type_group_status_user1_20240101t120000.txt"

    mock_sessions.return_value = pd.DataFrame(
        {
            "url": ["url1"],
            "session": [MagicMock()],
            "header": [{}],
            "output_path": ["p1.zip"],
            "token_semaphore": [fake_sem],
            "safe": ["SAFE1"],
        }
    )

    mock_dl_one.return_value = (5.0, "OK", "SAFE1", fake_sem)

    with (
        patch("cdsodatacli.download.remove_semaphore_token_file"),
        patch("cdsodatacli.download.remove_semaphore_session_file"),
        patch("cdsodatacli.download.filter_product_already_present") as mock_filter,
    ):

        mock_filter.return_value = (
            pd.DataFrame({"safe": ["SAFE1"], "status": [0], "id": ["id1"]}),
            defaultdict(int),
        )

        res = dl.download_list_product_multithread_v2(["id1"], ["SAFE1"], "/tmp/out")
        assert res["status"].iloc[0] == 1
