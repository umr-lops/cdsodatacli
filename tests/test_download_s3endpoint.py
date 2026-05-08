"""
pytest unit tests for:
  - get_a_free_s3_session
  - get_sessions_download_available_s3
  - cds_s3_download_one_product (S3 credentials version)
  - download_list_product_multithread_v4

Run with: pytest test_download_s3endpoint.py -v
"""

import os
import time
import threading
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock, call, PropertyMock
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED
from botocore.exceptions import BotoCoreError, ClientError
from collections import defaultdict


# ---------------------------------------------------------------------------
# Shared constants - Config with nested credential structure
# ---------------------------------------------------------------------------

FAKE_CONF = {
    "token_directory": "/fake/token_dir",
    "active_session_directory": "/fake/session_dir",
    "pre_spool": "/fake/pre_spool",
    "spool": "/fake/spool",
    "s3_bucket": "eodata",
    "s3_endpoint": "https://eodata.dataspace.copernicus.eu",
    "s3_region": "default",
    "URL_download": "https://fake.cdse/odata/v1/Products(%s)/$value",
    "logins": {
        "user1@example.fr": {
            "cdse-psswd": "passwd1",
            "s3-access-key": "AKIA_FAKE_KEY_1",
            "s3-secret": "FAKE_SECRET_1_LONG_STRING",
        },
        "user2@example.fr": {
            "cdse-psswd": "passwd2",
            "s3-access-key": "AKIA_FAKE_KEY_2",
            "s3-secret": "FAKE_SECRET_2_LONG_STRING",
        },
    },
}

FAKE_S3_PATH = "Sentinel-1/SAR/GRD/2022/05/03/S1A_IW_GRDH_1SDV_20220503T000000.SAFE"
FAKE_SAFENAME = "S1A_IW_GRDH_1SDV_20220503T000000"
FAKE_LOGIN = "user1@example.fr"
FAKE_ACCESS_KEY = FAKE_CONF["logins"][FAKE_LOGIN]["s3-access-key"]
FAKE_SECRET = FAKE_CONF["logins"][FAKE_LOGIN]["s3-secret"]
FAKE_S3_CREDENTIALS = {"s3-access-key": FAKE_ACCESS_KEY, "s3-secret": FAKE_SECRET}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_s3_object(key, size=1_000_000):
    """Return a MagicMock mimicking a boto3 S3 ObjectSummary."""
    obj = MagicMock()
    obj.key = key
    obj.size = size
    return obj


def make_s3_result(
    safename=FAKE_SAFENAME,
    status="Downloaded",
    speed=50.0,
    elapsed=20.0,
    total_mb=500.0,
):
    """Return a tuple matching cds_s3_download_one_product's return signature."""
    return speed, elapsed, total_mb, status, safename


def make_inputdf_for_sessions(safenames, s3paths=None, statuses=None):
    """
    Build input DataFrame for get_sessions_download_available_s3.
    
    Required columns: safe, urls, S3Path, outputpath, status
    """
    n = len(safenames)
    if s3paths is None:
        s3paths = [f"Sentinel-1/SAR/GRD/2022/05/03/{s}.SAFE" for s in safenames]
    if statuses is None:
        statuses = np.zeros(n)
    return pd.DataFrame(
        {
            "safe": safenames,  # NOTE: "safe" not "safename"
            "S3Path": s3paths,
            "urls": [f"https://fake.url/{s}" for s in safenames],
            "id": [f"id-{i}" for i in range(n)],
            "status": statuses,
            "outputpath": [f"/fake/spool/{s}" for s in safenames],  # NOTE: "outputpath" not "output_path"
        }
    )


def make_inputdf_for_v4(safenames, s3paths=None, statuses=None):
    """
    Build input DataFrame for download_list_product_multithread_v4.
    
    Required columns: safename, S3Path, id, status
    """
    n = len(safenames)
    if s3paths is None:
        s3paths = [f"Sentinel-1/SAR/GRD/2022/05/03/{s}.SAFE" for s in safenames]
    if statuses is None:
        statuses = np.zeros(n)
    return pd.DataFrame(
        {
            "safename": safenames,  # NOTE: "safename" for v4 input
            "S3Path": s3paths,
            "id": [f"id-{i}" for i in range(n)],
            "status": statuses,
        }
    )


def make_mock_future(result=None, exception=None):
    """Create a mock Future object for concurrency testing."""
    future = MagicMock(spec=Future)
    future.result.return_value = result
    future.exception.return_value = exception
    future.done.return_value = True
    return future


def make_active_sessions_status(logins=None, max_sessions=4):
    """Create active_s3_sessions_status dict structure."""
    if logins is None:
        logins = list(FAKE_CONF["logins"].keys())
    return {
        login: {session_id: False for session_id in range(max_sessions)}
        for login in logins
    }


# ---------------------------------------------------------------------------
# Fixtures (autouse)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def patch_conf():
    """Patch get_conf to return FAKE_CONF with proper nested credential structure."""
    with patch("cdsodatacli.download.get_conf", return_value=FAKE_CONF):
        yield


@pytest.fixture(autouse=True)
def patch_semaphores():
    with patch("cdsodatacli.download.remove_semaphore_session_file"):
        yield


@pytest.fixture(autouse=True)
def patch_tqdm():
    with patch("cdsodatacli.download.tqdm", side_effect=lambda *a, **kw: MagicMock()):
        yield


@pytest.fixture(autouse=True)
def patch_filter():
    """Default: all products need downloading (status=0)."""
    with patch("cdsodatacli.download.filter_product_already_present") as mock:

        def _default(
            cpt, df, outputdir, force_download, cdsodatacli_conf, extension=""
        ):
            cpt["product_absent_from_local_disks"] = len(df)
            result = df.copy()
            # Rename safename -> safe for internal processing
            if "safename" in result.columns:
                result = result.rename(columns={"safename": "safe"})
            result["outputpath"] = [f"/fake/spool/{s}" for s in result["safe"]]
            return result, cpt

        mock.side_effect = _default
        yield mock


@pytest.fixture(autouse=True)
def patch_requests_delete():
    """Prevent real HTTP calls for credential cleanup."""
    mock_resp = MagicMock()
    mock_resp.status_code = 204
    with patch("cdsodatacli.download.requests.delete", return_value=mock_resp) as m:
        yield m


@pytest.fixture
def patch_session_lock():
    """Mock the threading lock used in session management."""
    with patch("cdsodatacli.session._session_s3_lock"):
        yield


@pytest.fixture
def patch_get_credentials():
    """Mock get_a_credentials_from_conf_file to return credentials from FAKE_CONF."""
    def _get_creds(conf, account_group, login):
        return conf[account_group].get(login, {})
    
    with patch("cdsodatacli.session.get_a_credentials_from_conf_file", side_effect=_get_creds):
        yield


@pytest.fixture
def patch_threadpool_executor():
    """Mock ThreadPoolExecutor to control concurrency in tests."""
    with patch("cdsodatacli.download.ThreadPoolExecutor") as mock_executor_cls:
        mock_executor = MagicMock(spec=ThreadPoolExecutor)
        mock_executor_cls.return_value.__enter__.return_value = mock_executor
        mock_executor.submit = MagicMock()
        yield mock_executor_cls, mock_executor


@pytest.fixture
def patch_concurrent_wait():
    """Mock concurrent.futures.wait for deterministic testing."""
    with patch("cdsodatacli.download.wait") as mock_wait:
        yield mock_wait


# ---------------------------------------------------------------------------
# Import after fixtures so module-level calls don't fail
# ---------------------------------------------------------------------------

from cdsodatacli.download import (  # noqa: E402
    cds_s3_download_one_product,
    download_list_product_multithread_v4,
)
from cdsodatacli.session import (  # noqa: E402
    get_a_free_s3_session,
    get_sessions_download_available_s3,
)


# ===========================================================================
# Part 1 — get_a_free_s3_session unit tests
# ===========================================================================


class TestGetAFreeS3Session:
    """Tests for the session allocation helper."""

    @pytest.fixture(autouse=True)
    def setup_mocks(self, patch_session_lock, patch_get_credentials):
        """Ensure lock and credential helper are mocked for all tests."""
        yield

    def test_returns_first_free_session(self):
        """Should return session 0 when all are free."""
        active_sessions = make_active_sessions_status(["user1@example.fr"])
        
        result_sessions, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],  # Pass empty list, NOT None
        )
        
        assert session_id == 0
        assert login == "user1@example.fr"
        assert creds["s3-access-key"] == FAKE_ACCESS_KEY
        assert creds["s3-secret"] == FAKE_SECRET
        # Session should now be marked as active
        assert result_sessions["user1@example.fr"][0] is True

    def test_marks_session_as_active(self):
        """Returned session should be marked True in the status dict."""
        active_sessions = make_active_sessions_status(["user1@example.fr"])
        
        result_sessions, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )
        
        assert result_sessions["user1@example.fr"][session_id] is True
        # Original should be modified in-place (dict passed by reference)
        assert active_sessions["user1@example.fr"][0] is True

    def test_skips_blacklisted_accounts(self):
        """Blacklisted accounts should not be selected."""
        active_sessions = make_active_sessions_status(["user1@example.fr", "user2@example.fr"])
        
        result_sessions, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=["user1@example.fr"],  # Blacklist user1
        )
        
        assert login == "user2@example.fr"
        assert creds["s3-access-key"] == FAKE_CONF["logins"]["user2@example.fr"]["s3-access-key"]

    def test_returns_none_when_all_sessions_busy(self):
        """Should return None when no free sessions available."""
        active_sessions = make_active_sessions_status(["user1@example.fr"])
        # Mark all 4 sessions as busy
        for sid in range(4):
            active_sessions["user1@example.fr"][sid] = True
        
        result_sessions, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )
        
        assert session_id is None
        assert login is None
        assert creds == {}

    def test_round_robin_across_accounts(self):
        """Should cycle through accounts when selecting free sessions."""
        active_sessions = make_active_sessions_status(["user1@example.fr", "user2@example.fr"])
        
        # First call: should get user1 session 0
        _, sid1, login1, creds1 = get_a_free_s3_session(
            active_sessions,  # Pass same dict - will be modified in-place
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )
        assert login1 == "user1@example.fr"
        assert sid1 == 0
        
        # user1 session 0 is now busy, get next available
        _, sid2, login2, creds2 = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )
        # Should get user1 session 1 (next free in same account)
        assert login2 == "user1@example.fr"
        assert sid2 == 1

    def test_extracts_credentials_from_nested_config(self):
        """Verify credentials are pulled from conf[login][s3-access-key]."""
        active_sessions = make_active_sessions_status(["user2@example.fr"])
        
        _, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )
        
        expected_creds = FAKE_CONF["logins"]["user2@example.fr"]
        assert creds["s3-access-key"] == expected_creds["s3-access-key"]
        assert creds["s3-secret"] == expected_creds["s3-secret"]


# ===========================================================================
# Part 2 — get_sessions_download_available_s3 unit tests
# ===========================================================================


class TestGetSessionsDownloadAvailableS3:
    """Tests for the S3 session availability checker."""

    @pytest.fixture(autouse=True)
    def setup_mocks(self, patch_session_lock, patch_get_credentials):
        yield

    def test_returns_dataframe_with_credentials(self):
        """Output DataFrame should include s3_access_key and s3_secret columns."""
        subset = make_inputdf_for_sessions(["SAFE_A", "SAFE_B"])
        active_sessions = make_active_sessions_status()
        
        df_ready, updated_sessions = get_sessions_download_available_s3(
            conf=FAKE_CONF,
            active_s3_sessions_status=active_sessions,
            subset_to_treat=subset,
            blacklist=[],
            logins_group="logins",
        )
        
        assert isinstance(df_ready, pd.DataFrame)
        assert "s3_access_key" in df_ready.columns
        assert "s3_secret" in df_ready.columns
        assert "s3_session" in df_ready.columns
        assert "login" in df_ready.columns
        assert "S3Path" in df_ready.columns
        assert "output_path" in df_ready.columns
        assert "safe" in df_ready.columns

    def test_limits_to_available_sessions(self):
        """Should not return more products than free sessions."""
        # Only 2 free sessions available (mark 2 as busy)
        active_sessions = make_active_sessions_status(["user1@example.fr"])
        active_sessions["user1@example.fr"][2] = True
        active_sessions["user1@example.fr"][3] = True
        
        subset = make_inputdf_for_sessions(["SAFE_A", "SAFE_B", "SAFE_C", "SAFE_D"])
        
        df_ready, updated_sessions = get_sessions_download_available_s3(
            conf=FAKE_CONF,
            active_s3_sessions_status=active_sessions,
            subset_to_treat=subset,
            blacklist=[],
            logins_group="logins",
        )
        
        # Should only return up to 2 products (free sessions)
        assert len(df_ready) <= 2

    def test_marks_sessions_as_active_in_returned_status(self):
        """Returned active_s3_sessions_status should show allocated sessions as True."""
        subset = make_inputdf_for_sessions(["SAFE_A"])
        active_sessions = make_active_sessions_status()
        
        df_ready, updated_sessions = get_sessions_download_available_s3(
            conf=FAKE_CONF,
            active_s3_sessions_status=active_sessions,
            subset_to_treat=subset,
            blacklist=[],
            logins_group="logins",
        )
        
        # At least one session should be marked active
        found_active = any(
            updated_sessions[login][sid] is True
            for login in updated_sessions
            for sid in updated_sessions[login]
        )
        assert found_active

    def test_credentials_match_config_for_each_login(self):
        """Each row's credentials should match the login's config entry."""
        subset = make_inputdf_for_sessions(["SAFE_A", "SAFE_B"])
        active_sessions = make_active_sessions_status()
        
        df_ready, _ = get_sessions_download_available_s3(
            conf=FAKE_CONF,
            active_s3_sessions_status=active_sessions,
            subset_to_treat=subset,
            blacklist=[],
            logins_group="logins",
        )
        
        for _, row in df_ready.iterrows():
            login = row["login"]
            expected_creds = FAKE_CONF["logins"][login]
            assert row["s3_access_key"] == expected_creds["s3-access-key"]
            assert row["s3_secret"] == expected_creds["s3-secret"]

    def test_empty_subset_returns_empty_dataframe(self):
        """Empty input should return empty DataFrame."""
        subset = make_inputdf_for_sessions([])
        active_sessions = make_active_sessions_status()
        
        df_ready, updated_sessions = get_sessions_download_available_s3(
            conf=FAKE_CONF,
            active_s3_sessions_status=active_sessions,
            subset_to_treat=subset,
            blacklist=[],
            logins_group="logins",
        )
        
        assert len(df_ready) == 0
        expected_cols = [
            "s3_session", "login", "url", "S3Path", 
            "output_path", "safe", "s3_access_key", "s3_secret"
        ]
        assert list(df_ready.columns) == expected_cols

    def test_blacklist_excludes_accounts(self):
        """Blacklisted accounts should not appear in results."""
        subset = make_inputdf_for_sessions(["SAFE_A", "SAFE_B"])
        active_sessions = make_active_sessions_status()
        
        df_ready, _ = get_sessions_download_available_s3(
            conf=FAKE_CONF,
            active_s3_sessions_status=active_sessions,
            subset_to_treat=subset,
            blacklist=["user1@example.fr"],
            logins_group="logins",
        )
        
        if len(df_ready) > 0:
            assert "user1@example.fr" not in df_ready["login"].values


# ===========================================================================
# Part 3 — cds_s3_download_one_product tests (S3 credentials version)
# ===========================================================================


class TestCdsS3DownloadOneProductWithCredentials:
    """Tests for download function using direct S3 credentials."""

    def test_receives_s3_credentials_dict(self, tmp_path):
        """Function should accept s3_credentials dict with s3-access-key/s3-secret."""
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}
        
        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_bucket = MagicMock()
            mock_obj = make_s3_object(FAKE_S3_PATH + "/product.zip", size=100_000_000)
            mock_bucket.objects.filter.return_value = [mock_obj]
            mock_bucket.download_file = MagicMock()
            mock_boto3.return_value.Bucket.return_value = mock_bucket
            
            with patch("shutil.copy2"), patch("os.remove"), patch("os.chmod"):
                speed, elapsed, total_mb, status, safename = cds_s3_download_one_product(
                    FAKE_S3_PATH,
                    FAKE_S3_CREDENTIALS,
                    output,
                    conf
                )
            
            mock_boto3.assert_called_once()
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs["aws_access_key_id"] == FAKE_ACCESS_KEY
            assert call_kwargs["aws_secret_access_key"] == FAKE_SECRET
            assert status == "Downloaded"

    def test_handles_multi_file_safe_directory(self, tmp_path):
        """Should download all objects in a .SAFE directory tree."""
        output = str(tmp_path / FAKE_SAFENAME)
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}
        
        objects = [
            make_s3_object(f"{FAKE_S3_PATH}/manifest.safe", size=1_000),
            make_s3_object(f"{FAKE_S3_PATH}/measurement/data.tiff", size=50_000_000),
        ]
        
        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_bucket = MagicMock()
            mock_bucket.objects.filter.return_value = objects
            mock_bucket.download_file = MagicMock()
            mock_boto3.return_value.Bucket.return_value = mock_bucket
            
            with patch("os.makedirs"):
                speed, elapsed, total_mb, status, safename = cds_s3_download_one_product(
                    FAKE_S3_PATH,
                    FAKE_S3_CREDENTIALS,
                    output,
                    conf
                )
            
            assert mock_bucket.download_file.call_count == 2
            assert status == "Downloaded"

    def test_notfound_status_when_no_objects(self, tmp_path):
        """Should return NotFound when S3 path has no objects."""
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}
        
        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_bucket = MagicMock()
            mock_bucket.objects.filter.return_value = []
            mock_boto3.return_value.Bucket.return_value = mock_bucket
            
            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH,
                FAKE_S3_CREDENTIALS,
                output,
                conf
            )
            
            assert status == "NotFound"

    def test_s3error_on_boto_exception(self, tmp_path):
        """Should return S3Error on boto3 exceptions."""
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}
        
        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_boto3.side_effect = ClientError(
                {"Error": {"Code": "403", "Message": "Forbidden"}}, "Bucket"
            )
            
            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH,
                FAKE_S3_CREDENTIALS,
                output,
                conf
            )
            
            assert status == "S3Error"

    def test_cleans_up_tmp_on_error(self, tmp_path):
        """Should remove .tmp file if download fails mid-way."""
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        tmp_file = os.path.join(str(tmp_path), f"{FAKE_SAFENAME}.zip.tmp")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}
        
        open(tmp_file, "w").close()
        
        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_bucket = MagicMock()
            mock_obj = make_s3_object(FAKE_S3_PATH + "/product.zip")
            mock_bucket.objects.filter.return_value = [mock_obj]
            mock_bucket.download_file.side_effect = ClientError(
                {"Error": {"Code": "500"}}, "download_file"
            )
            mock_boto3.return_value.Bucket.return_value = mock_bucket
            
            with patch("os.remove") as mock_remove:
                cds_s3_download_one_product(
                    FAKE_S3_PATH,
                    FAKE_S3_CREDENTIALS,
                    output,
                    conf
                )
                mock_remove.assert_called()

    def test_skips_folder_marker_objects(self, tmp_path):
        """Objects ending with '/' should be skipped (S3 folder markers)."""
        output = str(tmp_path / FAKE_SAFENAME)
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}
        
        objects = [
            make_s3_object(f"{FAKE_S3_PATH}/", size=0),
            make_s3_object(f"{FAKE_S3_PATH}/manifest.safe", size=1_000),
        ]
        
        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_bucket = MagicMock()
            mock_bucket.objects.filter.return_value = objects
            mock_bucket.download_file = MagicMock()
            mock_boto3.return_value.Bucket.return_value = mock_bucket
            
            with patch("os.makedirs"):
                cds_s3_download_one_product(
                    FAKE_S3_PATH,
                    FAKE_S3_CREDENTIALS,
                    output,
                    conf
                )
            
            downloaded_keys = [call[0][0] for call in mock_bucket.download_file.call_args_list]
            assert f"{FAKE_S3_PATH}/" not in downloaded_keys
            assert f"{FAKE_S3_PATH}/manifest.safe" in downloaded_keys


# ===========================================================================
# Part 4 — download_list_product_multithread_v4 integration tests
# ===========================================================================


class TestV4InputValidation:
    """Input contracts."""

    def test_mismatched_lengths_raise(self):
        df = pd.DataFrame({
            "safename": ["SAFE_A", "SAFE_B"],
            "id": ["id0", "id1"],
        })
        with pytest.raises((AssertionError, KeyError)):
            download_list_product_multithread_v4(
                df, "/fake/out", account_group="logins"
            )

    def test_status_column_created_if_absent(self, patch_filter):
        """If inputdf has no 'status' column, v4 must add it silently."""
        safenames = ["SAFE_A"]
        df = make_inputdf_for_v4(safenames)
        df = df.drop(columns=["status"])  # Remove status to test auto-creation

        def _all_done(cpt, df_in, outputdir, force_download, cdsodatacli_conf, extension=""):
            empty = df_in.iloc[:0].copy()
            return empty, cpt

        patch_filter.side_effect = _all_done

        with patch("cdsodatacli.session.get_sessions_download_available_s3"):
            download_list_product_multithread_v4(
                df, "/fake/out", account_group="logins"
            )


class TestV4AllSuccessful:
    """Happy path: every product downloads with status 'Downloaded'."""

    def test_returns_dataframe(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf_for_v4(safenames)
        
        # Create proper return value: (DataFrame, active_sessions_dict)
        downloadable_df = make_inputdf_for_sessions(safenames)
        downloadable_df["login"] = [FAKE_LOGIN] * len(safenames)
        downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY] * len(safenames)
        downloadable_df["s3_secret"] = [FAKE_SECRET] * len(safenames)
        downloadable_df["s3_session"] = [0] * len(safenames)
        downloadable_df["output_path"] = [f"/fake/spool/{s}" for s in safenames]
        
        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            return_value=(downloadable_df, make_active_sessions_status()),
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=[make_s3_result(s) for s in safenames],
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future_a = make_mock_future(make_s3_result("SAFE_A"))
                mock_future_b = make_mock_future(make_s3_result("SAFE_B"))
                mock_executor.submit.side_effect = [mock_future_a, mock_future_b]
                patch_concurrent_wait.return_value = ({mock_future_a, mock_future_b}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = 1
                            cpt["successful_download"] += 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    result = download_list_product_multithread_v4(
                        inputdf, "/fake/out", account_group="logins"
                    )

        assert isinstance(result, pd.DataFrame)
        assert "status" in result.columns

    def test_all_status_1_on_downloaded(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf_for_v4(safenames)

        def mock_s3_worker(s3_path, s3_credentials, output_path, conf):
            assert isinstance(s3_credentials, dict)
            assert "s3-access-key" in s3_credentials
            assert "s3-secret" in s3_credentials
            safename = os.path.basename(output_path)
            return make_s3_result(safename)

        downloadable_df = make_inputdf_for_sessions(safenames)
        downloadable_df["login"] = [FAKE_LOGIN] * len(safenames)
        downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY] * len(safenames)
        downloadable_df["s3_secret"] = [FAKE_SECRET] * len(safenames)
        downloadable_df["s3_session"] = [0] * len(safenames)
        downloadable_df["output_path"] = [f"/fake/spool/{s}" for s in safenames]

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            return_value=(downloadable_df, make_active_sessions_status()),
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=mock_s3_worker,
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future_a = make_mock_future(make_s3_result("SAFE_A"))
                mock_future_b = make_mock_future(make_s3_result("SAFE_B"))
                mock_executor.submit.side_effect = [mock_future_a, mock_future_b]
                patch_concurrent_wait.return_value = ({mock_future_a, mock_future_b}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = 1
                            cpt["successful_download"] += 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    result = download_list_product_multithread_v4(
                        inputdf, "/fake/out", account_group="logins"
                    )

        assert (result["status"] == 1).all()


class TestV4DownloadErrors:
    """Non-OK status_meaning and unhandled exceptions."""

    def test_notfound_marks_status_minus1(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        safenames = ["SAFE_MISSING"]
        inputdf = make_inputdf_for_v4(safenames)

        downloadable_df = make_inputdf_for_sessions(safenames)
        downloadable_df["login"] = [FAKE_LOGIN]
        downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY]
        downloadable_df["s3_secret"] = [FAKE_SECRET]
        downloadable_df["s3_session"] = [0]
        downloadable_df["output_path"] = [f"/fake/spool/SAFE_MISSING"]

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            return_value=(downloadable_df, make_active_sessions_status()),
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result("SAFE_MISSING", status="NotFound", speed=np.nan),
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future = make_mock_future(
                    make_s3_result("SAFE_MISSING", status="NotFound", speed=np.nan)
                )
                mock_executor.submit.return_value = mock_future
                patch_concurrent_wait.return_value = ({mock_future}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = -1
                            cpt["status_NotFound"] = cpt.get("status_NotFound", 0) + 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    result = download_list_product_multithread_v4(
                        inputdf, "/fake/out", account_group="logins"
                    )

        assert result.loc[result["safe"] == "SAFE_MISSING", "status"].iloc[0] == -1

    def test_unhandled_exception_marks_status_minus1(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        safenames = ["SAFE_CRASH"]
        inputdf = make_inputdf_for_v4(safenames)

        downloadable_df = make_inputdf_for_sessions(safenames)
        downloadable_df["login"] = [FAKE_LOGIN]
        downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY]
        downloadable_df["s3_secret"] = [FAKE_SECRET]
        downloadable_df["s3_session"] = [0]
        downloadable_df["output_path"] = [f"/fake/spool/SAFE_CRASH"]

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            return_value=(downloadable_df, make_active_sessions_status()),
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=RuntimeError("unexpected crash"),
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future = MagicMock(spec=Future)
                mock_future.exception.return_value = RuntimeError("unexpected crash")
                mock_future.done.return_value = True
                mock_executor.submit.return_value = mock_future
                patch_concurrent_wait.return_value = ({mock_future}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = -1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    result = download_list_product_multithread_v4(
                        inputdf, "/fake/out", account_group="logins"
                    )

        assert result.loc[result["safe"] == "SAFE_CRASH", "status"].iloc[0] == -1


class TestV4CredentialsFlow:
    """Tests that credentials flow correctly through the v4 pipeline."""

    def test_credentials_passed_to_download_worker(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        """Verify s3_credentials from get_sessions_download_available_s3 reach cds_s3_download_one_product."""
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)
        captured_creds = {}

        def capture_worker(s3_path, s3_credentials, output_path, conf):
            captured_creds["received"] = s3_credentials.copy()
            return make_s3_result("SAFE_A")

        downloadable_df = make_inputdf_for_sessions(safenames)
        downloadable_df["login"] = [FAKE_LOGIN]
        downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY]
        downloadable_df["s3_secret"] = [FAKE_SECRET]
        downloadable_df["s3_session"] = [0]
        downloadable_df["output_path"] = [f"/fake/spool/SAFE_A"]

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            return_value=(downloadable_df, make_active_sessions_status()),
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=capture_worker,
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future = make_mock_future(make_s3_result("SAFE_A"))
                mock_executor.submit.return_value = mock_future
                patch_concurrent_wait.return_value = ({mock_future}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = 1
                            cpt["successful_download"] += 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    download_list_product_multithread_v4(
                        inputdf, "/fake/out", account_group="logins"
                    )

        assert "received" in captured_creds
        assert captured_creds["received"]["s3-access-key"] == FAKE_ACCESS_KEY
        assert captured_creds["received"]["s3-secret"] == FAKE_SECRET

    def test_multiple_accounts_use_correct_credentials(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        """Each product should use credentials from its assigned account."""
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf_for_v4(safenames)
        credential_log = {}

        def logging_worker(s3_path, s3_credentials, output_path, conf):
            safename = os.path.basename(output_path)
            credential_log[safename] = s3_credentials.copy()
            return make_s3_result(safename)

        downloadable_df = make_inputdf_for_sessions(safenames)
        downloadable_df["login"] = ["user1@example.fr", "user2@example.fr"]
        downloadable_df["s3_access_key"] = [
            FAKE_CONF["logins"]["user1@example.fr"]["s3-access-key"],
            FAKE_CONF["logins"]["user2@example.fr"]["s3-access-key"],
        ]
        downloadable_df["s3_secret"] = [
            FAKE_CONF["logins"]["user1@example.fr"]["s3-secret"],
            FAKE_CONF["logins"]["user2@example.fr"]["s3-secret"],
        ]
        downloadable_df["s3_session"] = [0, 0]
        downloadable_df["output_path"] = [f"/fake/spool/{s}" for s in safenames]

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            return_value=(downloadable_df, make_active_sessions_status()),
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=logging_worker,
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                futures = [make_mock_future(make_s3_result(s)) for s in safenames]
                mock_executor.submit.side_effect = futures
                patch_concurrent_wait.return_value = (set(futures), set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = 1
                            cpt["successful_download"] += 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    download_list_product_multithread_v4(
                        inputdf, "/fake/out", account_group="logins"
                    )

        assert "SAFE_A" in credential_log
        assert "SAFE_B" in credential_log
        assert credential_log["SAFE_A"]["s3-access-key"] == FAKE_CONF["logins"]["user1@example.fr"]["s3-access-key"]
        assert credential_log["SAFE_B"]["s3-access-key"] == FAKE_CONF["logins"]["user2@example.fr"]["s3-access-key"]


class TestV4NewParameters:
    """Tests for new optional parameters in v4."""

    def test_hideprogressbar_sets_env_var(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)

        downloadable_df = make_inputdf_for_sessions(safenames)
        downloadable_df["login"] = [FAKE_LOGIN]
        downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY]
        downloadable_df["s3_secret"] = [FAKE_SECRET]
        downloadable_df["s3_session"] = [0]
        downloadable_df["output_path"] = [f"/fake/spool/SAFE_A"]

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            return_value=(downloadable_df, make_active_sessions_status()),
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result("SAFE_A"),
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future = make_mock_future(make_s3_result("SAFE_A"))
                mock_executor.submit.return_value = mock_future
                patch_concurrent_wait.return_value = ({mock_future}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = 1
                            cpt["successful_download"] += 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    with patch.dict(os.environ, {}, clear=True):
                        download_list_product_multithread_v4(
                            inputdf, "/fake/out", account_group="logins", hideprogressbar=True
                        )
                        assert os.environ.get("DISABLE_TQDM") == "True"

    def test_check_on_disk_false_disables_filter(
        self, patch_filter, patch_threadpool_executor, patch_concurrent_wait,
        patch_session_lock, patch_get_credentials
    ):
        """When check_on_disk=False, force_download=True should be passed to filter."""
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)

        captured_force = {"value": None}
        
        def _capture_force(cpt, df_in, outputdir, force_download, cdsodatacli_conf, extension=""):
            captured_force["value"] = force_download
            empty = df_in.iloc[:0].copy()
            return empty, cpt

        patch_filter.side_effect = _capture_force

        with patch("cdsodatacli.session.get_sessions_download_available_s3"):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins", check_on_disk=False
            )

        assert captured_force["value"] is True


class TestV4NoSession:
    """get_sessions_download_available_s3 returns empty — should wait and retry."""

    def test_sleeps_when_no_session(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 3:
                empty_df = pd.DataFrame(columns=[
                    "s3_session", "login", "url", "S3Path", 
                    "output_path", "safe", "s3_access_key", "s3_secret"
                ])
                return empty_df, make_active_sessions_status()
            downloadable_df = make_inputdf_for_sessions(safenames)
            downloadable_df["login"] = [FAKE_LOGIN]
            downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY]
            downloadable_df["s3_secret"] = [FAKE_SECRET]
            downloadable_df["s3_session"] = [0]
            downloadable_df["output_path"] = [f"/fake/spool/SAFE_A"]
            return downloadable_df, make_active_sessions_status()

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            side_effect=sessions_side_effect,
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result("SAFE_A"),
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future = make_mock_future(make_s3_result("SAFE_A"))
                mock_executor.submit.return_value = mock_future
                patch_concurrent_wait.return_value = ({mock_future}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = 1
                            cpt["successful_download"] += 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    with patch("cdsodatacli.download.time.sleep") as mock_sleep:
                        download_list_product_multithread_v4(
                            inputdf, "/fake/out", account_group="logins"
                        )

        assert mock_sleep.call_count >= 2

    def test_sleep_duration_is_5_seconds(
        self, patch_threadpool_executor, patch_concurrent_wait, patch_filter,
        patch_session_lock, patch_get_credentials
    ):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 2:
                empty_df = pd.DataFrame(columns=[
                    "s3_session", "login", "url", "S3Path", 
                    "output_path", "safe", "s3_access_key", "s3_secret"
                ])
                return empty_df, make_active_sessions_status()
            downloadable_df = make_inputdf_for_sessions(safenames)
            downloadable_df["login"] = [FAKE_LOGIN]
            downloadable_df["s3_access_key"] = [FAKE_ACCESS_KEY]
            downloadable_df["s3_secret"] = [FAKE_SECRET]
            downloadable_df["s3_session"] = [0]
            downloadable_df["output_path"] = [f"/fake/spool/SAFE_A"]
            return downloadable_df, make_active_sessions_status()

        with patch(
            "cdsodatacli.session.get_sessions_download_available_s3",
            side_effect=sessions_side_effect,
        ):
            with patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result("SAFE_A"),
            ):
                mock_executor_cls, mock_executor = patch_threadpool_executor
                mock_future = make_mock_future(make_s3_result("SAFE_A"))
                mock_executor.submit.return_value = mock_future
                patch_concurrent_wait.return_value = ({mock_future}, set())
                
                with patch("cdsodatacli.download.process_completed_futures") as mock_process:
                    def side_effect_process(done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions):
                        for fut in done:
                            info = f2i[fut]
                            df2.loc[df2["safe"] == info["safename"], "status"] = 1
                            cpt["successful_download"] += 1
                        return done, {}, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
                    mock_process.side_effect = side_effect_process
                    
                    with patch("cdsodatacli.download.time.sleep") as mock_sleep:
                        download_list_product_multithread_v4(
                            inputdf, "/fake/out", account_group="logins"
                        )

        for c in mock_sleep.call_args_list:
            assert c.args[0] == 5


# ===========================================================================
# Part 5 — process_completed_futures helper tests
# ===========================================================================


class TestProcessCompletedFutures:
    """Tests for the process_completed_futures helper function."""

    def test_process_success_updates_status_and_counters(self, patch_filter):
        """Verify process_completed_futures handles successful downloads."""
        from cdsodatacli.download import process_completed_futures
        
        df2 = pd.DataFrame({"safe": ["SAFE_A"], "status": [2]})
        pbar = MagicMock()
        cpt = defaultdict(int)
        
        future = MagicMock(spec=Future)
        future.result.return_value = make_s3_result("SAFE_A", status="Downloaded")
        future.exception.return_value = None
        
        future_to_info = {
            future: {
                "safename": "SAFE_A",
                "login": "user1@example.fr",  # This is the key in active_sessions dict
                "s3_session_id": 0,
            }
        }
        
        (
            done, new_f2i, new_df2, new_pbar, speeds, elapsed, total, 
            new_cpt, errors, blacklist, sessions
        ) = process_completed_futures(
            {future}, future_to_info, df2, pbar, [], [], [], 
            cpt, defaultdict(int), [], {}
        )
        
        assert new_df2.loc[new_df2["safe"] == "SAFE_A", "status"].iloc[0] == 1
        assert new_cpt["successful_download"] == 1
        assert len(speeds) == 1

    def test_process_error_updates_status_and_counters(self, patch_filter):
        """Verify process_completed_futures handles failed downloads."""
        from cdsodatacli.download import process_completed_futures
        
        df2 = pd.DataFrame({"safe": ["SAFE_ERR"], "status": [2]})
        pbar = MagicMock()
        cpt = defaultdict(int)
        
        future = MagicMock(spec=Future)
        future.result.return_value = make_s3_result("SAFE_ERR", status="S3Error", speed=np.nan)
        future.exception.return_value = None
        
        future_to_info = {
            future: {
                "safename": "SAFE_ERR",
                "login": "user1@example.fr",
                "s3_session_id": 0,
            }
        }
        
        (
            done, new_f2i, new_df2, new_pbar, speeds, elapsed, total, 
            new_cpt, errors, blacklist, sessions
        ) = process_completed_futures(
            {future}, future_to_info, df2, pbar, [], [], [], 
            cpt, defaultdict(int), [], {}
        )
        
        assert new_df2.loc[new_df2["safe"] == "SAFE_ERR", "status"].iloc[0] == -1
        assert "status_S3Error" in new_cpt