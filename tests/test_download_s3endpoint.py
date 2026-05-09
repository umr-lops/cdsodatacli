"""
pytest unit tests for:
  - get_a_free_s3_session
  - get_sessions_download_available_s3
  - cds_s3_download_one_product (S3 credentials version)
  - download_list_product_multithread_v4

Run with: pytest test_download_s3endpoint.py -v
"""

import os
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor, Future
from botocore.exceptions import ClientError
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

    Mirrors the internal df2 columns that v4 passes as subset_to_treat.
    Required columns: safe, S3Path, urls, outputpath, status
    Note: 'urls' is read by get_sessions_download_available_s3 internally
    even though v4 uses S3Path for the actual download.
    """
    n = len(safenames)
    if s3paths is None:
        s3paths = [f"Sentinel-1/SAR/GRD/2022/05/03/{s}.SAFE" for s in safenames]
    if statuses is None:
        statuses = np.zeros(n)
    return pd.DataFrame(
        {
            "safe": safenames,
            "S3Path": s3paths,
            "id": [f"id-{i}" for i in range(n)],
            "status": statuses,
            "urls": [f"https://fake.cdse/{s}" for s in safenames],
            "outputpath": [f"/fake/spool/{s}" for s in safenames],
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
            "safename": safenames,
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

    with patch(
        "cdsodatacli.session.get_a_credentials_from_conf_file",
        side_effect=_get_creds,
    ):
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
        yield

    def test_returns_first_free_session(self):
        active_sessions = make_active_sessions_status(["user1@example.fr"])

        result_sessions, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )

        assert session_id == 0
        assert login == "user1@example.fr"
        assert creds["s3-access-key"] == FAKE_ACCESS_KEY
        assert creds["s3-secret"] == FAKE_SECRET
        assert result_sessions["user1@example.fr"][0] is True

    def test_marks_session_as_active(self):
        active_sessions = make_active_sessions_status(["user1@example.fr"])

        result_sessions, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )

        assert result_sessions["user1@example.fr"][session_id] is True
        assert active_sessions["user1@example.fr"][0] is True

    def test_skips_blacklisted_accounts(self):
        active_sessions = make_active_sessions_status(
            ["user1@example.fr", "user2@example.fr"]
        )

        result_sessions, session_id, login, creds = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=["user1@example.fr"],
        )

        assert login == "user2@example.fr"
        assert (
            creds["s3-access-key"]
            == FAKE_CONF["logins"]["user2@example.fr"]["s3-access-key"]
        )

    def test_returns_none_when_all_sessions_busy(self):
        active_sessions = make_active_sessions_status(["user1@example.fr"])
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
        active_sessions = make_active_sessions_status(
            ["user1@example.fr", "user2@example.fr"]
        )

        _, sid1, login1, _ = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )
        assert login1 == "user1@example.fr"
        assert sid1 == 0

        _, sid2, login2, _ = get_a_free_s3_session(
            active_sessions,
            conf=FAKE_CONF,
            account_group="logins",
            blacklist=[],
        )
        assert login2 == "user1@example.fr"
        assert sid2 == 1

    def test_extracts_credentials_from_nested_config(self):
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

        assert len(df_ready) <= 2

    def test_marks_sessions_as_active_in_returned_status(self):
        subset = make_inputdf_for_sessions(["SAFE_A"])
        active_sessions = make_active_sessions_status()

        df_ready, updated_sessions = get_sessions_download_available_s3(
            conf=FAKE_CONF,
            active_s3_sessions_status=active_sessions,
            subset_to_treat=subset,
            blacklist=[],
            logins_group="logins",
        )

        found_active = any(
            updated_sessions[login][sid] is True
            for login in updated_sessions
            for sid in updated_sessions[login]
        )
        assert found_active

    def test_credentials_match_config_for_each_login(self):
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

    def test_blacklist_excludes_accounts(self):
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
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_bucket = MagicMock()
            mock_obj = make_s3_object(FAKE_S3_PATH + "/product.zip", size=100_000_000)
            mock_bucket.objects.filter.return_value = [mock_obj]
            mock_bucket.download_file = MagicMock()
            mock_boto3.return_value.Bucket.return_value = mock_bucket

            with patch("shutil.copy2"), patch("os.remove"), patch("os.chmod"):
                speed, elapsed, total_mb, status, safename = (
                    cds_s3_download_one_product(
                        FAKE_S3_PATH, FAKE_S3_CREDENTIALS, output, conf
                    )
                )

            mock_boto3.assert_called_once()
            call_kwargs = mock_boto3.call_args[1]
            assert call_kwargs["aws_access_key_id"] == FAKE_ACCESS_KEY
            assert call_kwargs["aws_secret_access_key"] == FAKE_SECRET
            assert status == "Downloaded"

    def test_handles_multi_file_safe_directory(self, tmp_path):
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
                speed, elapsed, total_mb, status, safename = (
                    cds_s3_download_one_product(
                        FAKE_S3_PATH, FAKE_S3_CREDENTIALS, output, conf
                    )
                )

            assert mock_bucket.download_file.call_count == 2
            assert status == "Downloaded"

    def test_notfound_status_when_no_objects(self, tmp_path):
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_bucket = MagicMock()
            mock_bucket.objects.filter.return_value = []
            mock_boto3.return_value.Bucket.return_value = mock_bucket

            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_S3_CREDENTIALS, output, conf
            )

            assert status == "NotFound"

    def test_s3error_on_boto_exception(self, tmp_path):
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with patch("cdsodatacli.download.boto3.resource") as mock_boto3:
            mock_boto3.side_effect = ClientError(
                {"Error": {"Code": "403", "Message": "Forbidden"}}, "Bucket"
            )

            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_S3_CREDENTIALS, output, conf
            )

            assert status == "S3Error"

    def test_cleans_up_tmp_on_error(self, tmp_path):
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
                    FAKE_S3_PATH, FAKE_S3_CREDENTIALS, output, conf
                )
                mock_remove.assert_called()

    def test_skips_folder_marker_objects(self, tmp_path):
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
                    FAKE_S3_PATH, FAKE_S3_CREDENTIALS, output, conf
                )

            downloaded_keys = [
                c[0][0] for c in mock_bucket.download_file.call_args_list
            ]
            assert f"{FAKE_S3_PATH}/" not in downloaded_keys
            assert f"{FAKE_S3_PATH}/manifest.safe" in downloaded_keys


# ===========================================================================
# Part 4 — download_list_product_multithread_v4 integration tests
# ===========================================================================

# Shared helper: build the downloadable df returned by get_sessions_download_available_s3


def _make_ready_df(safenames, login=None):
    """Build df as returned by get_sessions_download_available_s3."""
    n = len(safenames)
    if login is None:
        login = FAKE_LOGIN
    return pd.DataFrame(
        {
            "safe": safenames,
            "S3Path": [f"Sentinel-1/SAR/GRD/2022/05/03/{s}.SAFE" for s in safenames],
            "id": [f"id-{i}" for i in range(n)],
            "status": np.zeros(n),
            "output_path": [f"/fake/spool/{s}" for s in safenames],
            "login": [login] * n,
            "s3_access_key": [FAKE_ACCESS_KEY] * n,
            "s3_secret": [FAKE_SECRET] * n,
            "s3_session": list(range(n)),
        }
    )


def _side_effect_process_success(
    done, f2i, df2, pbar, speeds, elapsed, total, cpt, errors, blacklist, sessions
):
    """process_completed_futures stub: marks each future's safename as status=1."""
    for fut in done:
        info = f2i.pop(fut, {})
        safename = info.get("safename", "unknown")
        login = info.get("login", "unknown")
        session_id = info.get("s3_session_id", 0)
        if login in sessions:
            sessions[login][session_id] = False
        try:
            speed, el, mb, status_meaning, safename = fut.result()
        except Exception:
            df2.loc[df2["safe"] == safename, "status"] = -1
            pbar.update(1)
            continue
        if status_meaning in ("OK", "Downloaded"):
            df2.loc[df2["safe"] == safename, "status"] = 1
            speeds.append(speed)
            elapsed.append(el)
            total.append(mb)
            cpt["successful_download"] += 1
        else:
            df2.loc[df2["safe"] == safename, "status"] = -1
        cpt[f"status_{status_meaning}"] += 1
        pbar.update(1)
    return (
        done,
        f2i,
        df2,
        pbar,
        speeds,
        elapsed,
        total,
        cpt,
        errors,
        blacklist,
        sessions,
    )


class TestV4InputValidation:
    """Input contracts."""

    def test_mismatched_lengths_raise(self):
        df = pd.DataFrame(
            {
                "safename": ["SAFE_A", "SAFE_B"],
                "id": ["id0", "id1"],
            }
        )
        with pytest.raises((AssertionError, KeyError)):
            download_list_product_multithread_v4(
                df, "/fake/out", account_group="logins"
            )

    def test_status_column_created_if_absent(self, patch_filter):
        df = make_inputdf_for_v4(["SAFE_A"])
        df = df.drop(columns=["status"])

        def _all_done(
            cpt, df_in, outputdir, force_download, cdsodatacli_conf, extension=""
        ):
            return df_in.iloc[:0].copy(), cpt

        patch_filter.side_effect = _all_done

        with patch("cdsodatacli.download.get_sessions_download_available_s3"):
            download_list_product_multithread_v4(
                df, "/fake/out", account_group="logins"
            )


class TestV4AllSuccessful:
    """Happy path: every product downloads with status 'Downloaded'."""

    def test_returns_dataframe(self):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf_for_v4(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=[make_s3_result(s) for s in safenames],
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )
        assert isinstance(result, pd.DataFrame)

    def test_all_status_1_on_downloaded(self):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf_for_v4(safenames)

        def s3_worker(s3_path, s3_credentials, output_path, conf):
            assert "s3-access-key" in s3_credentials
            assert "s3-secret" in s3_credentials
            return make_s3_result(os.path.basename(output_path))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=s3_worker,
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )
        assert (result["status"] == 1).all()

    def test_ok_status_meaning_also_sets_status_1(self):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result("SAFE_A", status="OK"),
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )
        assert result.loc[result["safe"] == "SAFE_A", "status"].iloc[0] == 1


class TestV4DownloadErrors:
    """Non-OK status_meaning and unhandled exceptions."""

    def test_notfound_marks_status_minus1(self):
        safenames = ["SAFE_MISSING"]
        inputdf = make_inputdf_for_v4(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result(
                    "SAFE_MISSING", status="NotFound", speed=np.nan
                ),
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )
        assert result.loc[result["safe"] == "SAFE_MISSING", "status"].iloc[0] == -1

    def test_unhandled_exception_marks_status_minus1(self):
        safenames = ["SAFE_CRASH"]
        inputdf = make_inputdf_for_v4(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=RuntimeError("unexpected crash"),
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )
        assert result.loc[result["safe"] == "SAFE_CRASH", "status"].iloc[0] == -1

    def test_all_failures_loop_terminates(self):
        """While loop must exit even if every product fails."""
        safenames = ["SAFE_A", "SAFE_B", "SAFE_C"]
        inputdf = make_inputdf_for_v4(safenames)

        def failing_worker(s3_path, s3_credentials, output_path, conf):
            return make_s3_result(
                os.path.basename(output_path), status="S3Error", speed=np.nan
            )

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=failing_worker,
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )
        assert (result["status"] != 0).all()


class TestV4CredentialsFlow:
    """Tests that credentials flow correctly through the v4 pipeline."""

    def test_credentials_passed_to_download_worker(self):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)
        captured_creds = {}

        def capture_worker(s3_path, s3_credentials, output_path, conf):
            captured_creds["received"] = s3_credentials.copy()
            return make_s3_result("SAFE_A")

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=capture_worker,
            ),
        ):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert "received" in captured_creds
        assert captured_creds["received"]["s3-access-key"] == FAKE_ACCESS_KEY
        assert captured_creds["received"]["s3-secret"] == FAKE_SECRET

    def test_multiple_accounts_use_correct_credentials(self):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf_for_v4(safenames)
        credential_log = {}

        def logging_worker(s3_path, s3_credentials, output_path, conf):
            safename = os.path.basename(output_path)
            credential_log[safename] = s3_credentials.copy()
            return make_s3_result(safename)

        # Build ready df with different logins per product
        ready_df = _make_ready_df(safenames)
        ready_df["login"] = ["user1@example.fr", "user2@example.fr"]
        ready_df["s3_access_key"] = [
            FAKE_CONF["logins"]["user1@example.fr"]["s3-access-key"],
            FAKE_CONF["logins"]["user2@example.fr"]["s3-access-key"],
        ]
        ready_df["s3_secret"] = [
            FAKE_CONF["logins"]["user1@example.fr"]["s3-secret"],
            FAKE_CONF["logins"]["user2@example.fr"]["s3-secret"],
        ]

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(ready_df, make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=logging_worker,
            ),
        ):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert "SAFE_A" in credential_log
        assert "SAFE_B" in credential_log
        assert (
            credential_log["SAFE_A"]["s3-access-key"]
            == FAKE_CONF["logins"]["user1@example.fr"]["s3-access-key"]
        )
        assert (
            credential_log["SAFE_B"]["s3-access-key"]
            == FAKE_CONF["logins"]["user2@example.fr"]["s3-access-key"]
        )


class TestV4NewParameters:
    """Tests for optional parameters in v4."""

    def test_hideprogressbar_sets_env_var(self):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                return_value=(_make_ready_df(safenames), make_active_sessions_status()),
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result("SAFE_A"),
            ),
            patch.dict(os.environ, {}, clear=True),
        ):
            download_list_product_multithread_v4(
                inputdf,
                "/fake/out",
                account_group="logins",
                hideprogressbar=True,
            )
            assert os.environ.get("DISABLE_TQDM") == "True"

    def test_check_on_disk_false_passes_force_download_true(self, patch_filter):
        inputdf = make_inputdf_for_v4(["SAFE_A"])
        captured = {}

        def _capture(
            cpt, df_in, outputdir, force_download, cdsodatacli_conf, extension=""
        ):
            captured["force_download"] = force_download
            return df_in.iloc[:0].copy(), cpt

        patch_filter.side_effect = _capture

        with patch("cdsodatacli.download.get_sessions_download_available_s3"):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins", check_on_disk=False
            )

        assert captured["force_download"] is True


class TestV4NoSession:
    """get_sessions_download_available_s3 returns empty — should wait and retry."""

    def _make_empty_ready_df(self):
        return pd.DataFrame(
            columns=[
                "safe",
                "S3Path",
                "id",
                "status",
                "output_path",
                "login",
                "s3_access_key",
                "s3_secret",
                "s3_session",
            ]
        )

    def test_sleeps_when_no_session(self):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(**kw):
            call_count["n"] += 1
            if call_count["n"] < 3:
                return self._make_empty_ready_df(), make_active_sessions_status()
            return _make_ready_df(safenames), make_active_sessions_status()

        def s3_worker(s3_path, s3_credentials, output_path, conf):
            return make_s3_result(os.path.basename(output_path))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=s3_worker,
            ),
            patch("cdsodatacli.download.time.sleep") as mock_sleep,
        ):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert mock_sleep.call_count >= 2

    def test_sleep_duration_is_5_seconds(self):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf_for_v4(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(**kw):
            call_count["n"] += 1
            if call_count["n"] < 2:
                return self._make_empty_ready_df(), make_active_sessions_status()
            return _make_ready_df(safenames), make_active_sessions_status()

        def s3_worker(s3_path, s3_credentials, output_path, conf):
            return make_s3_result(os.path.basename(output_path))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available_s3",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.process_completed_futures",
                side_effect=_side_effect_process_success,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=s3_worker,
            ),
            patch("cdsodatacli.download.time.sleep") as mock_sleep,
        ):
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

    # FIX 2: pass a real active_sessions dict so release_s3_session_after_usage
    # can look up login keys without KeyError.
    _active_sessions = {"user1@example.fr": {0: True, 1: False, 2: False, 3: False}}

    def test_process_success_updates_status_and_counters(self):
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
                "login": "user1@example.fr",
                "s3_session_id": 0,
            }
        }
        active_sessions = {"user1@example.fr": {0: True, 1: False, 2: False, 3: False}}

        (
            done,
            new_f2i,
            new_df2,
            new_pbar,
            speeds,
            elapsed,
            total,
            new_cpt,
            errors,
            blacklist,
            sessions,
        ) = process_completed_futures(
            {future},
            future_to_info,
            df2,
            pbar,
            [],
            [],
            [],
            cpt,
            defaultdict(int),
            [],
            active_sessions,
        )

        assert new_df2.loc[new_df2["safe"] == "SAFE_A", "status"].iloc[0] == 1
        assert new_cpt["successful_download"] == 1
        assert len(speeds) == 1
        # session must be released after processing
        assert sessions["user1@example.fr"][0] is False

    def test_process_error_updates_status_and_counters(self):
        from cdsodatacli.download import process_completed_futures

        df2 = pd.DataFrame({"safe": ["SAFE_ERR"], "status": [2]})
        pbar = MagicMock()
        cpt = defaultdict(int)

        future = MagicMock(spec=Future)
        future.result.return_value = make_s3_result(
            "SAFE_ERR", status="S3Error", speed=np.nan
        )
        future.exception.return_value = None

        future_to_info = {
            future: {
                "safename": "SAFE_ERR",
                "login": "user1@example.fr",
                "s3_session_id": 0,
            }
        }
        active_sessions = {"user1@example.fr": {0: True, 1: False, 2: False, 3: False}}

        (
            done,
            new_f2i,
            new_df2,
            new_pbar,
            speeds,
            elapsed,
            total,
            new_cpt,
            errors,
            blacklist,
            sessions,
        ) = process_completed_futures(
            {future},
            future_to_info,
            df2,
            pbar,
            [],
            [],
            [],
            cpt,
            defaultdict(int),
            [],
            active_sessions,
        )

        assert new_df2.loc[new_df2["safe"] == "SAFE_ERR", "status"].iloc[0] == -1
        assert "status_S3Error" in new_cpt
        assert sessions["user1@example.fr"][0] is False
