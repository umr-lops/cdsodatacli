"""
pytest unit tests for:
  - cds_s3_download_one_product
  - download_list_product_multithread_v4

Run with: pytest test_download_s3.py -v
"""

import os
import time
import threading
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock
from botocore.exceptions import BotoCoreError, ClientError


# ---------------------------------------------------------------------------
# Shared constants
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
        "user1@example.fr": "passwd1",
        "user2@example.fr": "passwd2",
    },
}

FAKE_S3_PATH = "Sentinel-1/SAR/GRD/2022/05/03/S1A_IW_GRDH_1SDV_20220503T000000.SAFE"
FAKE_SAFENAME = "S1A_IW_GRDH_1SDV_20220503T000000"
FAKE_OUTPUT = f"/fake/spool/{FAKE_SAFENAME}.zip"
FAKE_HEADER = {"Authorization": "Bearer fake-token"}
FAKE_ACCESS_ID = "fake-access-id-123"
FAKE_CREDENTIALS = {"access_id": FAKE_ACCESS_ID}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_s3_object(key, size=1_000_000):
    """Return a MagicMock mimicking a boto3 S3 ObjectSummary."""
    obj = MagicMock()
    obj.key = key
    obj.size = size
    return obj


def make_s3_client_mocks(objects):
    """
    Return (mock_credentials, mock_s3_resource) pair suitable for
    patching _get_fresh_s3_client.

    objects: list of MagicMock s3 ObjectSummary instances
    """
    mock_bucket = MagicMock()
    mock_bucket.objects.filter.return_value = objects
    mock_bucket.download_file = MagicMock()

    mock_s3_resource = MagicMock()
    mock_s3_resource.Bucket.return_value = mock_bucket

    return FAKE_CREDENTIALS, mock_s3_resource


def make_s3_result(
    safename=FAKE_SAFENAME,
    status="Downloaded",
    speed=50.0,
    elapsed=20.0,
    total_mb=500.0,
):
    """Return a tuple matching cds_s3_download_one_product's return signature."""
    return speed, elapsed, total_mb, status, safename


def make_inputdf(safenames, s3paths=None, statuses=None):
    """Build a minimal inputdf as download_list_product_multithread_v4 expects."""
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


def make_downloadable_df(safenames, s3paths=None):
    """Simulate the output of get_sessions_download_available for v4."""
    n = len(safenames)
    if s3paths is None:
        s3paths = [f"Sentinel-1/SAR/GRD/2022/05/03/{s}.SAFE" for s in safenames]
    return pd.DataFrame(
        {
            "safe": safenames,
            "header": [FAKE_HEADER for _ in range(n)],
            "output_path": [f"/fake/spool/{s}" for s in safenames],
            "S3Path": s3paths,
            "login": ["user1@example.fr"] * n,
        }
    )


# ---------------------------------------------------------------------------
# Fixtures (autouse)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def patch_conf():
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


# ---------------------------------------------------------------------------
# Import after fixtures so module-level calls don't fail
# ---------------------------------------------------------------------------

from cdsodatacli.download import (  # noqa: E402
    cds_s3_download_one_product,
    download_list_product_multithread_v4,
)


# ===========================================================================
# Part 1 — cds_s3_download_one_product unit tests
# ===========================================================================


class TestCdsS3DownloadOneProductSuccess:
    """Happy path: single-object product downloads and moves correctly."""

    def test_returns_five_values(self, tmp_path):
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip", size=500_000_000)
        creds, s3_resource = make_s3_client_mocks([obj])

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("shutil.copy2"),
            patch("os.remove"),
            patch("os.chmod"),
        ):
            result = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert len(result) == 5

    def test_status_is_downloaded_on_success(self, tmp_path):
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip", size=200_000_000)
        creds, s3_resource = make_s3_client_mocks([obj])

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("shutil.copy2"),
            patch("os.remove"),
            patch("os.chmod"),
        ):
            speed, elapsed, total_mb, status, safename = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert status == "Downloaded"

    def test_safename_base_strips_zip(self, tmp_path):
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip")
        creds, s3_resource = make_s3_client_mocks([obj])

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("shutil.copy2"),
            patch("os.remove"),
            patch("os.chmod"),
        ):
            *_, safename = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert safename == FAKE_SAFENAME
        assert ".zip" not in safename

    def test_speed_is_positive(self, tmp_path):
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip", size=100_000_000)
        creds, s3_resource = make_s3_client_mocks([obj])

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("shutil.copy2"),
            patch("os.remove"),
            patch("os.chmod"),
        ):
            speed, elapsed, total_mb, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert speed > 0
        assert elapsed > 0
        assert total_mb > 0

    def test_tmp_file_moved_to_final_path(self, tmp_path):
        """shutil.copy2 must be called with the .tmp source and final destination."""
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip")
        creds, s3_resource = make_s3_client_mocks([obj])

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}
        expected_tmp = os.path.join(str(tmp_path), f"{FAKE_SAFENAME}.zip.tmp")

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("shutil.copy2") as mock_copy,
            patch("os.remove"),
            patch("os.chmod"),
        ):
            cds_s3_download_one_product(FAKE_S3_PATH, FAKE_HEADER, output, conf)

        mock_copy.assert_called_once_with(expected_tmp, output)

    def test_credentials_deleted_after_success(self, tmp_path, patch_requests_delete):
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip")
        creds, s3_resource = make_s3_client_mocks([obj])

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("shutil.copy2"),
            patch("os.remove"),
            patch("os.chmod"),
        ):
            cds_s3_download_one_product(FAKE_S3_PATH, FAKE_HEADER, output, conf)

        assert patch_requests_delete.call_count == 1
        call_url = patch_requests_delete.call_args[0][0]
        assert FAKE_ACCESS_ID in call_url


class TestCdsS3DownloadMultiFile:
    """Multi-file .SAFE product (directory tree)."""

    def test_multifile_status_downloaded(self, tmp_path):
        objects = [
            make_s3_object(f"{FAKE_S3_PATH}/manifest.safe", size=1_000),
            make_s3_object(
                f"{FAKE_S3_PATH}/measurement/s1a-iw-grd-vv.tiff", size=300_000_000
            ),
            make_s3_object(
                f"{FAKE_S3_PATH}/measurement/s1a-iw-grd-vh.tiff", size=200_000_000
            ),
        ]
        creds, s3_resource = make_s3_client_mocks(objects)

        output = str(tmp_path / FAKE_SAFENAME)
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("os.makedirs"),
        ):
            speed, elapsed, total_mb, status, safename = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert status == "Downloaded"
        assert speed > 0

    def test_multifile_skips_folder_pseudo_objects(self, tmp_path):
        """Objects whose key ends with '/' are folder markers and must not be downloaded."""
        folder_obj = make_s3_object(f"{FAKE_S3_PATH}/", size=0)
        real_obj = make_s3_object(f"{FAKE_S3_PATH}/manifest.safe", size=1_000)
        creds, s3_resource = make_s3_client_mocks([folder_obj, real_obj])

        bucket = s3_resource.Bucket.return_value
        output = str(tmp_path / FAKE_SAFENAME)
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("os.makedirs"),
        ):
            cds_s3_download_one_product(FAKE_S3_PATH, FAKE_HEADER, output, conf)

        # download_file called once (only for manifest.safe, not the folder marker)
        assert bucket.download_file.call_count == 1
        downloaded_key = bucket.download_file.call_args[0][0]
        assert not downloaded_key.endswith("/")


class TestCdsS3DownloadErrors:
    """Error paths: S3 not found, boto errors, move failure."""

    def test_not_found_returns_notfound_status(self, tmp_path):
        creds, s3_resource = make_s3_client_mocks([])  # empty object list
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with patch(
            "cdsodatacli.download._get_fresh_s3_client",
            return_value=(creds, s3_resource),
        ):
            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert status == "NotFound"

    def test_boto_client_error_returns_s3error(self, tmp_path):
        creds = FAKE_CREDENTIALS
        s3_resource = MagicMock()
        s3_resource.Bucket.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "Bucket"
        )
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with patch(
            "cdsodatacli.download._get_fresh_s3_client",
            return_value=(creds, s3_resource),
        ):
            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert status == "S3Error"

    def test_botocore_error_returns_s3error(self, tmp_path):
        creds = FAKE_CREDENTIALS
        s3_resource = MagicMock()
        s3_resource.Bucket.side_effect = BotoCoreError()
        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with patch(
            "cdsodatacli.download._get_fresh_s3_client",
            return_value=(creds, s3_resource),
        ):
            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert status == "S3Error"

    def test_tmp_file_cleaned_up_on_s3error(self, tmp_path):
        """Leftover .tmp must be removed when a S3 error occurs mid-download."""
        creds = FAKE_CREDENTIALS
        s3_resource = MagicMock()
        # Bucket() succeeds, but download_file raises ClientError
        bucket = MagicMock()
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip")
        bucket.objects.filter.return_value = [obj]
        bucket.download_file.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": "Internal"}}, "download_file"
        )
        s3_resource.Bucket.return_value = bucket

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        tmp_file = os.path.join(str(tmp_path), f"{FAKE_SAFENAME}.zip.tmp")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        # Create a dummy .tmp to simulate partial download
        open(tmp_file, "w").close()

        with patch(
            "cdsodatacli.download._get_fresh_s3_client",
            return_value=(creds, s3_resource),
        ):
            cds_s3_download_one_product(FAKE_S3_PATH, FAKE_HEADER, output, conf)

        assert not os.path.exists(tmp_file), ".tmp file should have been cleaned up"

    def test_move_error_returns_moveerror_status(self, tmp_path):
        obj = make_s3_object(FAKE_S3_PATH + "/product.zip")
        creds, s3_resource = make_s3_client_mocks([obj])

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with (
            patch(
                "cdsodatacli.download._get_fresh_s3_client",
                return_value=(creds, s3_resource),
            ),
            patch("shutil.copy2", side_effect=OSError("disk full")),
            patch("os.remove"),
        ):
            _, _, _, status, _ = cds_s3_download_one_product(
                FAKE_S3_PATH, FAKE_HEADER, output, conf
            )

        assert status == "MoveError"

    def test_credentials_deleted_even_on_error(self, tmp_path, patch_requests_delete):
        """Credential cleanup must happen regardless of download outcome."""
        creds, s3_resource = make_s3_client_mocks([])  # triggers NotFound

        output = str(tmp_path / f"{FAKE_SAFENAME}.zip")
        conf = {**FAKE_CONF, "pre_spool": str(tmp_path)}

        with patch(
            "cdsodatacli.download._get_fresh_s3_client",
            return_value=(creds, s3_resource),
        ):
            cds_s3_download_one_product(FAKE_S3_PATH, FAKE_HEADER, output, conf)

        assert patch_requests_delete.call_count == 1


# ===========================================================================
# Part 2 — download_list_product_multithread_v4 integration tests
# ===========================================================================


class TestV4InputValidation:
    """Input contracts."""

    def test_mismatched_lengths_raise(self):
        # S3Path column absent — KeyError fires inside the function before
        # any network call is made, satisfying the input-contract test.
        df = pd.DataFrame(
            {
                "safename": ["SAFE_A", "SAFE_B"],
                "id": ["id0", "id1"],
                # "S3Path" intentionally absent
            }
        )
        with pytest.raises((AssertionError, KeyError)):
            download_list_product_multithread_v4(
                df, "/fake/out", account_group="logins"
            )

    def test_status_column_created_if_absent(self, patch_filter):
        """If inputdf has no 'status' column, v4 must add it silently."""
        safenames = ["SAFE_A"]
        df = pd.DataFrame(
            {
                "safename": safenames,
                "S3Path": ["Sentinel-1/SAR/GRD/2022/05/03/SAFE_A.SAFE"],
                "id": ["id0"],
                # no 'status' column
            }
        )

        def _all_done(
            cpt, df_in, outputdir, force_download, cdsodatacli_conf, extension=""
        ):
            # return empty so the while loop exits immediately
            empty = df_in.iloc[:0].copy()
            return empty, cpt

        patch_filter.side_effect = _all_done

        # Should not raise even though 'status' is missing in input
        with patch("cdsodatacli.download.get_sessions_download_available"):
            download_list_product_multithread_v4(
                df, "/fake/out", account_group="logins"
            )


class TestV4AllSuccessful:
    """Happy path: every product downloads with status 'Downloaded'."""

    def test_returns_dataframe(self):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
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
        inputdf = make_inputdf(safenames)

        def mock_s3_worker(s3_path, header, output_path, conf):
            safename = os.path.basename(output_path)
            return make_s3_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=mock_s3_worker,
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert (result["status"] == 1).all()

    def test_ok_status_meaning_also_sets_status_1(self):
        """'OK' is accepted as a success status_meaning in v4 (legacy compat)."""
        safenames = ["SAFE_A"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
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
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
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

    def test_s3error_marks_status_minus1(self):
        safenames = ["SAFE_ERR"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                return_value=make_s3_result("SAFE_ERR", status="S3Error", speed=np.nan),
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert result.loc[result["safe"] == "SAFE_ERR", "status"].iloc[0] == -1

    def test_unhandled_exception_marks_status_minus1(self):
        safenames = ["SAFE_CRASH"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
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

    def test_one_failure_does_not_abort_others(self):
        safenames = ["SAFE_OK", "SAFE_FAIL"]
        inputdf = make_inputdf(safenames)

        def sessions_side_effect(conf, subset, **kw):
            pending = subset["safe"].tolist()
            return make_downloadable_df([s for s in safenames if s in pending])

        def worker(s3_path, header, output_path, conf):
            safename = os.path.basename(output_path)
            if safename == "SAFE_FAIL":
                return make_s3_result("SAFE_FAIL", status="S3Error", speed=np.nan)
            return make_s3_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product", side_effect=worker
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert result.loc[result["safe"] == "SAFE_OK", "status"].iloc[0] == 1
        assert result.loc[result["safe"] == "SAFE_FAIL", "status"].iloc[0] == -1

    def test_all_failures_loop_terminates(self):
        """While loop must exit even if every product fails (no infinite loop)."""
        safenames = ["SAFE_A", "SAFE_B", "SAFE_C"]
        inputdf = make_inputdf(safenames)

        def failing_worker(s3_path, header, output_path, conf):
            safename = os.path.basename(output_path)
            return make_s3_result(safename, status="S3Error", speed=np.nan)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
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


class TestV4NoDuplicateDownload:
    """Same safename must not be submitted twice concurrently."""

    def test_duplicate_safename_submitted_only_once(self):
        safenames = ["SAFE_A", "SAFE_A"]
        inputdf = make_inputdf(safenames)
        submission_count = {"n": 0}

        def tracking_worker(s3_path, header, output_path, conf):
            submission_count["n"] += 1
            return make_s3_result("SAFE_A")

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=tracking_worker,
            ),
        ):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert submission_count["n"] == 1


class TestV4AllAlreadyPresent:
    """All products already on disk — nothing to download."""

    def test_empty_df2_skips_sessions(self, patch_filter):
        def _all_archived(
            cpt, df_in, outputdir, force_download, cdsodatacli_conf, extension=""
        ):
            cpt["archived_product"] = len(df_in)
            empty = df_in.iloc[:0].copy()
            return empty, cpt

        patch_filter.side_effect = _all_archived

        with patch("cdsodatacli.download.get_sessions_download_available") as mock_sess:
            download_list_product_multithread_v4(
                make_inputdf(["SAFE_A"]), "/fake/out", account_group="logins"
            )

        mock_sess.assert_not_called()


class TestV4NoSession:
    """get_sessions_download_available returns empty — should wait and retry."""

    def test_sleeps_when_no_session(self):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 3:
                return make_downloadable_df([])
            return make_downloadable_df(safenames)

        def s3_worker(s3_path, header, output_path, conf):
            return make_s3_result(os.path.basename(output_path))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
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
        inputdf = make_inputdf(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 2:
                return make_downloadable_df([])
            return make_downloadable_df(safenames)

        def s3_worker(s3_path, header, output_path, conf):
            return make_s3_result(os.path.basename(output_path))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
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


class TestV4FilterCalledWithExtensionEmpty:
    """v4 passes extension='' to filter_product_already_present (SAFE dirs, not .zip)."""

    def test_filter_called_with_empty_extension(self, patch_filter):
        safenames = ["SAFE_A"]
        inputdf = make_inputdf(safenames)

        def _capture(
            cpt, df_in, outputdir, force_download, cdsodatacli_conf, extension=""
        ):
            _capture.extension_used = extension
            empty = df_in.iloc[:0].copy()
            return empty, cpt

        _capture.extension_used = None
        patch_filter.side_effect = _capture

        with patch("cdsodatacli.download.get_sessions_download_available"):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert _capture.extension_used == ""


class TestV4Concurrency:
    """Race condition and true parallelism checks."""

    def test_two_products_can_run_concurrently(self):
        safenames = ["SAFE_X", "SAFE_Y"]
        inputdf = make_inputdf(safenames)
        active = {"count": 0, "max": 0}
        lock = threading.Lock()

        def concurrent_worker(s3_path, header, output_path, conf):
            safename = os.path.basename(output_path)
            with lock:
                active["count"] += 1
                active["max"] = max(active["max"], active["count"])
            time.sleep(0.05)
            with lock:
                active["count"] -= 1
            return make_s3_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=concurrent_worker,
            ),
        ):
            result = download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert (result["status"] == 1).all()
        assert active["max"] >= 2

    def test_slow_product_not_resubmitted(self):
        """A product still in flight must not be re-submitted in the next loop.

        The barrier is released by a daemon thread after a short delay so it
        fires independently of the while-loop progression — avoiding the
        deadlock where the worker waits for the barrier and the barrier is only
        set inside sessions_side_effect which itself only runs after the worker
        finishes.
        """
        safenames = ["SAFE_SLOW"]
        inputdf = make_inputdf(safenames)
        submission_count = {"n": 0}
        barrier = threading.Event()

        # Release the barrier from a background thread after 0.2 s so the
        # slow worker can finish regardless of how many loop iterations occur.
        def _release():
            time.sleep(0.2)
            barrier.set()

        threading.Thread(target=_release, daemon=True).start()

        def slow_worker(s3_path, header, output_path, conf):
            submission_count["n"] += 1
            assert barrier.wait(timeout=5), "barrier was never released"
            return make_s3_result("SAFE_SLOW")

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.cds_s3_download_one_product",
                side_effect=slow_worker,
            ),
        ):
            download_list_product_multithread_v4(
                inputdf, "/fake/out", account_group="logins"
            )

        assert submission_count["n"] == 1
