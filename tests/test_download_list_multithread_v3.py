"""
pytest unit tests for download_list_product_multithread_v3
run with: pytest test_download_multithread_v3.py -v

Signature updated to:
    download_list_product_multithread_v3(
        inputdf,
        outputdir,
        account_group,
        hideprogressbar=False,
        check_on_disk=True,
        cdsodatacli_conf_file=None,
    )
"""

import os
import time
import threading
import pytest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_CONF = {
    "token_directory": "/fake/token_dir",
    "active_session_directory": "/fake/session_dir",
    "pre_spool": "/fake/pre_spool",
    "spool": "/fake/spool",
    "URL_download": "https://fake.cdse/odata/v1/Products(%s)/$value",
    "logins": {
        "user1@emailadrress.fr": "passwd1",
        "user2@emailadrress.fr": "passwd2",
    },
}

FAKE_LOGIN = "user@example.com"


def make_inputdf(safenames, ids=None, statuses=None):
    """Build a minimal inputdf as download_list_product_multithread_v3 now expects.
    Uses 'safename' column — v3 (like v4) accepts 'safename' and renames to 'safe'
    internally before passing to filter_product_already_present.
    """
    n = len(safenames)
    if ids is None:
        ids = [f"id-{i}" for i in range(n)]
    if statuses is None:
        statuses = np.zeros(n)
    return pd.DataFrame(
        {
            "safename": safenames,
            "id": ids,
            "status": statuses,
        }
    )


def make_future_result(safename, status="OK", speed=10.0):
    """Return a tuple as CDS_Odata_download_one_product_v2 would."""
    return (speed, status, safename)


def make_df2(safenames):
    """Build a minimal df2 as filter_product_already_present would return."""
    n = len(safenames)
    return pd.DataFrame(
        {
            "safe": safenames,
            "status": np.zeros(n),
            "id": [f"id-{i}" for i in range(n)],
            "login": FAKE_LOGIN,
            "outputpath": [f"/fake/out/{s}.zip" for s in safenames],
        }
    )


def make_downloadable_df(safenames):
    """Simulate the output of get_sessions_download_available."""
    n = len(safenames)
    return pd.DataFrame(
        {
            "safe": safenames,
            "session": [MagicMock() for _ in range(n)],
            "header": [{"Authorization": "Bearer tok"} for _ in range(n)],
            "url": [f"https://fake.cdse/{s}" for s in safenames],
            "login": FAKE_LOGIN,
            "output_path": [f"/fake/out/{s}.zip" for s in safenames],
        }
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def patch_conf():
    """Always return FAKE_CONF from get_conf."""
    with patch("cdsodatacli.download.get_conf", return_value=FAKE_CONF):
        yield


@pytest.fixture(autouse=True)
def patch_filter():
    """
    By default filter_product_already_present returns all products as
    to-download (status=0).  Individual tests can override this.
    """
    with patch("cdsodatacli.download.filter_product_already_present") as mock:

        def _default(cpt, df, outputdir, force_download, cdsodatacli_conf, **kwargs):
            cpt["product_absent_from_local_disks"] = len(df)
            return make_df2(df["safe"].tolist()), cpt

        mock.side_effect = _default
        yield mock


@pytest.fixture(autouse=True)
def patch_semaphores():
    with patch("cdsodatacli.download.remove_semaphore_session_file") as sess:
        yield sess


@pytest.fixture(autouse=True)
def patch_tqdm():
    """Silence tqdm output during tests."""
    with patch("cdsodatacli.download.tqdm", side_effect=lambda *a, **kw: MagicMock()):
        yield


# ---------------------------------------------------------------------------
# Import after patching so module-level calls don't fail
# ---------------------------------------------------------------------------

from cdsodatacli.download import download_list_product_multithread_v3  # noqa: E402


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAllSuccessful:
    """Happy path: every product downloads successfully on first attempt."""

    def test_returns_dataframe(self):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=[make_future_result(s) for s in safenames],
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert isinstance(result, pd.DataFrame)

    def test_all_status_1(self):
        safenames = ["SAFE_A", "SAFE_B"]
        inputdf = make_inputdf(safenames)

        def mock_download_side_effect(session, header, url, output_path, **kw):
            safename = os.path.basename(output_path).replace(".zip", "")
            return make_future_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=mock_download_side_effect,
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )

        assert (result["status"] == 1).all()


class TestInputValidation:
    """Input contracts."""

    def test_mismatched_safe_id_lengths_raise(self):
        """inputdf missing required column triggers KeyError/AssertionError
        before any network call is made."""
        df = pd.DataFrame(
            {
                "safename": ["SAFE_A", "SAFE_B"],
                # "id" intentionally absent — function must raise before network calls
            }
        )
        with pytest.raises((AssertionError, KeyError)):
            download_list_product_multithread_v3(
                inputdf=df,
                outputdir="/fake/out",
                account_group="logins",
            )

    def test_status_column_created_if_absent(self, patch_filter):
        """If inputdf has no 'status' column, v3 must add it silently."""
        df = pd.DataFrame(
            {
                "safename": ["SAFE_A"],
                "id": ["id0"],
                # no 'status' column
            }
        )

        def _all_done(
            cpt, df_in, outputdir, force_download, cdsodatacli_conf, **kwargs
        ):
            empty = df_in.iloc[:0].copy()
            return empty, cpt

        patch_filter.side_effect = _all_done

        with patch("cdsodatacli.download.get_sessions_download_available"):
            # Should not raise even though 'status' is missing in input
            download_list_product_multithread_v3(
                inputdf=df,
                outputdir="/fake/out",
                account_group="logins",
            )


class TestDownloadError:
    """One product fails with a non-OK status."""

    def test_failed_product_status_minus1(self):
        safenames = ["SAFE_OK", "SAFE_FAIL"]
        inputdf = make_inputdf(safenames)

        def sessions_side_effect(conf, subset, **kw):
            pending = subset["safe"].tolist()
            return make_downloadable_df([s for s in safenames if s in pending])

        def worker_side_effect(session, header, url, output_path, **kw):
            safename = os.path.basename(output_path).replace(".zip", "")
            if safename == "SAFE_FAIL":
                return make_future_result(
                    "SAFE_FAIL", status="Unauthorized", speed=np.nan
                )
            return make_future_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=worker_side_effect,
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_FAIL", "status"].iloc[0] == -1
        assert result.loc[result["safe"] == "SAFE_OK", "status"].iloc[0] == 1


class TestWorkerException:
    """Worker raises an unhandled exception."""

    def test_exception_marks_product_as_error(self):
        safenames = ["SAFE_CRASH"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=RuntimeError("disk full"),
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_CRASH", "status"].iloc[0] == -1

    def test_other_products_continue_after_exception(self):
        """A crash on one product must not abort the others."""
        safenames = ["SAFE_CRASH", "SAFE_OK"]
        inputdf = make_inputdf(safenames)

        def sessions_side_effect(conf, subset, **kw):
            pending = subset["safe"].tolist()
            return make_downloadable_df([s for s in safenames if s in pending])

        def worker_side_effect(session, header, url, output_path, **kw):
            safename = os.path.basename(output_path).replace(".zip", "")
            if safename == "SAFE_CRASH":
                raise RuntimeError("boom")
            return make_future_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=worker_side_effect,
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_OK", "status"].iloc[0] == 1
        assert result.loc[result["safe"] == "SAFE_CRASH", "status"].iloc[0] == -1


class TestNoDuplicateDownload:
    """Same product must not be submitted twice concurrently."""

    def test_duplicate_not_submitted(self):
        """currently_downloading must block re-submission of the same safename."""
        safenames = ["SAFE_A", "SAFE_A"]
        inputdf = make_inputdf(safenames, ids=["id0", "id0"])
        submission_count = {"n": 0}

        def tracking_worker(session, header, url, output_path, **kw):
            submission_count["n"] += 1
            safename = os.path.basename(output_path).replace(".zip", "")
            return make_future_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=tracking_worker,
            ),
        ):
            download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert submission_count["n"] == 1


class TestAllAlreadyPresent:
    """All products already on disk — nothing to download."""

    def test_empty_df2_returns_immediately(self, patch_filter):
        def _all_archived(
            cpt, df, outputdir, force_download, cdsodatacli_conf, **kwargs
        ):
            cpt["archived_product"] = len(df)
            empty = pd.DataFrame({"safe": [], "status": [], "id": []})
            return empty, cpt

        patch_filter.side_effect = _all_archived

        with patch("cdsodatacli.download.get_sessions_download_available") as mock_sess:
            download_list_product_multithread_v3(
                inputdf=make_inputdf(["SAFE_A"]),
                outputdir="/fake/out",
                account_group="logins",
            )
        mock_sess.assert_not_called()


class TestNoSessionAvailable:
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

        def odata_worker(session, header, url, output_path, **kw):
            return make_future_result(os.path.basename(output_path).replace(".zip", ""))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=odata_worker,
            ),
            patch("cdsodatacli.download.time.sleep") as mock_sleep,
        ):
            download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert mock_sleep.call_count >= 2


# ---------------------------------------------------------------------------
# Race condition tests
# ---------------------------------------------------------------------------


class TestRaceCondition:
    """
    Verify that currently_downloading prevents double submission and that
    true concurrency is achieved for distinct products.
    """

    def test_same_safename_submitted_only_once_across_loops(self):
        """
        Simulate a slow first download: the product is still running when the
        outer while loop fires again.  It must NOT be re-submitted.
        """
        safenames = ["SAFE_SLOW"]
        inputdf = make_inputdf(safenames)
        submission_count = {"n": 0}
        barrier = threading.Event()

        # Release the barrier from a daemon thread — avoids the deadlock
        # where the worker waits for the barrier but the barrier is set
        # inside sessions_side_effect which only runs after the worker finishes.
        def _release():
            time.sleep(0.2)
            barrier.set()

        threading.Thread(target=_release, daemon=True).start()

        def slow_worker(session, header, url, output_path, **kw):
            submission_count["n"] += 1
            assert barrier.wait(timeout=5), "barrier was never released"
            return make_future_result("SAFE_SLOW")

        def sessions_side_effect(*a, **kw):
            return make_downloadable_df(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=slow_worker,
            ),
        ):
            download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert submission_count["n"] == 1

    def test_two_different_products_can_run_concurrently(self):
        """Two distinct products must both be submitted and both reach status=1."""
        safenames = ["SAFE_X", "SAFE_Y"]
        inputdf = make_inputdf(safenames)
        active_at_same_time = {"count": 0, "max": 0}
        lock = threading.Lock()

        def concurrent_worker(session, header, url, output_path, **kw):
            safename = os.path.basename(output_path).replace(".zip", "")
            with lock:
                active_at_same_time["count"] += 1
                active_at_same_time["max"] = max(
                    active_at_same_time["max"], active_at_same_time["count"]
                )
            time.sleep(0.05)
            with lock:
                active_at_same_time["count"] -= 1
            return make_future_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=concurrent_worker,
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert (result["status"] == 1).all()
        assert active_at_same_time["max"] >= 2

    def test_tmp_file_not_moved_twice(self):
        """
        If two futures for the same product somehow complete, the second
        FileNotFoundError on the already-removed .tmp must be caught gracefully.
        """
        safenames = ["SAFE_RACE"]
        inputdf = make_inputdf(safenames * 2, ids=["id0", "id0"])
        call_count = {"n": 0}

        def worker_with_race(session, header, url, output_path, **kw):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise FileNotFoundError("tmp already moved by first thread")
            return make_future_result("SAFE_RACE")

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames * 2),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=worker_with_race,
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert (result["status"] != 0).any()


# ---------------------------------------------------------------------------
# Server not answering / network failure tests
# ---------------------------------------------------------------------------


class TestServerNotAnswering:
    """
    Simulate various network-level failures: timeout, connection refused,
    chunked encoding error, and HTTP 5xx.
    """

    def _run_with_worker_error(self, exc, safenames=None):
        safenames = safenames or ["SAFE_NET"]
        inputdf = make_inputdf(safenames)
        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=exc,
            ),
        ):
            return download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )

    def test_connection_error_marks_product_failed(self):
        result = self._run_with_worker_error(ConnectionError("refused"))
        assert result.loc[result["safe"] == "SAFE_NET", "status"].iloc[0] == -1

    def test_timeout_marks_product_failed(self):
        result = self._run_with_worker_error(Timeout("timed out"))
        assert result.loc[result["safe"] == "SAFE_NET", "status"].iloc[0] == -1

    def test_chunked_encoding_error_marks_product_failed(self):
        result = self._run_with_worker_error(ChunkedEncodingError("chunked"))
        assert result.loc[result["safe"] == "SAFE_NET", "status"].iloc[0] == -1

    def test_network_error_does_not_crash_daemon(self):
        """Other products must still succeed even if one hits a network error."""
        safenames = ["SAFE_TIMEOUT", "SAFE_OK"]
        inputdf = make_inputdf(safenames)

        def sessions_side_effect(conf, subset, **kw):
            pending = subset["safe"].tolist()
            return make_downloadable_df([s for s in safenames if s in pending])

        def worker(session, header, url, output_path, **kw):
            safename = os.path.basename(output_path).replace(".zip", "")
            if safename == "SAFE_TIMEOUT":
                raise Timeout("server did not respond")
            return make_future_result(safename)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=worker,
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_OK", "status"].iloc[0] == 1
        assert result.loc[result["safe"] == "SAFE_TIMEOUT", "status"].iloc[0] == -1

    def test_http_503_returned_as_status_meaning(self):
        """Non-OK status_meaning must mark product -1, not retry forever."""
        safenames = ["SAFE_503"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                return_value=make_future_result(
                    "SAFE_503", status="Service Unavailable", speed=np.nan
                ),
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_503", "status"].iloc[0] == -1

    def test_all_workers_timeout_loop_eventually_exits(self):
        """If every product fails, the while loop must terminate."""
        safenames = ["SAFE_A", "SAFE_B", "SAFE_C"]
        inputdf = make_inputdf(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=Timeout("all down"),
            ),
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert (result["status"] != 0).all()


# ---------------------------------------------------------------------------
# Extended no-session / throttling tests
# ---------------------------------------------------------------------------


class TestNoSessionExtended:
    """More thorough coverage of the 'no session available' branch."""

    def test_sleep_duration_is_correct(self):
        """The code sleeps 5 seconds when no session is available."""
        safenames = ["SAFE_A"]
        inputdf = make_inputdf(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 2:
                return make_downloadable_df([])
            return make_downloadable_df(safenames)

        def odata_worker(session, header, url, output_path, **kw):
            return make_future_result(os.path.basename(output_path).replace(".zip", ""))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=odata_worker,
            ),
            patch("cdsodatacli.download.time.sleep") as mock_sleep,
        ):
            download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        for c in mock_sleep.call_args_list:
            assert c.args[0] == 5

    def test_many_empty_session_rounds_then_success(self):
        """Session starved for 10 rounds then recovers — product must reach status=1."""
        safenames = ["SAFE_STARVED"]
        inputdf = make_inputdf(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] <= 10:
                return make_downloadable_df([])
            return make_downloadable_df(safenames)

        def odata_worker(session, header, url, output_path, **kw):
            return make_future_result(os.path.basename(output_path).replace(".zip", ""))

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=odata_worker,
            ),
            patch("cdsodatacli.download.time.sleep"),  # don't actually sleep 10×5s
        ):
            result = download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_STARVED", "status"].iloc[0] == 1

    def test_no_session_does_not_submit_any_future(self):
        """When sessions are empty, no future must be submitted to the executor."""
        safenames = ["SAFE_A"]
        inputdf = make_inputdf(safenames)
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 2:
                return make_downloadable_df([])
            return make_downloadable_df(safenames)

        submitted = []

        def tracking_worker(*a, **kw):
            submitted.append(1)
            return make_future_result("SAFE_A")

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=tracking_worker,
            ),
            patch("cdsodatacli.download.time.sleep"),
        ):
            download_list_product_multithread_v3(
                inputdf=inputdf,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert len(submitted) == 1
