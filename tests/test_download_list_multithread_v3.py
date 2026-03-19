"""
pytest unit tests for download_list_product_multithread_v3
run with: pytest test_download_multithread_v3.py -v
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

# Format: CDSE_access_token_<login>_<date>.txt
# split("_") -> [0]=CDSE [1]=access [2]=token [3]=login [4]=date.txt
FAKE_LOGIN = "user@example.com"
FAKE_DATE_STR = "20240101t120000"
# FAKE_SEMAPHORE = f"/fake/token_dir/CDSE_access_token_{FAKE_LOGIN}_{FAKE_DATE_STR}.txt"


def make_future_result(safename, status="OK", speed=10.0):
    """Return a tuple as CDS_Odata_download_one_product_v2 would."""
    return (speed, status, safename)


# def make_future_result(s):
#     # The 3rd value must be the string 's'
#     return (1.0, "OK", s, FAKE_SEMAPHORE)


def make_df2(safenames):
    """Build a minimal df2 as filter_product_already_present would return."""
    n = len(safenames)
    df = pd.DataFrame(
        {
            "safe": safenames,
            "status": np.zeros(n),
            "id": [f"id-{i}" for i in range(n)],
            "login": FAKE_LOGIN,
            "outputpath": [f"/fake/out/{s}.zip" for s in safenames],
        }
    )
    return df


def make_downloadable_df(safenames):
    """Simulate the output of get_sessions_download_available."""
    n = len(safenames)
    sessions = [MagicMock() for _ in range(n)]
    df = pd.DataFrame(
        {
            "safe": safenames,
            "session": sessions,
            "header": [{"Authorization": "Bearer tok"} for _ in range(n)],
            "url": [f"https://fake.cdse/{s}" for s in safenames],
            "login": FAKE_LOGIN,
            "output_path": [f"/fake/out/{s}.zip" for s in safenames],
            # "token_semaphore": [FAKE_SEMAPHORE for _ in range(n)],
        }
    )
    return df


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def patch_conf(monkeypatch):
    """Always return FAKE_CONF from get_conf."""
    with patch("cdsodatacli.download.get_conf", return_value=FAKE_CONF):
        yield


@pytest.fixture(autouse=True)
def patch_filter(monkeypatch):
    """
    By default filter_product_already_present returns all products as
    to-download (status=0).  Individual tests can override this.
    """
    with patch("cdsodatacli.download.filter_product_already_present") as mock:

        def _default(cpt, df, outputdir, force_download, cdsodatacli_conf):
            cpt["product_absent_from_local_disks"] = len(df)
            return make_df2(df["safe"].tolist()), cpt

        mock.side_effect = _default
        yield mock


@pytest.fixture(autouse=True)
def patch_semaphores():
    with (
        # patch("cdsodatacli.download.remove_semaphore_token_file") as tok,
        patch("cdsodatacli.download.remove_semaphore_session_file") as sess,
    ):
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
                list_id=["id0", "id1"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert isinstance(result, pd.DataFrame)

    def test_all_status_1(self):
        safenames = ["SAFE_A", "SAFE_B"]

        # Define a side effect function instead of a list
        # This will return a result for the safename requested,
        # no matter how many times it is called.
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
                side_effect=mock_download_side_effect,  # Use the function here
            ),
        ):
            result = download_list_product_multithread_v3(
                list_id=["id0", "id1"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )

        # Now that the mock doesn't crash, we check that all
        # unique products eventually succeeded.
        assert (result["status"] == 1).all()


class TestDownloadError:
    """One product fails with a non-OK status."""

    def test_failed_product_status_minus1(self):
        safenames = ["SAFE_OK", "SAFE_FAIL"]
        results = [
            make_future_result("SAFE_OK", status="OK", speed=3.45),
            make_future_result("SAFE_FAIL", status="Unauthorized", speed=np.nan),
        ]
        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                return_value=make_downloadable_df(safenames),
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                side_effect=results,
            ),
        ):
            result = download_list_product_multithread_v3(
                list_id=["id0", "id1"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_FAIL", "status"].iloc[0] == -1
        assert result.loc[result["safe"] == "SAFE_OK", "status"].iloc[0] == 1


class TestWorkerException:
    """Worker raises an unhandled exception (e.g. CalledProcessError)."""

    def test_exception_marks_product_as_error(self):
        safenames = ["SAFE_CRASH"]
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
                list_id=["id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_CRASH", "status"].iloc[0] == -1

    def test_other_products_continue_after_exception(self):
        """A crash on one product must not abort the others."""
        safenames = ["SAFE_CRASH", "SAFE_OK"]

        def sessions_side_effect(conf, subset, **kw):
            # return only products still pending in subset
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
                list_id=["id0", "id1"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_OK", "status"].iloc[0] == 1
        assert result.loc[result["safe"] == "SAFE_CRASH", "status"].iloc[0] == -1


class TestNoDuplicateDownload:
    """Same product must not be submitted twice concurrently."""

    def test_duplicate_not_submitted(self):
        """currently_downloading must block re-submission of the same safename."""
        safenames = ["SAFE_A", "SAFE_A"]  # duplicate in listing
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
                list_id=["id0", "id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        # despite two entries in the listing, the worker must be called only once
        assert submission_count["n"] == 1


class TestAllAlreadyPresent:
    """All products already on disk — nothing to download."""

    def test_empty_df2_returns_immediately(self, patch_filter):
        def _all_archived(cpt, df, outputdir, force_download, cdsodatacli_conf):
            cpt["archived_product"] = len(df)
            empty = pd.DataFrame({"safe": [], "status": [], "id": []})
            return empty, cpt

        patch_filter.side_effect = _all_archived

        with patch("cdsodatacli.download.get_sessions_download_available") as mock_sess:
            download_list_product_multithread_v3(
                list_id=["id0"],
                list_safename=["SAFE_A"],
                outputdir="/fake/out",
                account_group="logins",
            )
        mock_sess.assert_not_called()


class TestNoSessionAvailable:
    """get_sessions_download_available returns empty — should wait and retry."""

    def test_sleeps_when_no_session(self):
        safenames = ["SAFE_A"]
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 3:
                return make_downloadable_df([])  # empty first 2 calls
            return make_downloadable_df(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                return_value=make_future_result("SAFE_A"),
            ),
            patch("cdsodatacli.download.time.sleep") as mock_sleep,
        ):
            download_list_product_multithread_v3(
                list_id=["id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert mock_sleep.call_count >= 2


class TestAssertLengths:
    """list_id and list_safename must have the same length."""

    def test_mismatched_lengths_raise(self):
        with pytest.raises(AssertionError):
            download_list_product_multithread_v3(
                list_id=["id0", "id1"],
                list_safename=["SAFE_A"],
                outputdir="/fake/out",
                account_group="logins",
            )


# ---------------------------------------------------------------------------
# NEW: Race condition tests
# ---------------------------------------------------------------------------


class TestRaceCondition:
    """
    Verify that two threads cannot write the same .tmp file simultaneously
    and that currently_downloading prevents double submission.
    """

    def test_same_safename_submitted_only_once_across_loops(self):
        """
        Simulate a slow first download: the product is still running when the
        outer while loop fires again.  It must NOT be re-submitted.
        """
        safenames = ["SAFE_SLOW"]
        submission_count = {"n": 0}
        barrier = threading.Event()

        def slow_worker(session, header, url, output_path, **kw):
            submission_count["n"] += 1
            barrier.wait(timeout=2)  # block until test releases it
            return make_future_result("SAFE_SLOW")

        loop_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            loop_count["n"] += 1
            if loop_count["n"] == 1:
                barrier.set()  # release the worker on 2nd loop entry
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
                list_id=["id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        # despite multiple loop iterations, the worker must be called only once
        assert submission_count["n"] == 1

    def test_two_different_products_can_run_concurrently(self):
        """
        Two distinct products must both be submitted and both reach status=1.
        """
        safenames = ["SAFE_X", "SAFE_Y"]
        active_at_same_time = {"count": 0, "max": 0}
        lock = threading.Lock()

        def concurrent_worker(session, header, url, output_path, **kw):
            safename = os.path.basename(output_path).replace(".zip", "")
            with lock:
                active_at_same_time["count"] += 1
                active_at_same_time["max"] = max(
                    active_at_same_time["max"], active_at_same_time["count"]
                )
            time.sleep(0.05)  # simulate real I/O overlap
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
                list_id=["id0", "id1"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert (result["status"] == 1).all()
        # both workers overlapped at least once (proves true parallelism)
        assert active_at_same_time["max"] >= 2

    def test_tmp_file_not_moved_twice(self):
        """
        If two futures for the same product somehow complete (should not happen
        after the fix), the second shutil.copy2 must not crash the daemon —
        the FileNotFoundError on the already-removed .tmp must be caught.
        """
        safenames = ["SAFE_RACE"]

        # Simulate: first call succeeds, second finds .tmp already gone
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
            # should not raise — the exception must be caught by the except block
            result = download_list_product_multithread_v3(
                list_id=["id0", "id0"],
                list_safename=safenames * 2,
                outputdir="/fake/out",
                account_group="logins",
            )
        # at least one attempt succeeded
        assert (result["status"] != 0).any()


# ---------------------------------------------------------------------------
# NEW: Server not answering / network failure tests
# ---------------------------------------------------------------------------


class TestServerNotAnswering:
    """
    Simulate various network-level failures: timeout, connection refused,
    chunked encoding error, and HTTP 5xx.
    """

    def _run_with_worker_error(self, exc, safenames=None):
        safenames = safenames or ["SAFE_NET"]
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
                list_id=["id0"] * len(safenames),
                list_safename=safenames,
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
        """ChunkedEncodingError is handled inside the worker but we test the
        outer layer too in case the worker itself raises unexpectedly."""
        result = self._run_with_worker_error(ChunkedEncodingError("chunked"))
        assert result.loc[result["safe"] == "SAFE_NET", "status"].iloc[0] == -1

    def test_network_error_does_not_crash_daemon(self):
        """Other products must still succeed even if one hits a network error."""
        safenames = ["SAFE_TIMEOUT", "SAFE_OK"]

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
                list_id=["id0", "id1"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_OK", "status"].iloc[0] == 1
        assert result.loc[result["safe"] == "SAFE_TIMEOUT", "status"].iloc[0] == -1

    def test_http_503_returned_as_status_meaning(self):
        """
        Worker returns a non-OK status_meaning (e.g. 'Service Unavailable')
        rather than raising — product must be marked -1, not retried forever.
        """
        safenames = ["SAFE_503"]
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
                list_id=["id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_503", "status"].iloc[0] == -1

    def test_all_workers_timeout_loop_eventually_exits(self):
        """
        If every product fails, the while loop must terminate (no infinite loop).
        """
        safenames = ["SAFE_A", "SAFE_B", "SAFE_C"]
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
                list_id=["id0", "id1", "id2"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        # all must be settled (1 or -1), none stuck at 0
        assert (result["status"] != 0).all()


# ---------------------------------------------------------------------------
# NEW: Extended no-session / throttling tests
# ---------------------------------------------------------------------------


class TestNoSessionExtended:
    """
    More thorough coverage of the 'no session available' branch,
    including prolonged starvation and eventual recovery.
    """

    def test_sleep_duration_is_correct(self):
        """The code sleeps 5 seconds when no session is available."""
        safenames = ["SAFE_A"]
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] < 2:
                return make_downloadable_df([])
            return make_downloadable_df(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                return_value=make_future_result("SAFE_A"),
            ),
            patch("cdsodatacli.download.time.sleep") as mock_sleep,
        ):
            download_list_product_multithread_v3(
                list_id=["id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        # each sleep call must use exactly 5 seconds
        for c in mock_sleep.call_args_list:
            assert c.args[0] == 5

    def test_many_empty_session_rounds_then_success(self):
        """
        Session starved for 10 rounds, then becomes available.
        Product must eventually reach status=1.
        """
        safenames = ["SAFE_STARVED"]
        call_count = {"n": 0}

        def sessions_side_effect(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] <= 10:
                return make_downloadable_df([])
            return make_downloadable_df(safenames)

        with (
            patch(
                "cdsodatacli.download.get_sessions_download_available",
                side_effect=sessions_side_effect,
            ),
            patch(
                "cdsodatacli.download.CDS_Odata_download_one_product_v2",
                return_value=make_future_result("SAFE_STARVED"),
            ),
            patch("cdsodatacli.download.time.sleep"),
        ):
            result = download_list_product_multithread_v3(
                list_id=["id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        assert result.loc[result["safe"] == "SAFE_STARVED", "status"].iloc[0] == 1

    def test_no_session_does_not_submit_any_future(self):
        """When sessions are empty, no future must be submitted to the executor."""
        safenames = ["SAFE_A"]
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
                list_id=["id0"],
                list_safename=safenames,
                outputdir="/fake/out",
                account_group="logins",
            )
        # worker called exactly once (only after session became available)
        assert len(submitted) == 1
