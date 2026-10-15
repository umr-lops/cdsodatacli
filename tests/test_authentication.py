import datetime

from cdsodatacli.fetch_access_token import (
    _ACCESS_TOKEN_CACHE,
    get_access_token,
)
from cdsodatacli.utils import get_conf
from dotenv import load_dotenv
import os
import pytest
import requests

load_dotenv()
conf = get_conf()


@pytest.fixture(autouse=True)
def _reset_token_cache():
    """Ensure the in-memory token cache does not leak between tests."""
    _ACCESS_TOKEN_CACHE.clear()
    yield
    _ACCESS_TOKEN_CACHE.clear()


def has_live_creds():
    if os.getenv("DEFAULT_LOGIN_CDSE") and os.getenv("DEFAULT_PASSWD_CDSE"):
        return True
    default_login = conf.get("default_login", {})
    return bool(default_login)


# # test with live credentials: removed because CI is failing and it is not a good practice to have live tests in unit tests
# @pytest.mark.skipif(not has_live_creds(), reason="No CDS credentials available for live auth test")
# def test_get_access_token_using_urllib3_and_requests():
#     login_cdse = os.getenv("DEFAULT_LOGIN_CDSE", None)
#     passwd = os.getenv("DEFAULT_PASSWD_CDSE", None)
#     # for local test -> use the localconfig.yml/config.yml files
#     if login_cdse is None or passwd is None:
#         print("using cdsodatacli localconfig.yml/config.yml for login")
#         default_login = conf.get("default_login", {})
#         login_cdse, passwd = list(default_login.items())[0]
#     headers = get_access_token(email=login_cdse, password=passwd)
#     assert "Authorization" in headers
#     assert headers["Authorization"].startswith("Bearer ")
#     assert "Accept" in headers
#     assert headers["Accept"] == "application/json"


# mock a 401 Unauthorized response
def test_get_access_token_raises_on_401(monkeypatch):
    # make requests.post raise an HTTPError or return 401 response
    class DummyResp:
        def raise_for_status(self):
            raise requests.exceptions.HTTPError("401 Client Error: Unauthorized")

    from cdsodatacli import retry as _retry

    # The token request is retried with backoff; skip the sleeps to keep the test fast
    monkeypatch.setattr(_retry.time, "sleep", lambda s: None)
    monkeypatch.setattr("requests.post", lambda *a, **k: DummyResp())

    with pytest.raises(requests.exceptions.HTTPError):
        get_access_token("baduser@example.com", "badpassword")


# mock a successful response
def test_get_access_token_success(monkeypatch):
    class DummyRespOK:
        def raise_for_status(self):  # no-op
            return None

        def json(self):
            return {"access_token": "TESTTOKEN1234567890"}

    monkeypatch.setattr("requests.post", lambda *a, **k: DummyRespOK())

    headers = get_access_token("user", "pass")
    assert "Authorization" in headers
    assert headers["Authorization"].startswith("Bearer TESTTOKEN")
    assert headers["Accept"] == "application/json"


class _RespOK:
    def raise_for_status(self):
        return None

    def json(self):
        return {"access_token": "TESTTOKEN1234567890"}


class _RespError:
    def __init__(self, message):
        self._message = message

    def raise_for_status(self):
        raise requests.exceptions.HTTPError(self._message)


def test_get_access_token_reuses_cached_token(monkeypatch):
    """A still-valid token must be reused, not re-fetched (issue #152)."""
    calls = {"n": 0}

    def fake_post(*a, **k):
        calls["n"] += 1
        return _RespOK()

    monkeypatch.setattr("requests.post", fake_post)

    headers = get_access_token("user@ex.com", "pass")
    headers2 = get_access_token("user@ex.com", "pass")

    assert calls["n"] == 1
    assert headers == headers2


def test_get_access_token_cache_is_per_email(monkeypatch):
    calls = {"n": 0}

    def fake_post(*a, **k):
        calls["n"] += 1
        return _RespOK()

    monkeypatch.setattr("requests.post", fake_post)

    get_access_token("a@ex.com", "pass")
    get_access_token("b@ex.com", "pass")

    assert calls["n"] == 2


def test_get_access_token_refetches_when_expired(monkeypatch):
    monkeypatch.setattr("requests.post", lambda *a, **k: _RespOK())
    get_access_token("user@ex.com", "pass")

    # Force the cached token to look older than its validity window
    _ACCESS_TOKEN_CACHE["user@ex.com"][
        "creation-date"
    ] = datetime.datetime.now() - datetime.timedelta(hours=1)
    get_access_token("user@ex.com", "pass")

    assert _ACCESS_TOKEN_CACHE["user@ex.com"]["creation-date"] is not None


def test_request_access_token_retries_transient_failures(monkeypatch):
    """Transient failures (e.g. 400/429 rate limit) are retried, then succeed."""
    from cdsodatacli import fetch_access_token as fat
    from cdsodatacli import retry as _retry

    monkeypatch.setattr(_retry.time, "sleep", lambda s: None)
    attempts = {"n": 0}

    def fake_post(*a, **k):
        attempts["n"] += 1
        if attempts["n"] < 3:
            return _RespError("400 Client Error: Bad Request")
        return _RespOK()

    monkeypatch.setattr("requests.post", fake_post)

    headers = fat._request_access_token("user@ex.com", "pass")
    assert headers["Authorization"].startswith("Bearer TESTTOKEN")
    assert attempts["n"] == 3


def test_request_access_token_raises_after_max_retries(monkeypatch):
    """Persistent failures are surfaced after exhausting retries."""
    from cdsodatacli import fetch_access_token as fat
    from cdsodatacli import retry as _retry

    monkeypatch.setattr(_retry.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        "requests.post", lambda *a, **k: _RespError("400 Client Error: Bad Request")
    )

    with pytest.raises(requests.exceptions.HTTPError):
        fat._request_access_token("user@ex.com", "pass")
