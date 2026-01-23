from cdsodatacli.fetch_access_token import get_access_token
from cdsodatacli.utils import get_conf
from dotenv import load_dotenv
import os
import pytest
import requests

load_dotenv()
conf = get_conf()


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
