from cdsodatacli.fetch_access_token import get_access_token
from cdsodatacli.utils import get_conf
from dotenv import load_dotenv
import os

load_dotenv()
conf = get_conf()


def test_get_access_token_using_urllib3_and_requests():
    login_cdse = os.getenv("DEFAULT_LOGIN_CDSE", None)
    passwd = os.getenv("DEFAULT_PASSWD_CDSE", None)
    # for local test -> use the localconfig.yml/config.yml files
    if login_cdse is None or passwd is None:
        print("using cdsodatacli localconfig.yml/config.yml for login")
        default_login = conf.get("default_login", {})
        login_cdse, passwd = list(default_login.items())[0]
    headers = get_access_token(email=login_cdse, password=passwd)
    assert "Authorization" in headers
    assert headers["Authorization"].startswith("Bearer ")
    assert "Accept" in headers
    assert headers["Accept"] == "application/json"
