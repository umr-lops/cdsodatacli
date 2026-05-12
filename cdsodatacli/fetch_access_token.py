import logging
import datetime
import threading
import random
import urllib3
import requests

MAX_VALIDITY_ACCESS_TOKEN = 600  # sec (defined by CDS API)
ACTIVE_ACCESS_TOKEN = (
    {}
)  # login -> list of {'access-token': str, 'access-token-creation-date': datetime}
_token_cache_lock = threading.Lock()  # protect concurrent access from threads

logger = logging.getLogger(__name__)


def get_a_login_from_conf_file(conf, account_group):
    if isinstance(conf[account_group], list):
        login = random.choice(
            [list(d.keys())[0] for d in conf[account_group] if isinstance(d, dict)]
        )
    elif isinstance(conf[account_group], dict):

        login = random.choice(list(conf[account_group].keys()))
    else:
        raise ValueError(
            f"Unexpected format for account group {account_group} in config file"
        )
    return login


def get_a_credentials_from_conf_file(conf, account_group, login):
    """
    Arguments:
        conf (dict):
        account_group (str):
        type_passwd (str): 's3' or 'cdse-psswd'

    Returns:
        credentials (dict): with keys 's3-access-key','cdse-psswd', 's3-secret'
    """
    if isinstance(conf[account_group], list):
        idx = next(i for i, d in enumerate(conf[account_group]) if login in d)
        credentials = conf[account_group][idx][login]  # ['cdse-psswd']
    elif isinstance(conf[account_group], dict):
        credentials = conf[account_group][login]  # ['cdse-psswd']
    else:
        raise ValueError(
            f"Unexpected format for account group {account_group} in config file"
        )
    return credentials


def get_bearer_access_token(
    conf, specific_account=None, specific_psswd=None, account_group="logins"
):
    """
    Get a CDSE bearer access token for a given account.
    Uses an in-memory cache to avoid hitting the CDSE identity server on every call.
    Expired tokens are cleaned up automatically.

    Parameters
    ----------
    conf (dict): cdsodatacli configuration
    specific_account (str): optional, if None a random account from the group is chosen
    specific_psswd (str): optional, if None if password is found from config file
    account_group (str): key in conf for the account group [default='logins']

    Returns
    -------
        token (str): bearer access token
        date_generation (datetime.datetime): token creation time
        login (str): account used
    """
    if specific_account is None:
        get_a_login_from_conf_file(conf=conf, account_group=account_group)
    else:
        login = specific_account

    if specific_psswd is None:
        credentials = get_a_credentials_from_conf_file(
            conf=conf, account_group=account_group, login=login
        )
        passwd = credentials["cdse-psswd"]
    else:
        passwd = specific_psswd
    logger.debug(
        f"Requesting access token for account {login} from group {account_group}"
    )
    logger.debug(f"Password for {login} is {'*' * len(passwd) if passwd else '(none)'}")
    # check if a valid token already exists in cache before hitting the server
    with _token_cache_lock:
        if login in ACTIVE_ACCESS_TOKEN:
            for entry in ACTIVE_ACCESS_TOKEN[login]:
                age = (
                    datetime.datetime.today() - entry["access-token-creation-date"]
                ).total_seconds()
                if age < MAX_VALIDITY_ACCESS_TOKEN - 30:  # 30s safety margin
                    logger.debug("reusing cached token for %s (age=%ds)", login, age)
                    return (
                        entry["access-token"],
                        entry["access-token-creation-date"],
                        login,
                    )

    # no valid cached token found — fetch a new one from CDSE identity server
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.post(
        conf["URL_identity"],
        data={
            "client_id": "cdse-public",
            "username": login,
            "password": passwd,
            "grant_type": "password",
        },
        verify=False,
        timeout=10,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise ValueError("No access token for account %s" % login)
    date_generation = datetime.datetime.today()
    logger.debug("new token obtained for %s", login)

    with _token_cache_lock:
        # store new token
        if login not in ACTIVE_ACCESS_TOKEN:
            ACTIVE_ACCESS_TOKEN[login] = []
        ACTIVE_ACCESS_TOKEN[login].append(
            {
                "access-token": token,
                "access-token-creation-date": date_generation,
            }
        )

        # clean expired tokens for all logins — no nested lock needed
        for logintest in list(ACTIVE_ACCESS_TOKEN.keys()):
            cutoff = datetime.datetime.now() - datetime.timedelta(
                seconds=MAX_VALIDITY_ACCESS_TOKEN
            )
            ACTIVE_ACCESS_TOKEN[logintest] = [
                entry
                for entry in ACTIVE_ACCESS_TOKEN[logintest]
                if entry["access-token-creation-date"] > cutoff
            ]
            if not ACTIVE_ACCESS_TOKEN[logintest]:
                logger.debug("clean active access token for: %s", logintest)
                del ACTIVE_ACCESS_TOKEN[logintest]

    return token, date_generation, login


def get_access_token(email, password):
    """Helper to retrieve OIDC token.
    one can generate as many access token as wanted per CDSE account
    """
    auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    auth_data = {
        "client_id": "cdse-public",
        "username": email,
        "password": password,
        "grant_type": "password",
    }

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    # response = requests.post(auth_url, data=auth_data, verify=False)
    response = requests.post(auth_url, data=auth_data, verify=False, timeout=10)
    response.raise_for_status()
    token = response.json().get("access_token")
    logger.debug(f"Obtained ACCESS_TOKEN for {email}")
    # check that token was obtained
    if not token:
        raise ValueError("No access token found in the response.")
    else:
        if len(token) > 20:
            logger.debug("Token: %s...%s", token[:10], token[-10:])
        else:
            logger.debug("Token: %s", token)
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}
