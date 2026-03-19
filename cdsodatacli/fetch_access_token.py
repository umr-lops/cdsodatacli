import logging
import datetime
import threading
import random
import urllib3
import requests

MAX_VALIDITY_ACCESS_TOKEN = 600  # sec (defined by CDS API)
DATE_FORMAT_YMDTHMS = "%Y%m%dt%H%M%S"
ACTIVE_ACCESS_TOKEN = (
    {}
)  # login -> list of {'access-token': str, 'access-token-creation-date': datetime}
_token_cache_lock = threading.Lock()  # protect concurrent access from threads


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
        login = random.choice(list(conf[account_group].keys()))
    else:
        login = specific_account
    if specific_psswd is None:
        passwd = conf[account_group][login]
    else:
        passwd = specific_psswd

    # check if a valid token already exists in cache before hitting the server
    with _token_cache_lock:
        if login in ACTIVE_ACCESS_TOKEN:
            for entry in ACTIVE_ACCESS_TOKEN[login]:
                age = (
                    datetime.datetime.today() - entry["access-token-creation-date"]
                ).total_seconds()
                if age < MAX_VALIDITY_ACCESS_TOKEN - 30:  # 30s safety margin
                    logging.debug("reusing cached token for %s (age=%ds)", login, age)
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
    logging.debug("new token obtained for %s", login)

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

        # clean expired tokens for all logins
        for logintest in list(ACTIVE_ACCESS_TOKEN.keys()):
            valid_tokens = []
            for entry in ACTIVE_ACCESS_TOKEN[logintest]:
                age = (
                    datetime.datetime.today() - entry["access-token-creation-date"]
                ).total_seconds()
                if age > MAX_VALIDITY_ACCESS_TOKEN:
                    logging.debug(
                        "removing expired token for %s (age=%ds)", logintest, age
                    )
                    # expired -> do NOT keep it
                    ACTIVE_ACCESS_TOKEN[logintest].remove(entry)
                else:
                    valid_tokens.append(entry)  # still valid -> keep it
            ACTIVE_ACCESS_TOKEN[logintest] = valid_tokens
            if not ACTIVE_ACCESS_TOKEN[logintest]:
                del ACTIVE_ACCESS_TOKEN[
                    logintest
                ]  # remove login key if no valid tokens left

    return token, date_generation, login


def get_valid_access_token(login):
    """
    Get a valid access token for a specific login from the in-memory cache.

    Parameters
    ----------
    login (str): CDSE account email address

    Returns
    -------
        token (str): valid bearer access token
        date_generation (datetime.datetime): token creation time
        or (None, None) if no valid token found for this login
    """
    with _token_cache_lock:
        if login not in ACTIVE_ACCESS_TOKEN:
            logging.debug("no token found in cache for %s", login)
            return None, None
        for entry in ACTIVE_ACCESS_TOKEN[login]:
            age = (
                datetime.datetime.today() - entry["access-token-creation-date"]
            ).total_seconds()
            if (
                age < MAX_VALIDITY_ACCESS_TOKEN - 30
            ):  # 30s safety margin, same as in get_bearer_access_token
                logging.debug("valid token found in cache for %s (age=%ds)", login, age)
                return entry["access-token"], entry["access-token-creation-date"]
        logging.debug("no valid token found in cache for %s (all expired)", login)
        return None, None


# def write_token_semaphore_file(
#     login, date_generation_access_token, token_dir, access_token
# ):
#     """
#     When a we get an access token for a given CDSE account, we can use it for 600 seconds
#     then we need to store it on disk with the date of creation of the token

#     Parameters
#     ----------
#     safename (str):
#     login (str) :email address of CDSE account
#     date_generation_access_token (datetime.datetime)
#     token_dir (str)

#     Returns
#     -------

#     """
#     path_acces_token_file = os.path.join(
#         token_dir,
#         "CDSE_access_token_%s_%s.txt"
#         % (login, date_generation_access_token.strftime(DATE_FORMAT_YMDTHMS)),
#     )
#     fid = open(path_acces_token_file, "w")
#     fid.write(access_token)
#     fid.close()
#     return path_acces_token_file


# def get_list_of_existing_token_semaphore_file(token_dir, account=None):
#     """
#     a bearer access token can be re-used (no need to have one token per download)
#     present method lists all the access token that can be used
#       for a specific account or in general

#     Parameters

#         account (str): optional

#     Returns
#     -------
#         lst_token (list)
#     """
#     if account is not None:
#         lst_token0 = glob.glob(
#             os.path.join(token_dir, "CDSE_access_token_%s_*.txt" % account)
#         )
#     else:
#         lst_token0 = glob.glob(os.path.join(token_dir, "CDSE_access_token_*.txt"))

#     lst_token = []
#     for ll in lst_token0:
#         date_generation_access_token = datetime.datetime.strptime(
#             os.path.basename(ll).split("_")[4].replace(".txt", ""), DATE_FORMAT_YMDTHMS
#         )
#         if (
#             datetime.datetime.today() - date_generation_access_token
#         ).total_seconds() < MAX_VALIDITY_ACCESS_TOKEN:
#             lst_token.append(ll)
#     logging.debug("Number of token found: %s", len(lst_token))
#     return lst_token


# def remove_semaphore_token_file(token_dir, login, date_generation_access_token):
#     """
#     this function is supposed to be used when a download is finished ( could be long time after the validity expired)

#     token_dir (str):
#     safename (str): basename of the product
#     login (str): CDSE email account
#     date_generation_access_token (datetime.datetime)

#     Returns
#     -------

#     """
#     path_token = os.path.join(
#         token_dir,
#         "CDSE_access_token_%s_%s.txt"
#         % (login, date_generation_access_token.strftime(DATE_FORMAT_YMDTHMS)),
#     )
#     exists = os.path.exists(path_token)
#     if (
#         exists
#         and (datetime.datetime.today() - date_generation_access_token).total_seconds()
#         >= MAX_VALIDITY_ACCESS_TOKEN
#     ):
#         os.remove(path_token)
#         logging.debug("token semaphore file removed")


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
    logging.debug(f"Obtained ACCESS_TOKEN for {email}")
    # check that token was obtained
    if not token:
        raise ValueError("No access token found in the response.")
    else:
        if len(token) > 20:
            logging.debug("Token: %s...%s", token[:10], token[-10:])
        else:
            logging.debug("Token: %s", token)
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}
