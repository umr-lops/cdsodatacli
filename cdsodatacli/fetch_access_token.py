import subprocess
import logging
import json
import datetime
import os
import glob
import random
import urllib3
import requests

MAX_VALIDITY_ACCESS_TOKEN = 600  # sec (defined by CDS API)
DATE_FORMAT_YMDTHMS = "%Y%m%dt%H%M%S"

def get_bearer_access_token(
    conf,
    quiet=True,
    specific_account=None,
    passwd=None,
    account_group="logins",
):
    """
    OData access token (validity=600sec)

    Parameters
    ----------
    conf (dict): conf CDSE coming from yaml file (defines the account and groups of account)
    quiet (bool): True -> curl in silent mode
    specific_account (str) [optional, default=None -> first available account in config file]
    passwd (str): [optional, default is to search in config files]
    account_group (str): name of the group of accounts in the config file [default='logins']

    Returns
    -------
        token (str): access token
        date_generation_access_token (datetime.datetime): date of generation of the token
        login (str): CDSE account actually used (or the one defined as input or randomly chosen in a group of accounts)
        path_access_token_file (str): path of the access token semaphore file created to store the token

    """
    path_access_token_file = None
    url_identity = conf["URL_identity"]
    if specific_account is None:
        all_accounts = list(conf[account_group].keys())
        login = random.choice(all_accounts)
        if passwd is None:
            passwd = conf[account_group][all_accounts[0]]
    else:
        login = specific_account
        if passwd is None:
            logging.debug("conf[account_group] %s", type(conf[account_group]))
            passwd = conf[account_group][specific_account]
    if quiet:
        prefix = "curl -s "
    else:
        prefix = "curl "
    option_insecure = (
        " --insecure"  # added because workers have deprecated SSL certificates
    )
    cmd = (
        prefix
        + " --location --request POST "
        + url_identity
        + " --header 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'grant_type=password' --data-urlencode 'username=%s' --data-urlencode 'password=%s' --data-urlencode 'client_id=cdse-public' %s"
        % (login, passwd, option_insecure)
    )

    logging.debug("cmd: %s", cmd)
    date_generation_access_token = datetime.datetime.today()
    answer_identity = subprocess.check_output(cmd, shell=True)
    logging.debug("answer_identity: %s", answer_identity)
    toto = answer_identity.decode("utf8").replace("'", '"')
    data = json.loads(toto)
    if "access_token" not in data:
        # raise Exception('you probably have a bad account (%s) in your config file: %s',login,data)
        logging.info(
            "you probably have a bad account (%s) in your config file: %s", login, data
        )
        token = None
    else:
        token = data["access_token"]
        path_access_token_file = write_token_semaphore_file(
            login=login,
            date_generation_access_token=date_generation_access_token,
            token_dir=conf["token_directory"],
            access_token=token,
        )
    return token, date_generation_access_token, login, path_access_token_file


def write_token_semaphore_file(
    login, date_generation_access_token, token_dir, access_token
):
    """
    When a we get an access token for a given CDSE account, we can use it for 600 seconds
    then we need to store it on disk with the date of creation of the token

    Parameters
    ----------
    safename (str):
    login (str) :email address of CDSE account
    date_generation_access_token (datetime.datetime)
    token_dir (str)

    Returns
    -------

    """
    path_acces_token_file = os.path.join(
        token_dir,
        "CDSE_access_token_%s_%s.txt"
        % (login, date_generation_access_token.strftime(DATE_FORMAT_YMDTHMS)),
    )
    fid = open(path_acces_token_file, "w")
    fid.write(access_token)
    fid.close()
    return path_acces_token_file


def get_list_of_existing_token_semaphore_file(token_dir, account=None):
    """
    a bearer access token can be re-used (no need to have one token per download)
    present method lists all the access token that can be used
      for a specific account or in general

    Parameters

        account (str): optional

    Returns
    -------
        lst_token (list)
    """
    if account is not None:
        lst_token0 = glob.glob(
            os.path.join(token_dir, "CDSE_access_token_%s_*.txt" % account)
        )
    else:
        lst_token0 = glob.glob(os.path.join(token_dir, "CDSE_access_token_*.txt"))

    lst_token = []
    for ll in lst_token0:
        date_generation_access_token = datetime.datetime.strptime(
            os.path.basename(ll).split("_")[4].replace(".txt", ""), DATE_FORMAT_YMDTHMS
        )
        if (
            datetime.datetime.today() - date_generation_access_token
        ).total_seconds() < MAX_VALIDITY_ACCESS_TOKEN:
            lst_token.append(ll)
    logging.debug("Number of token found: %s", len(lst_token))
    return lst_token


def remove_semaphore_token_file(token_dir, login, date_generation_access_token):
    """
    this function is supposed to be used when a download is finished ( could be long time after the validity expired)

    token_dir (str):
    safename (str): basename of the product
    login (str): CDSE email account
    date_generation_access_token (datetime.datetime)

    Returns
    -------

    """
    path_token = os.path.join(
        token_dir,
        "CDSE_access_token_%s_%s.txt"
        % (login, date_generation_access_token.strftime(DATE_FORMAT_YMDTHMS)),
    )
    exists = os.path.exists(path_token)
    if (
        exists
        and (datetime.datetime.today() - date_generation_access_token).total_seconds()
        >= MAX_VALIDITY_ACCESS_TOKEN
    ):
        os.remove(path_token)
        logging.debug("token semaphore file removed")


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
