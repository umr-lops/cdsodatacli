import pdb
from cdsodatacli.utils import conf
import subprocess
import logging
import json
import datetime
import os
import glob
import random

MAX_SESSION_PER_ACCOUNT = 4


def get_bearer_access_token(quiet=True, specific_account=None):
    """
    OData access token (validity=600sec)
    specific_account (str) [optional, default=None -> first available account in config file]
    Returns
    -------

    """
    url_identity = conf["URL_identity"]
    if specific_account is None:
        all_accounts = conf["logins"].keys()
        login = all_accounts[0]
        passwd = conf["logins"][all_accounts[0]]
    else:
        login = specific_account
        logging.debug('conf["logins"] %s', type(conf["logins"]))
        passwd = conf["logins"][specific_account]
    if quiet:
        prefix = "curl -s "
    else:
        prefix = "curl "
    cmd = (
        prefix
        + " --location --request POST "
        + url_identity
        + " --header 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'grant_type=password' --data-urlencode 'username=%s' --data-urlencode 'password=%s'  --data-urlencode 'client_id=cdse-public'"
        % (login, passwd)
    )

    logging.debug("cmd: %s", cmd)
    date_generation_access_token = datetime.datetime.today()
    answer_identity = subprocess.check_output(cmd, shell=True)
    logging.debug("answer_identity: %s", answer_identity)
    toto = answer_identity.decode("utf8").replace("'", '"')
    data = json.loads(toto)
    if "access_token" not in data:
        raise Exception('you probably have a bad account (%s) in your config file: %s',login,data)
    return data["access_token"], date_generation_access_token


def write_token_semphore_file(safename, login, date_generation_access_token, token_dir):
    """

    Parameters
    ----------
    safename (str):
    login (str) :email address of CDSE account
    date_generation_access_token (datetime.datetime)
    token_dir (str)

    Returns
    -------

    """
    path_semphore_token = os.path.join(
        token_dir,
        "CDSE_access_token_%s_%s_%s.txt"
        % (login, date_generation_access_token.strftime("%Y%m%dt%H%M%S"), safename),
    )
    fid = open(path_semphore_token, "w")
    fid.close()
    return path_semphore_token


def get_list_of_exising_token(token_dir):
    """
    we suppose ech account CDSE have maximum 4 tokens

    Returns
    -------
        lst_token (list)
    """
    lst_token = glob.glob(os.path.join(token_dir, "CDSE_access_token_*.txt"))
    logging.debug("Number of token found: %s", len(lst_token))
    # nb_account = len(conf["logins"])
    return lst_token


def get_a_free_account(counts):
    """

    Parameters
    ----------
    counts (collections.defaultdict(int)) counter of active session for each CDSE account
    Returns
    -------

    """
    candidate = None
    all_free_accounts = []
    for acc in counts:
        if counts[acc] < MAX_SESSION_PER_ACCOUNT:
            all_free_accounts.append(acc)
        else:
            logging.debug("account: %s is full", acc)
    logging.debug("counts after attribution %s", counts)
    logging.debug("all_free_accounts %s", all_free_accounts)
    if len(all_free_accounts) > 0:
        candidate = random.choice(all_free_accounts)
        counts[candidate] += 1
    else:
        logging.debug("there is no free CDSE account to use in your localconfig.yml")
    return candidate, counts


def remove_semaphore_token_file(
    token_dir, safename, login, date_generation_access_token
):
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
        "CDSE_access_token_%s_%s_%s.txt"
        % (login, date_generation_access_token.strftime("%Y%m%dt%H%M%S"), safename),
    )
    os.remove(path_token)
    logging.debug("token semaphore file removed")


