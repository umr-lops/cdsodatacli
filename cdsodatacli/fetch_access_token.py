import pdb
from cdsodatacli.utils import conf
import subprocess
import logging
import json
import datetime
import os
import glob
import random

MAX_VALIDITY_ACCESS_TOKEN = 600  # sec (defined by CDS API)


def get_bearer_access_token(quiet=True, specific_account=None, account_group="logins"):
    """
    OData access token (validity=600sec)
    specific_account (str) [optional, default=None -> first available account in config file]
    Returns
    -------

    """
    path_semphore_token = None
    url_identity = conf["URL_identity"]
    if specific_account is None:
        all_accounts = list(conf[account_group].keys())
        login = random.choice(all_accounts)
        passwd = conf[account_group][all_accounts[0]]
    else:
        login = specific_account
        logging.debug("conf[account_group] %s", type(conf[account_group]))
        passwd = conf[account_group][specific_account]
    if quiet:
        prefix = "curl -s "
    else:
        prefix = "curl "
    option_insecure = ' --insecure' # added because workers have deprecated SSL certificates
    cmd = (
        prefix
        + " --location --request POST "
        + url_identity
        + " --header 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'grant_type=password' --data-urlencode 'username=%s' --data-urlencode 'password=%s' --data-urlencode 'client_id=cdse-public' %s"
        % (login, passwd,option_insecure)
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
        path_semphore_token = write_token_semphore_file(
            login=login,
            date_generation_access_token=date_generation_access_token,
            token_dir=conf["token_directory"],
            access_token=token,
        )
    return token, date_generation_access_token, login, path_semphore_token


def write_token_semphore_file(
    login, date_generation_access_token, token_dir, access_token
):
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
        "CDSE_access_token_%s_%s.txt"
        % (login, date_generation_access_token.strftime("%Y%m%dt%H%M%S")),
    )
    fid = open(path_semphore_token, "w")
    fid.write(access_token)
    fid.close()
    return path_semphore_token


def get_list_of_exising_token(token_dir, account=None):
    """

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
            os.path.basename(ll).split("_")[4].replace(".txt", ""), "%Y%m%dt%H%M%S"
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
        % (login, date_generation_access_token.strftime("%Y%m%dt%H%M%S")),
    )
    exists = os.path.exists(path_token)
    if (
        exists
        and (datetime.datetime.today() - date_generation_access_token).total_seconds()
        >= MAX_VALIDITY_ACCESS_TOKEN
    ):
        os.remove(path_token)
        logging.debug("token semaphore file removed")
