import os
import logging
import pdb
import random
import glob

import pandas as pd
import requests
from collections import defaultdict
from cdsodatacli.utils import conf
from cdsodatacli.fetch_access_token import (
    get_list_of_exising_token,
    get_bearer_access_token,
)

MAX_SESSION_PER_ACCOUNT = 4  # each account CDSE have maximum 4 active sessions


def get_list_active_session(login_group=None):
    """

    Returns
    -------
        consolidated_active_session_semaphore (list)
    """
    lst_sessions = glob.glob(
        os.path.join(conf["active_session_directory"], "CDSE_active_session_*.txt")
    )

    if login_group is not None:
        consolidated_active_session_semaphore = []
        for token_sess in lst_sessions:
            acc_found = os.path.basename(token_sess).split("_")[3]
            if acc_found in conf[login_group]:
                consolidated_active_session_semaphore.append(token_sess)
    else:
        consolidated_active_session_semaphore = lst_sessions
    logging.debug("Number of active sessions found: %s", len(lst_sessions))
    return consolidated_active_session_semaphore


def get_a_free_account(counts, blacklist=None):
    """

    Parameters
    ----------
    counts (collections.defaultdict(int)) counter of active session for each CDSE account
    blacklist (list): list of account not usable [default=None]

    Returns
    -------

    """
    candidate = None
    if blacklist is None:
        blacklist = []
    all_free_accounts = []
    for acc in counts:
        if counts[acc] < MAX_SESSION_PER_ACCOUNT and acc not in blacklist:
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


def write_active_session_semphore_file(safename, login, session_dir):
    """

    Parameters
    ----------
    safename (str):
    login (str) :email address of CDSE account
    session_dir (str)

    Returns
    -------

    """
    path_semphore_session = os.path.join(
        session_dir,
        "CDSE_active_session_%s_%s.txt" % (login, safename),
    )
    fid = open(path_semphore_session, "w")
    fid.close()
    return path_semphore_session


def remove_semaphore_session_file(session_dir, safename=None, login=None):
    """
    this function is supposed to be used when a download is finished

    session_dir (str):
    safename (str): basename of the product
    login (str): CDSE email account

    Returns
    -------

    """
    if safename is None:
        safename_str = "*"
    else:
        safename_str = safename
    if login is None:
        login_str = "*"
    else:
        login_str = login

    path_semphore_session = os.path.join(
        session_dir,
        "CDSE_active_session_%s_%s.txt" % (login_str, safename_str),
    )
    lst = glob.glob(path_semphore_session)
    for llu in lst:
        os.remove(llu)
    logging.debug("session semaphore file removed")


def get_sessions_download_available(
    subset_to_treat, hideProgressBar=True, blacklist=None, logins_group="logins"
):
    """

    Parameters
    ----------
    subset_to_treat (pandas.DatFrame)
    hideProgressBar (bool)
    blacklist (list): list of account not usable [default=None]
    logins_group (str): logins or loginsbackfill (for instance, it depends on the localconfig.yml)
    Returns
    -------

    """
    df_products_downloadable = pd.DataFrame()
    all_sessions = []
    all_headers = []
    all_semaphores = []
    all_session_semaphores = []
    usable_accounts = []
    all_safe_basename = []
    bunch_product_downloadable = []
    bunch_urls_to_download = []
    outputfiles_download_coming = []

    lst_sessions_active = get_list_active_session(login_group=logins_group)
    # account_free = None
    account_counter = defaultdict(int)
    for aa in conf[logins_group]:
        account_counter[aa] = 0
    logging.debug("(re)init the counts for accounts.")
    for toto in lst_sessions_active:
        account = os.path.basename(toto).split("_")[3]
        account_counter[account] += 1
    logging.debug("counts after tokens browsing %s", account_counter)
    for ss in range(len(subset_to_treat)):
        safename_product = subset_to_treat["safe"].iloc[ss]

        account_free, account_counter = get_a_free_account(
            counts=account_counter, blacklist=blacklist
        )
        if account_free is None:
            logging.debug("no more account available for now.")
            break  # no more account free
        else:
            lst_usable_tokens = get_list_of_exising_token(
                token_dir=conf["token_directory"], account=account_free
            )
            if (
                lst_usable_tokens == []
            ):  # in case no token ready to be used -> create new one
                (
                    access_token,
                    date_generation_access_token,
                    login,
                    path_semphore_token,
                ) = get_bearer_access_token(
                    quiet=hideProgressBar,
                    specific_account=account_free,
                    account_group=logins_group,
                )
            else:  # select randomly one token among existing
                path_semphore_token = random.choice(lst_usable_tokens)
                access_token = open(path_semphore_token).readlines()[0]
            if access_token is not None:
                bunch_product_downloadable.append(safename_product)
                bunch_urls_to_download.append(subset_to_treat["urls"].iloc[ss])
                outputfiles_download_coming.append(
                    subset_to_treat["outputpath"].iloc[ss]
                )
                usable_accounts.append(account_free)
                path_semaphore_session = write_active_session_semphore_file(
                    safename_product,
                    login=account_free,
                    session_dir=conf["active_session_directory"],
                )
                headers = {"Authorization": "Bearer %s" % access_token}
                logging.debug("headers: %s", headers)
                session = requests.Session()
                session.headers.update(headers)
                all_sessions.append(session)
                all_headers.append(headers)
                all_semaphores.append(path_semphore_token)
                all_safe_basename.append(safename_product)
                all_session_semaphores.append(path_semaphore_session)
    df_products_downloadable["session"] = all_sessions
    df_products_downloadable["header"] = all_headers
    df_products_downloadable["token_semaphore"] = all_semaphores
    df_products_downloadable["url"] = bunch_urls_to_download
    df_products_downloadable["output_path"] = outputfiles_download_coming
    df_products_downloadable["session_semaphore"] = all_session_semaphores
    df_products_downloadable["safe"] = all_safe_basename
    return df_products_downloadable
