import os
import logging
import random
import glob

import pandas as pd
import threading
import requests
from collections import defaultdict
from cdsodatacli.fetch_access_token import (
    get_bearer_access_token,
    get_valid_access_token,
    get_a_credentials_from_conf_file,
)

S3_SESSIONS_STATUS = {}
_session_s3_lock = threading.Lock()  # protect concurrent access from threads

MAX_SESSION_PER_ACCOUNT = 4  # each account CDSE have maximum 4 active sessions
logger = logging.getLogger(__name__)


def get_list_active_session(conf, login_group=None):
    """
    Method to get the list of active session semaphore files on disk.

    Parameters
    ----------
    conf (dict) configuration dictionary of cdsodatacli package
    login_group (str): e.g. logins or loginsbackfill (for instance, it depends on the localconfig.yml)

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
    Method to get a free (i.e. account for which at least one the 4 active session it not used) CDSE account for downloading.

    Parameters
    ----------
    counts (collections.defaultdict(int)) counter of active session for each CDSE account
    blacklist (list): list of account not usable [default=None]

    Returns
    -------
        candidate (str): email address of the CDSE account free to use
        counts (collections.defaultdict(int)): updated counter of active session for each CDSE account

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


def get_a_free_s3_session(active_s3_sessions_status, conf, account_group, blacklist):
    """
    there are only 2 methods that interact with active_s3_sessions_status memory variable: this one and release_s3_session_after_usage()

    Arguments:
        active_s3_sessions_status (dict):
        conf (dict): cdsodatacli configuration
        account_group (str): name of the group of logins
        blacklist (list):  list of logins temporarily black listed for downloads, can be empty

    Returns:
        active_s3_sessions_status (dict): updated
        candidate_session (int): 0 1 2 3 (see maximum session per account)
        candidate_login (str): CDSE login email
        s3_long_term_credentials (dict): with keys 's3-access-key' and 's3-secret'

    """
    candidate_session = None
    candidate_login = None
    s3_long_term_credentials = {}
    with _session_s3_lock:
        for login in active_s3_sessions_status:
            if login not in blacklist:
                for session_id in active_s3_sessions_status[login]:
                    if (
                        active_s3_sessions_status[login][session_id] is False
                        and candidate_session is None
                    ):
                        candidate_session = session_id
                        candidate_login = login
                        # set this session at active at this point in the code
                        active_s3_sessions_status[login][session_id] = True
                        credentials_from_conf_file = get_a_credentials_from_conf_file(
                            conf=conf, account_group=account_group, login=login
                        )
                        s3_long_term_credentials = {
                            "s3-access-key": credentials_from_conf_file[
                                "s3-access-key"
                            ],
                            "s3-secret": credentials_from_conf_file["s3-secret"],
                        }
                        break
    return (
        active_s3_sessions_status,
        candidate_session,
        candidate_login,
        s3_long_term_credentials,
    )


def release_s3_session_after_usage(active_s3_sessions_status, login, session_id):
    """
    there are only 2 methods that interact with active_s3_sessions_status memory variable: this one and get_a_free_s3_session()

    Arguments:
        active_s3_sessions_status (dict):
        login (str): CDSE account email
        session_id (ind): index 0 1 2 or 3 (see maximum of session per account)

    Returns:
        active_s3_sessions_status (dict): updated

    """
    with _session_s3_lock:
        active_s3_sessions_status[login][session_id] = False
        logger.info("release S3 session %s #%s", login, session_id)

    return active_s3_sessions_status


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
    conf, subset_to_treat, blacklist=None, logins_group="logins"
):
    """

    Parameters
    ----------
    conf (dict) configuration dictionary of cdsodatacli package
    subset_to_treat (pandas.DatFrame)
    blacklist (list): list of account not usable [default=None]
    logins_group (str): name of the group of CDSE accounts to use (can contain multiple accounts, it depends on the localconfig.yml)


    Returns
    -------

    """
    df_products_downloadable = pd.DataFrame()
    all_sessions = []
    all_logins = []
    all_headers = []
    # all_semaphores = []
    all_session_semaphores = []
    usable_accounts = []
    all_safe_basename = []
    bunch_product_downloadable = []
    bunch_urls_to_download = []
    bunch_s3path_to_download = []
    outputfiles_download_coming = []

    lst_sessions_active = get_list_active_session(conf, login_group=logins_group)
    # account_free = None
    account_counter = defaultdict(int)
    for aa in conf[logins_group]:
        if isinstance(aa, str):
            account_tmp = aa
        elif isinstance(aa, dict):
            account_tmp = list(aa)[0]
        else:
            raise ValueError(
                f"Unexpected format for account {aa} in group {logins_group}"
            )
        account_counter[account_tmp] = 0
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
            # lst_usable_tokens = get_list_of_existing_token_semaphore_file(
            #     token_dir=conf["token_directory"], account=account_free
            # )

            access_token, date_generation_access_token = get_valid_access_token(
                login=account_free
            )
            login = account_free
            # lst_usable_tokens = []
            if (
                access_token is None
            ):  # in case no token ready to be used -> create new one
                (
                    access_token,
                    date_generation_access_token,
                    login,
                ) = get_bearer_access_token(
                    conf=conf,
                    specific_account=account_free,
                    account_group=logins_group,
                )

            # else:  # select randomly one token among existing
            #     path_semphore_token = random.choice(lst_usable_tokens)
            #     access_token = open(path_semphore_token).readlines()[0]
            if access_token is not None:
                bunch_product_downloadable.append(safename_product)
                bunch_urls_to_download.append(subset_to_treat["urls"].iloc[ss])
                bunch_s3path_to_download.append(subset_to_treat["S3Path"].iloc[ss])
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
                all_logins.append(login)
                all_headers.append(headers)
                # all_semaphores.append(path_semphore_token)
                all_safe_basename.append(safename_product)
                all_session_semaphores.append(path_semaphore_session)
    df_products_downloadable["session"] = all_sessions
    df_products_downloadable["header"] = all_headers
    # df_products_downloadable["token_semaphore"] = all_semaphores
    df_products_downloadable["login"] = all_logins
    df_products_downloadable["url"] = bunch_urls_to_download
    df_products_downloadable["S3Path"] = bunch_s3path_to_download  # to check S3path
    df_products_downloadable["output_path"] = outputfiles_download_coming
    df_products_downloadable["session_semaphore"] = all_session_semaphores
    df_products_downloadable["safe"] = all_safe_basename
    return df_products_downloadable


def get_sessions_download_available_s3(
    conf,
    active_s3_sessions_status,
    subset_to_treat,
    blacklist,
    logins_group="logins",
):
    """
    This method should return the list of available sessions for a group of CDSE accounts
    contrarily to get_sessions_download_available() it use thread locked in memory variable to list active sessions

    Arguments

        conf (dict) configuration dictionary of cdsodatacli package
        active_s3_sessions_status (dict): login:session_id(int):False->inactive True>-active (set to inactive at begining of a download)
        subset_to_treat (pandas.DatFrame)
        blacklist (list): list of account not usable
        logins_group (str): name of the group of CDSE accounts to use (can contain multiple accounts, it depends on the localconfig.yml)


    Returns


    """
    df_products_ready_for_download = pd.DataFrame()
    all_safe_basename = []
    bunch_urls_to_download = []
    bunch_s3path_to_download = []
    outputfiles_download_coming = []
    all_s3_sessions = []
    all_logins = []
    all_s3_access_keys = []
    all_s3_secrets = []
    for ss in range(len(subset_to_treat)):
        safename_product = subset_to_treat["safe"].iloc[ss]
        # get S3 credentials of a free session

        (
            active_s3_sessions_status,
            candidate_session,
            candidate_login,
            s3_credentials,
        ) = get_a_free_s3_session(
            active_s3_sessions_status,
            conf=conf,
            account_group=logins_group,
            blacklist=blacklist,
        )

        if candidate_session is None:
            logging.debug(
                "no more S3 session available for now in that group of logins."
            )
            break  # no more account free
        else:

            # bunch_product_downloadable.append(safename_product)
            bunch_urls_to_download.append(subset_to_treat["urls"].iloc[ss])
            bunch_s3path_to_download.append(subset_to_treat["S3Path"].iloc[ss])
            all_s3_access_keys.append(s3_credentials["s3-access-key"])
            all_s3_secrets.append(s3_credentials["s3-secret"])
            outputfiles_download_coming.append(subset_to_treat["outputpath"].iloc[ss])
            all_s3_sessions.append(candidate_session)
            all_logins.append(candidate_login)

            all_safe_basename.append(safename_product)

    df_products_ready_for_download["s3_session"] = all_s3_sessions
    # df_products_downloadable["token_semaphore"] = all_semaphores
    df_products_ready_for_download["login"] = all_logins
    df_products_ready_for_download["url"] = bunch_urls_to_download
    df_products_ready_for_download["S3Path"] = (
        bunch_s3path_to_download  # to check S3path
    )
    df_products_ready_for_download["output_path"] = outputfiles_download_coming
    df_products_ready_for_download["safe"] = all_safe_basename
    df_products_ready_for_download["s3_access_key"] = all_s3_access_keys
    df_products_ready_for_download["s3_secret"] = all_s3_secrets

    return df_products_ready_for_download, active_s3_sessions_status
