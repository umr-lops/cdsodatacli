import logging
import pandas as pd
import threading

from cdsodatacli.fetch_access_token import (
    get_a_credentials_from_conf_file,
)

_session_s3_lock = threading.Lock()  # protect concurrent access from threads

MAX_SESSION_PER_ACCOUNT = 4  # each account CDSE have maximum 4 active sessions
logger = logging.getLogger(__name__)


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
        logger.debug("release S3 session %s #%s", login, session_id)

    return active_s3_sessions_status


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

    Arguments:
        conf (dict) configuration dictionary of cdsodatacli package
        active_s3_sessions_status (dict): login:session_id(int):False->inactive True>-active (set to inactive at begining of a download)
        subset_to_treat (pandas.DatFrame)
        blacklist (list): list of account not usable
        logins_group (str): name of the group of CDSE accounts to use (can contain multiple accounts, it depends on the localconfig.yml)


    Returns:
        df_products_ready_for_download (pandas.DataFrame): with columns 's3_session', 'login', 'S3Path', 'output_path', 'safe', 's3_access_key', 's3_secret'
        active_s3_sessions_status (dict): updated with the sessions that are now set to active

    """
    df_products_ready_for_download = pd.DataFrame()
    all_safe_basename = []
    # bunch_urls_to_download = []
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
            # bunch_urls_to_download.append(subset_to_treat["urls"].iloc[ss])
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
    # df_products_ready_for_download["url"] = bunch_urls_to_download
    df_products_ready_for_download["S3Path"] = (
        bunch_s3path_to_download  # to check S3path
    )
    df_products_ready_for_download["output_path"] = outputfiles_download_coming
    df_products_ready_for_download["safe"] = all_safe_basename
    df_products_ready_for_download["s3_access_key"] = all_s3_access_keys
    df_products_ready_for_download["s3_secret"] = all_s3_secrets

    return df_products_ready_for_download, active_s3_sessions_status
