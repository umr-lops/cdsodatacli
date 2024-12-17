import pdb
import subprocess

import requests
import logging
from tqdm import tqdm
import datetime
import time
import os
import shutil
import random
import pandas as pd
import geopandas as gpd
from requests.exceptions import ChunkedEncodingError
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import traceback
from cdsodatacli.fetch_access_token import (
    get_bearer_access_token,
    write_token_semphore_file,
    remove_semaphore_token_file,
    MAX_VALIDITY_ACCESS_TOKEN,
    get_list_of_exising_token,
)
from cdsodatacli.session import (
    remove_semaphore_session_file,
    get_sessions_download_available,
    MAX_SESSION_PER_ACCOUNT,
)
from cdsodatacli.query import fetch_data
from cdsodatacli.utils import conf, test_safe_archive, test_safe_spool, test_safe_in_outputdir
from cdsodatacli.product_parser import ExplodeSAFE
from collections import defaultdict

# chunksize = 4096
chunksize = 8192  # like in the CDSE example

# def CDS_Odata_download_one_product(session, headers, url, output_filepath):
#     """
#
#     Parameters
#     ----------
#     session (request Obj)
#     headers (dict)
#     url (str)
#     output_filepath (str) full path where to store fetch file
#
#     Returns
#     -------
#         speed (float): download speed in Mo/second
#
#     """
#     t0 = time.time()
#     with open(output_filepath, "wb") as f:
#         logging.info("Downloading %s" % output_filepath)
#         response = session.get(url, headers=headers, stream=True)
#         total_length = int(int(response.headers.get("content-length")) / 1000 / 1000)
#         logging.debug("total_length : %s Mo", total_length)
#         if total_length is None:  # no content length header
#             f.write(response.content)
#         else:
#             dl = 0
#             with tqdm(
#                 total=total_length, disable=bool(os.environ.get("DISABLE_TQDM", False))
#             ) as progress_bar:
#                 for data in tqdm(
#                     response.iter_content(chunk_size=chunksize),
#                     disable=bool(os.environ.get("DISABLE_TQDM", False)),
#                 ):
#                     dl += len(data)
#                     f.write(data)
#                     progress_bar.update(chunksize / 1000.0 / 1000.0)  # update progress
#     elapsed_time = time.time() - t0
#     logging.info("time to download this product: %1.1f sec", elapsed_time)
#     speed = total_length / elapsed_time
#     logging.info("average download speed: %1.1fMo/sec", speed)
#     return speed


def CDS_Odata_download_one_product_v2(
    session, headers, url, output_filepath, semaphore_token_file
):
    """
     v2 is without tqdm
    Parameters
    ----------
    session (request Obj)
    headers (dict)
    url (str)
    output_filepath (str): full path where to store fetch file
    semaphore_token_file (str): full path of the file storing an active access token

    Returns
    -------
        speed (float): download speed in Mo/second

    """
    speed = np.nan
    status_meaning = "unknown_code"
    t0 = time.time()
    # output_filepath_tmp = (
    #     output_filepath.replace(conf["spool"], conf["pre_spool"]) + ".tmp"
    # )
    output_filepath_tmp = os.path.join(
        conf["pre_spool"], os.path.basename(output_filepath) + ".tmp"
    )
    safename_base = os.path.basename(output_filepath).replace(".zip", "")
    with open(output_filepath_tmp, "wb") as f:
        logging.debug("Downloading %s" % output_filepath)
        response = session.get(url, headers=headers, stream=True)
        status = response.status_code
        status_meaning = response.reason
        # Check for 'Transfer-Encoding: chunked'
        if (
            "Transfer-Encoding" in response.headers
            and response.headers["Transfer-Encoding"] == "chunked"
        ):
            logging.warning(
                "Server is using 'Transfer-Encoding: chunked'. Content length may not be accurate."
            )
        if response.ok:
            total_length = int(
                int(response.headers.get("content-length")) / 1000 / 1000
            )
            logging.debug("total_length : %s Mo", total_length)
            try:
                for chunk in response.iter_content(chunk_size=chunksize):
                    if chunk:
                        f.write(chunk)
            except ChunkedEncodingError as e:
                status = -1
                status_meaning = "ChunkedEncodingError"
    if (not response.ok or status == -1) and os.path.exists(output_filepath_tmp):
        logging.debug("remove empty file %s", output_filepath_tmp)
        os.remove(output_filepath_tmp)
    elapsed_time = time.time() - t0
    if status == 200:  # means OK download
        speed = total_length / elapsed_time
        # shutil.move(output_filepath_tmp, output_filepath)
        status = subprocess.check_output('mv '+output_filepath_tmp+' '+output_filepath,shell=True)
        logging.debug('move status: %s',status)
        os.chmod(output_filepath, mode=0o0775)
    logging.debug("time to download this product: %1.1f sec", elapsed_time)
    logging.debug("average download speed: %1.1fMo/sec", speed)
    return speed, status_meaning, safename_base, semaphore_token_file


def filter_product_already_present(cpt, df, outputdir, force_download=False):
    """

    Parameters
    ----------
    cpt (collections.defaultdict(int))
    df (pd.DataFrame)
    outputdir (str)
    force_download (bool)

    Returns
    -------

    """
    all_output_filepath = []
    all_urls_to_download = []
    index_to_download = []
    for ii, safename_product in enumerate(df["safe"]):
        to_download = False
        if force_download:
            to_download = True
        if test_safe_archive(safename=safename_product):
            cpt["archived_product"] += 1
        elif test_safe_spool(safename=safename_product):
            cpt["in_spool_product"] += 1
        elif test_safe_in_outputdir(outputdir=outputdir,safename=safename_product):
            cpt["in_outdir_product"] += 1
        else:
            to_download = True
            cpt["product_absent_from_local_disks"] += 1
        if to_download:
            index_to_download.append(ii)
            id_product = df["id"].iloc[ii]
            url_product = conf["URL_download"] % id_product

            logging.debug("url_product : %s", url_product)
            logging.debug(
                "id_product : %s safename_product : %s",
                id_product,
                safename_product,
            )

            output_filepath = os.path.join(outputdir, safename_product + ".zip")
            all_output_filepath.append(output_filepath)
            all_urls_to_download.append(url_product)
    df_todownload = df.iloc[index_to_download]
    df_todownload["urls"] = all_urls_to_download
    df_todownload["outputpath"] = all_output_filepath
    return df_todownload, cpt


def download_list_product_multithread_v2(
    list_id,
    list_safename,
    outputdir,
    hideProgressBar=False,
    account_group="logins",
    check_on_disk=True,
):
    """
    v2 is handling multi account round-robin and token semaphore files
    Parameters
    ----------
    list_id (list)
    list_safename (list)
    outputdir (str)
    hideProgressBar (bool): True -> no tqdm progress bar in stdout
    account_group (str)
    check_on_disk (bool): True -> if the product is in the spool dir or in archive dir the download is skipped

    Returns
    -------
        df2 (pd.DataFrame):
    """
    assert len(list_id) == len(list_safename)
    logging.info("check_on_disk : %s", check_on_disk)
    cpt = defaultdict(int)
    cpt["products_in_initial_listing"] = len(list_id)

    if hideProgressBar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    # status, 0->not treated, -1->error download , 1-> successful download
    df = pd.DataFrame(
        {"safe": list_safename, "status": np.zeros(len(list_safename)), "id": list_id}
    )

    df2, cpt = filter_product_already_present(
        cpt, df, outputdir, force_download=check_on_disk == False
    )

    logging.info("%s", cpt)
    while_loop = 0
    blacklist = []
    while (df2["status"] == 0).any():

        while_loop += 1
        subset_to_treat = df2[df2["status"] == 0]
        dfproductDownloaddable = get_sessions_download_available(
            subset_to_treat,
            hideProgressBar=True,
            blacklist=blacklist,
            logins_group=account_group,
        )
        logging.info(
            "while_loop : %s, prod. to treat: %s, slot avail.:%s, %s",
            while_loop,
            len(subset_to_treat),
            len(dfproductDownloaddable),
            cpt,
        )
        with ThreadPoolExecutor(
            max_workers=len(dfproductDownloaddable)
        ) as executor, tqdm(total=len(dfproductDownloaddable)) as pbar:
            future_to_url = {
                executor.submit(
                    CDS_Odata_download_one_product_v2,
                    dfproductDownloaddable["session"].iloc[jj],
                    dfproductDownloaddable["header"].iloc[jj],
                    dfproductDownloaddable["url"].iloc[jj],
                    dfproductDownloaddable["output_path"].iloc[jj],
                    dfproductDownloaddable["token_semaphore"][jj],
                ): (jj)
                for jj in range(len(dfproductDownloaddable))
            }
            errors_per_account = defaultdict(int)
            for future in as_completed(future_to_url):
                # try:
                (
                    speed,
                    status_meaning,
                    safename_base,
                    semaphore_token_file,
                ) = future.result()
                # remove semaphore once the download is over (successful or not)
                login = os.path.basename(semaphore_token_file).split("_")[3]
                date_generation_access_token = datetime.datetime.strptime(
                    os.path.basename(semaphore_token_file)
                    .split("_")[4]
                    .replace(".txt", ""),
                    "%Y%m%dt%H%M%S",
                )

                remove_semaphore_token_file(
                    token_dir=conf["token_directory"],
                    login=login,
                    date_generation_access_token=date_generation_access_token,
                )
                logging.info("remove session semaphore for %s", login)
                remove_semaphore_session_file(
                    session_dir=conf["active_session_directory"],
                    safename=safename_base,
                    login=login,
                )

                # except KeyboardInterrupt:
                #     cpt["interrupted"] += 1
                #     raise ("keyboard interrupt")
                # except:
                #     logging.error("traceback : %s", traceback.format_exc())
                #     speed = np.nan
                #     status_meaning = "DownloadError"

                if status_meaning == "OK":
                    df2.loc[(df2["safe"] == safename_base), "status"] = 1
                    all_speeds.append(speed)
                    cpt["successful_download"] += 1
                else:
                    df2.loc[(df2["safe"] == safename_base), "status"] = -1
                    errors_per_account[login] += 1
                    logging.info("error found for %s meaning %s", login, status_meaning)
                    # df2["status"][df2["safe"] == safename_base] = -1 # download in error
                cpt["status_%s" % status_meaning] += 1

                pbar.update(1)
            for acco in errors_per_account:
                if errors_per_account[acco] >= MAX_SESSION_PER_ACCOUNT:
                    blacklist.append(acco)
                    logging.info("%s black listed for next loops", acco)
    logging.info("download over.")
    logging.info("counter: %s", cpt)
    # safety remove active session, all reamining because of error
    remove_semaphore_session_file(
        session_dir=conf["active_session_directory"],
        safename=None,
        login=None,
    )

    if len(all_speeds) > 0:
        logging.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )
    return df2


def download_list_product(
    list_id, list_safename, outputdir, specific_account, hideProgressBar=False
):
    """

    Parameters
    ----------
    list_id (list) of string could be hash (eg a1e74573-aa77-55d6-a08d-7b6612761819) provided by CDS Odata
    list_safename (list) of string basename of SAFE product (eg. S1A_IW_GRDH_1SDV_20221013T065030_20221013T0650...SAFE)
    outputdir (str) path where product will be stored
    hideProgressBar (bool): True -> no tqdm progress bar
    specific_account (str):

    Returns
    -------

    """
    assert len(list_id) == len(list_safename)
    cpt = defaultdict(int)
    all_speeds = []
    cpt["products_in_initial_listing"] = len(list_id)
    lst_usable_tokens = get_list_of_exising_token(token_dir=conf["token_directory"])
    if lst_usable_tokens == []:  # in case no token ready to be used -> create new one
        (
            access_token,
            date_generation_access_token,
            login,
            path_semphore_token,
        ) = get_bearer_access_token(
            quiet=hideProgressBar, specific_account=specific_account
        )
    else:  # select randomly one token among existing
        path_semphore_token = random.choice(lst_usable_tokens)
        date_generation_access_token = datetime.datetime.strptime(
            os.path.basename(path_semphore_token).split("_")[4].replace(".txt", ""),
            "%Y%m%dt%H%M%S",
        )
        access_token = open(path_semphore_token).readlines()[0]
    if access_token is not None:
        headers = {"Authorization": "Bearer %s" % access_token}
        logging.debug("headers: %s", headers)
        session = requests.Session()
        session.headers.update(headers)
        if hideProgressBar:
            os.environ["DISABLE_TQDM"] = "True"

        pbar = tqdm(
            range(len(list_id)), disable=bool(os.environ.get("DISABLE_TQDM", False))
        )
        for ii in pbar:
            pbar.set_description("CDSE download %s" % cpt)
            id_product = list_id[ii]
            url_product = conf["URL_download"] % id_product
            safename_product = list_safename[ii]
            if test_safe_archive(safename=safename_product):
                cpt["archived_product"] += 1
            elif test_safe_spool(safename=safename_product):
                cpt["in_spool_product"] += 1
            else:
                cpt["product_absent_from_local_disks"] += 1

                logging.debug("url_product : %s", url_product)
                logging.debug(
                    "id_product : %s safename_product : %s",
                    id_product,
                    safename_product,
                )
                if (
                    datetime.datetime.today() - date_generation_access_token
                ).total_seconds() >= MAX_VALIDITY_ACCESS_TOKEN:
                    logging.info("get a new access token")
                    (
                        access_token,
                        date_generation_access_token,
                        specific_account,
                        path_semphore_token,
                    ) = get_bearer_access_token(specific_account=specific_account)
                    headers = {"Authorization": "Bearer %s" % access_token}
                    session.headers.update(headers)
                else:
                    logging.debug("reuse same access token, still valid.")
                output_filepath = os.path.join(outputdir, safename_product + ".zip")
                # if access_token is None -> crash of the method but it is expected since this method is supposed to be used with a working account
                # path_semaphore_token = write_token_semphore_file(
                #     login=specific_account,
                #     date_generation_access_token=date_generation_access_token,
                #     token_dir=conf["token_directory"],
                #     access_token=access_token,
                # )

                # try:
                (
                    speed,
                    status_meaning,
                    safename_base,
                    path_semphore_token,
                ) = CDS_Odata_download_one_product_v2(
                    session,
                    headers,
                    url=url_product,
                    output_filepath=output_filepath,
                    semaphore_token_file=path_semphore_token,
                )
                remove_semaphore_token_file(
                    token_dir=conf["token_directory"],
                    login=specific_account,
                    date_generation_access_token=date_generation_access_token,
                )
                remove_semaphore_session_file(
                    session_dir=conf["active_session_directory"],
                    safename=safename_base,
                    login=specific_account,
                )
                if status_meaning == "OK":
                    all_speeds.append(speed)
                    cpt["successful_download"] += 1
                cpt["status_%s" % status_meaning] += 1
                # except KeyboardInterrupt:
                #     cpt["interrupted"] += 1
                #     raise ("keyboard interrupt")
                # except:
                #     cpt["download_KO"] += 1
                #     logging.error(
                #         "impossible to fetch %s from CDS: %s",
                #         url_product,
                #         traceback.format_exc(),
                #     )
    logging.info("download over.")
    logging.info("counter: %s", cpt)
    if len(all_speeds) > 0:
        logging.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )


def test_listing_content(listing_path):
    """
    make sure that a lsiting of products to download respect the following format:
        cdse-hash-id,safename
    Arguments:
    ---------
        listing_path (str):
    Returns
    -------

    """
    fid = open(listing_path)
    first_line = fid.readline()
    listing_OK = False
    if "," in first_line:
        if "SAFE" in first_line.split(",")[1] and "S" in first_line.split(",")[1][0]:
            listing_OK = True
    return listing_OK


def add_missing_cdse_hash_ids_in_listing(listing_path):
    """

    Parameters
    ----------
    listing_path (str):

    Returns
    -------

    """
    res = pd.DataFrame({"id": [], "safename": []})
    df_raw = pd.read_csv(listing_path, names=["safenames"])
    df_raw = df_raw[df_raw['safenames'].str.contains('.SAFE')]
    list_safe_a = df_raw["safenames"].values
    delta = datetime.timedelta(seconds=1)
    gdf = gpd.GeoDataFrame(
        {
            # "start_datetime" : [ None  ],
            # "end_datetime"   : [ None ],
            "start_datetime": [ExplodeSAFE(jj).startdate - delta for jj in list_safe_a],
            "end_datetime": [ExplodeSAFE(jj).enddate - delta for jj in list_safe_a],
            # "start_datetime": [
            #     datetime.datetime.strptime(jj.split("_")[5], "%Y%m%dT%H%M%S") - delta
            #     for jj in list_safe_a
            # ],
            # "end_datetime": [
            #     datetime.datetime.strptime(jj.split("_")[6], "%Y%m%dT%H%M%S") + delta
            #     for jj in list_safe_a
            # ],
            "geometry": np.tile([None], len(list_safe_a)),
            "collection": np.tile(["SENTINEL-1"], len(list_safe_a)),
            "name": list_safe_a,
            "sensormode": [ExplodeSAFE(jj).mode for jj in list_safe_a],
            "producttype": [ExplodeSAFE(jj).product[0:3] for jj in list_safe_a],
            "Attributes": np.tile([None], len(list_safe_a)),
        }
    )
    sea_min_pct = 0
    if len(gdf['geometry'])>0:
        collected_data_norm = fetch_data(gdf, min_sea_percent=sea_min_pct)
        if not collected_data_norm is None:
            res = collected_data_norm[["Id", "Name"]]
            res.rename(columns={"Name": "safename"},inplace=True)
            res.rename(columns={"Id": "id"},inplace=True)
    return res


def download_list_product_sequential(
    list_id, list_safename, outputdir, hideProgressBar=False
):
    """

    Parameters
    ----------
    list_id (list) of string could be hash (eg a1e74573-aa77-55d6-a08d-7b6612761819) provided by CDS Odata
    list_safename (list) of string basename of SAFE product (eg. S1A_IW_GRDH_1SDV_20221013T065030_20221013T0650...SAFE)
    outputdir (str) path where product will be stored
    hideProgressBar (bool): True -> no tqdm progress bar
    specific_account (str): default is None [optional]

    Returns
    -------

    """
    assert len(list_id) == len(list_safename)
    logins_group = "logins"
    cpt = defaultdict(int)
    cpt["total_product_to_download"] = len(list_id)
    df = pd.DataFrame(
        {"safe": list_safename, "status": np.zeros(len(list_safename)), "id": list_id}
    )
    df2, cpt = filter_product_already_present(cpt, df, outputdir)

    df_products_downloadable = get_sessions_download_available(
        df2,
        hideProgressBar=hideProgressBar,
        blacklist=None,
        logins_group=logins_group,
    )
    logging.info("product downloadable: %s", len(df_products_downloadable))
    df_products_downloadable["status"] = 0
    if hideProgressBar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    pbar = tqdm(
        range(len(df_products_downloadable)),
        disable=bool(os.environ.get("DISABLE_TQDM", False)),
    )
    for ii in pbar:
        pbar.set_description("CDSE download %s" % cpt)
        # id_product = df2['safe'][ii]
        # url_product = conf["URL_download"] % id_product
        url_product = df_products_downloadable["url"].iloc[ii]
        session = df_products_downloadable["session"].iloc[ii]
        login = os.path.basename(
            df_products_downloadable["session_semaphore"].iloc[ii]
        ).split("_")[3]
        headers = df_products_downloadable["header"].iloc[ii]
        path_semaphore_token = df_products_downloadable["token_semaphore"].iloc[ii]

        output_filepath = df_products_downloadable["output_path"].iloc[ii]
        safename_product = df_products_downloadable["safe"].iloc[ii]
        logging.info("start download : %s", safename_product)
        date_generation_access_token = datetime.datetime.strptime(
            os.path.basename(path_semaphore_token).split("_")[4].replace(".txt", ""),
            "%Y%m%dt%H%M%S",
        )

        logging.debug("url_product : %s", url_product)
        if (
            datetime.datetime.today() - date_generation_access_token
        ).total_seconds() >= MAX_VALIDITY_ACCESS_TOKEN or not os.path.exists(
            path_semaphore_token
        ):

            logging.info("get a new access token")
            (
                access_token,
                date_generation_access_token,
                login,
                path_semaphore_token,
            ) = get_bearer_access_token(
                specific_account=None, account_group=logins_group
            )
            headers = {"Authorization": "Bearer %s" % access_token}
            session.headers.update(headers)
        else:
            logging.debug("reuse same access token, still valid.")
        # output_filepath = os.path.join(outputdir, safename_product + ".zip")
        # write_token_semphore_file() already called in  get_bearer_access_token() , called by get_sessions_download_available()
        # path_semaphore_token = write_token_semphore_file(
        #     login=login,
        #     date_generation_access_token=date_generation_access_token,
        #     token_dir=conf["token_directory"],
        #     access_token=access_token,
        # )
        # try:
        (
            speed,
            status_meaning,
            safename_base,
            path_semaphore_token,
        ) = CDS_Odata_download_one_product_v2(
            session,
            headers,
            url=url_product,
            output_filepath=output_filepath,
            semaphore_token_file=path_semaphore_token,
        )
        # remove the token file, there is a check in the method on its validity
        remove_semaphore_token_file(
            token_dir=conf["token_directory"],
            login=login,
            date_generation_access_token=date_generation_access_token,
        )
        remove_semaphore_session_file(
            session_dir=conf["active_session_directory"],
            safename=safename_base,
            login=login,
        )
        if status_meaning == "OK":
            all_speeds.append(speed)
            df_products_downloadable["status"].iloc[ii] = 1
            cpt["successful_download"] += 1
        else:
            df_products_downloadable["status"].iloc[ii] = -1
        cpt["status_%s" % status_meaning] += 1
        # except KeyboardInterrupt:
        #     cpt["interrupted"] += 1
        #     raise ("keyboard interrupt")
        # except:
        #     cpt["download_KO"] += 1
        #     logging.error(
        #         "impossible to fetch %s from CDS: %s",
        #         url_product,
        #         traceback.format_exc(),
        #     )
    logging.info("download over.")
    logging.info("counter: %s", cpt)
    if len(all_speeds) > 0:
        logging.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )
    return df_products_downloadable


def main():
    """
    package as an alias for this method
    Returns
    -------

    """
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="download-from-CDS")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--hideProgressBar",
        action="store_true",
        default=False,
        help="hide the tqdm progress bar for each prodict download",
    )
    parser.add_argument(
        "--listing",
        required=True,
        help="list of product to treat csv files id,safename",
    )
    parser.add_argument(
        "--login",
        required=True,
        help="CDSE account to be used for download (email address)",
    )
    parser.add_argument(
        "--outputdir",
        required=True,
        help="directory where to store fetch files",
    )

    args = parser.parse_args()
    fmt = "%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s"
    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    t0 = time.time()
    # inputs = open(args.listing).readlines()
    inputdf = pd.read_csv(args.listing, names=["id", "safename"], delimiter=",")
    if not os.path.exists(args.outputdir):
        logging.debug("mkdir on %s", args.outputdir)
        os.makedirs(args.outputdir, 0o0775)
    download_list_product(
        list_id=inputdf["id"].values,
        list_safename=inputdf["safename"].values,
        outputdir=args.outputdir,
        hideProgressBar=args.hideProgressBar,
        specific_account=args.login,
    )
    logging.info("end of function")
