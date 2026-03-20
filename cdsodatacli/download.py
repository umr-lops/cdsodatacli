import requests
import logging
from tqdm import tqdm
import datetime
import time
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import warnings
import traceback
import shutil
import pandas as pd
import geopandas as gpd
from requests.exceptions import ChunkedEncodingError
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
import numpy as np
from cdsodatacli.fetch_access_token import (
    get_bearer_access_token,
    get_valid_access_token,
    MAX_VALIDITY_ACCESS_TOKEN,
)
from cdsodatacli.session import (
    remove_semaphore_session_file,
    get_sessions_download_available,
    MAX_SESSION_PER_ACCOUNT,
)
from cdsodatacli.query import fetch_data, WORLDPOLYGON
from cdsodatacli.utils import (
    get_conf,
    check_safe_in_archive,
    check_safe_in_spool,
    check_safe_in_outputdir,
)
from cdsodatacli.product_parser import ExplodeSAFE
from collections import defaultdict

CHECK_INTERVAL = 1800  # seconds
# chunksize = 4096
chunksize = 8192  # like in the CDSE example
MAX_RETRIES = 2

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
    session,
    headers,
    url,
    output_filepath,
    conf,
):
    """

     v2 is without tqdm

    Parameters
    ----------
    session (request Obj)
    headers (dict)
    url (str)
    output_filepath (str): full path where to store fetch file
    conf (dict): configuration

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
    # conf = get_conf(path_config_file=cdsodatacli_conf_file)
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
            except ChunkedEncodingError:
                status = -1
                status_meaning = "ChunkedEncodingError"
    if (not response.ok or status == -1) and os.path.exists(output_filepath_tmp):
        logging.debug("remove empty file %s", output_filepath_tmp)
        os.remove(output_filepath_tmp)
    elapsed_time = time.time() - t0

    # Dans CDS_Odata_download_one_product_v2, utiliser shutil.copy2 + os.remove
    # directement, sans passer par shutil.move qui retente os.rename en interne
    if status == 200 and os.path.exists(output_filepath_tmp):
        speed = total_length / elapsed_time
        try:
            shutil.copy2(output_filepath_tmp, output_filepath)
            os.remove(output_filepath_tmp)
            os.chmod(output_filepath, mode=0o0775)
        except Exception as e:
            logging.error("Failed to move %s: %s", output_filepath_tmp, e)
            if os.path.exists(output_filepath_tmp):
                os.remove(output_filepath_tmp)  # nettoyer quoi qu'il arrive
            status_meaning = "MoveError"

        # except OSError as e:
        #     logging.error("Failed to move %s: %s", output_filepath_tmp, e)
        #     status_meaning = "MoveError"
    logging.debug("time to download this product: %1.1f sec", elapsed_time)
    logging.debug("average download speed: %1.1fMo/sec", speed)
    return speed, status_meaning, safename_base


def cds_s3_download_one_product(
    s3_path,       
    output_filepath,
    conf,
):
    """
    Download a single SAFE product via the CDSE S3 endpoint using boto3.


    
    The conf dict must contain:
        - pre_spool   (str): temp directory for .tmp files
        - s3_access_key (str): CDSE S3 access key
        - s3_secret_key (str): CDSE S3 secret key
        - s3_endpoint   (str): e.g. "https://eodata.dataspace.copernicus.eu"
        - s3_bucket     (str): e.g. "eodata"
        - s3_path       (str): prefix inside the bucket, e.g.
                               "Sentinel-1/SAR/GRD/2022/05/03/S1A_IW_GRDH_1SDV_20220503T000000.SAFE"
        - s3_region     (str): 'default' CDSE requires "default"

    Argument:
        s3_path (str): e.g. "Sentinel-1/SAR/GRD/2022/05/03/S1A_IW_GRDH_1SDV_20220503T000000.SAFE"
        output_filepath (str): output full path when download is finished (not the pre-spool but the spool)
        conf (dict): configuration see details above

    Returns
    -------
        speed         (float): download speed in Mo/second
        elapsed_time (int): number of seconds to download the product
        total_mb (int): MegaBytes downloaded (zip)
        status_meaning (str): human-readable outcome
        safename_base  (str): basename without .zip
    """


    speed = np.nan
    status_meaning = "unknown"
    t0 = time.time()

    safename_base = os.path.basename(output_filepath).replace(".zip", "")
    output_filepath_tmp = os.path.join(
        conf["pre_spool"], os.path.basename(output_filepath) + ".tmp"
    )

    try:
        s3 = boto3.resource(
            "s3",
            endpoint_url=conf["s3_endpoint"],
            aws_access_key_id=conf["s3_access_key"],
            aws_secret_access_key=conf["s3_secret_key"],
            region_name=conf.get("s3_region", "default"),
        )
        bucket = s3.Bucket(conf["s3_bucket"])

        # List all objects under the SAFE prefix
        objects = list(bucket.objects.filter(Prefix=s3_path))
        if not objects:
            raise FileNotFoundError(f"No S3 objects found under prefix: {s3_path}")

        total_bytes = sum(obj.size for obj in objects)
        total_mb = total_bytes / 1e6
        logging.debug("Total size to download: %.1f Mo", total_mb)

        # For a zipped single-file product, download into tmp then move
        # For a .SAFE folder (multi-file), download each file in place
        if len(objects) == 1:
            obj = objects[0]
            logging.debug("Downloading single object %s -> %s", obj.key, output_filepath_tmp)
            bucket.download_file(obj.key, output_filepath_tmp)

            elapsed_time = time.time() - t0
            speed = total_mb / elapsed_time

            try:
                shutil.copy2(output_filepath_tmp, output_filepath)
                os.remove(output_filepath_tmp)
                os.chmod(output_filepath, mode=0o0775)
                status_meaning = "Downloaded"
            except Exception as e:
                logging.error("Failed to move %s: %s", output_filepath_tmp, e)
                if os.path.exists(output_filepath_tmp):
                    os.remove(output_filepath_tmp)
                status_meaning = "MoveError"

        else:
            # Multi-file .SAFE: reconstruct directory tree under output_filepath
            for obj in objects:
                relative_key = os.path.relpath(obj.key, s3_path)
                local_file = os.path.join(output_filepath, relative_key)
                os.makedirs(os.path.dirname(local_file), exist_ok=True)
                if not obj.key.endswith("/"):  # skip folder pseudo-objects
                    logging.debug("Downloading %s -> %s", obj.key, local_file)
                    bucket.download_file(obj.key, local_file)

            elapsed_time = time.time() - t0
            speed = total_mb / elapsed_time
            status_meaning = "Downloaded"

    except FileNotFoundError as e:
        logging.error("S3 product not found: %s", e)
        status_meaning = "NotFound"
        elapsed_time = time.time() - t0
    except (BotoCoreError, ClientError) as e:
        logging.error("S3 error while downloading %s: %s", output_filepath, e)
        status_meaning = "S3Error"
        elapsed_time = time.time() - t0
        if os.path.exists(output_filepath_tmp):
            os.remove(output_filepath_tmp)

    logging.debug("time to download this product: %1.1f sec", elapsed_time)
    logging.debug("average download speed: %1.1f Mo/sec", speed)
    return speed,elapsed_time,total_mb, status_meaning, safename_base

def filter_product_already_present(
    cpt, df, outputdir, cdsodatacli_conf, force_download=False
):
    """
    Based on a dataframe of products to download, filter those already present locally.


    Parameters
    ----------
    cpt (collections.defaultdict(int))
    df (pd.DataFrame)
    outputdir (str)
    cdsodatacli_conf (dict): configuration dictionary of the lib cdsodatacli
    force_download (bool): True -> download all products even if already present locally [optional, default is False]


    Returns
    -------
        df_todownload (pd.DataFrame): dataframe of products to download
        cpt (collections.defaultdict(int)): updated counter

    """
    if "id" not in df.columns:
        id_present = False
    else:
        id_present = True

    all_output_filepath = []
    all_urls_to_download = []
    index_to_download = []
    for ii in tqdm(range(len(df["safe"]))):
        # for ii, safename_product in enumerate(df["safe"]):
        safename_product = df["safe"].iloc[ii]
        to_download = False
        if force_download:
            to_download = True
        is_in_archive, archive_file = check_safe_in_archive(
            safename=safename_product, conf=cdsodatacli_conf
        )
        if is_in_archive:
            beg_archive = ("-").join(archive_file.split("/")[0:3])
            cpt["preproc-archive_%s" % beg_archive] += 1
            cpt["preproc-archived_product"] += 1
        elif check_safe_in_spool(safename=safename_product, conf=cdsodatacli_conf):
            cpt["preproc-in_spool_product"] += 1
        elif check_safe_in_outputdir(outputdir=outputdir, safename=safename_product):
            cpt["preproc-in_outdir_product"] += 1
        else:
            to_download = True
            cpt["preproc-product_absent_from_local_disks"] += 1
        if to_download:
            index_to_download.append(ii)
            if id_present:
                id_product = df["id"].iloc[ii]
                url_product = cdsodatacli_conf["URL_download"] % id_product

                logging.debug("url_product : %s", url_product)
                logging.debug(
                    "id_product : %s safename_product : %s",
                    id_product,
                    safename_product,
                )
                all_urls_to_download.append(url_product)

            output_filepath = os.path.join(outputdir, safename_product + ".zip")
            all_output_filepath.append(output_filepath)

    df_todownload = df.iloc[index_to_download]
    if id_present:
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
    cdsodatacli_conf_file=None,
):
    """
    .. deprecated::
        Use :func:`download_list_product_multithread_v3` instead. Will be removed in next release.
    v2 is handling multi account round-robin and token semaphore files
    Parameters
    ----------
    list_id (list): product hash
    list_safename (list): product names
    outputdir (str): the directory where to store the product collected
    hideProgressBar (bool): True -> no tqdm progress bar in stdout
    account_group (str): the name of the group of CDSE logins to be used
    check_on_disk (bool): True -> if the product is in the spool dir or in archive dir the download is skipped
    cdsodatacli_conf_file (str): path to the cdsodatacli configuration file [ optional, default is None -> use cdsodatacli default behavior]

    Returns
    -------
        df2 (pd.DataFrame):
    """
    warnings.warn(
        "download_list_product_multithread_v2 is deprecated and will be removed in next release. "
        "Use get_bearer_access_token instead.",
        DeprecationWarning,
        stacklevel=2,  # pointe vers l'appelant, pas vers cette ligne
    )
    assert len(list_id) == len(list_safename)
    logging.info("check_on_disk : %s", check_on_disk)
    cpt = defaultdict(int)
    cpt["products_in_initial_listing"] = len(list_id)
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    if hideProgressBar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    # status, 0->not treated, -1->error download , 1-> successful download
    df = pd.DataFrame(
        {"safe": list_safename, "status": np.zeros(len(list_safename)), "id": list_id}
    )
    force_download = not check_on_disk
    df2, cpt = filter_product_already_present(
        cpt, df, outputdir, force_download=force_download, cdsodatacli_conf=conf
    )

    logging.info("%s", cpt)
    while_loop = 0
    blacklist = []
    while (df2["status"] == 0).any():

        while_loop += 1
        subset_to_treat = df2[df2["status"] == 0]
        dfproductDownloaddable = get_sessions_download_available(
            conf,
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
        with (
            ThreadPoolExecutor(max_workers=len(dfproductDownloaddable)) as executor,
            tqdm(total=len(dfproductDownloaddable)) as pbar,
        ):
            future_to_url = {
                executor.submit(
                    CDS_Odata_download_one_product_v2,
                    dfproductDownloaddable["session"].iloc[jj],
                    dfproductDownloaddable["header"].iloc[jj],
                    dfproductDownloaddable["url"].iloc[jj],
                    dfproductDownloaddable["output_path"].iloc[jj],
                    # dfproductDownloaddable["token_semaphore"][jj],
                    conf=conf,
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
                    # semaphore_token_file,
                ) = future.result()
                # remove semaphore once the download is over (successful or not)
                # login = os.path.basename(semaphore_token_file).split("_")[3]
                # date_generation_access_token = datetime.datetime.strptime(
                #     os.path.basename(semaphore_token_file)
                #     .split("_")[4]
                #     .replace(".txt", ""),
                #     "%Y%m%dt%H%M%S",
                # )

                # remove_semaphore_token_file(
                #     token_dir=conf["token_directory"],
                #     login=login,
                #     date_generation_access_token=date_generation_access_token,
                # )
                login = dfproductDownloaddable["login"][
                    dfproductDownloaddable["safe"] == safename_base
                ].values
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
    list_id,
    list_safename,
    outputdir,
    specific_account,
    specific_passwd=None,
    hideProgressBar=False,
    conf=None,
):
    """

    Parameters
    ----------
    list_id (list) of string could be hash (eg a1e74573-aa77-55d6-a08d-7b6612761819) provided by CDS Odata
    list_safename (list) of string basename of SAFE product (eg. S1A_IW_GRDH_1SDV_20221013T065030_20221013T0650...SAFE)
    outputdir (str) path where product will be stored
    specific_account (str): CDSE account to use
    specific_passwd (str): optional, None -> password is found from conf
    hideProgressBar (bool): True -> no tqdm progress bar
    cdsodatacli_conf (dict): configuration


    Returns
    -------

    """
    assert len(list_id) == len(list_safename)
    # conf = get_conf(path_config_file=cdsodatacli_conf_file)
    cpt = defaultdict(int)
    all_speeds = []
    cpt["products_in_initial_listing"] = len(list_id)
    # lst_usable_tokens = get_list_of_existing_token_semaphore_file(
    #     token_dir=conf["token_directory"]
    # )

    (
        access_token,
        date_generation_access_token,
        login,
    ) = get_bearer_access_token(
        conf=conf,
        # quiet=hideProgressBar,
        specific_account=specific_account,
        specific_psswd=specific_passwd,
    )
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
            if check_safe_in_archive(safename=safename_product, conf=conf):
                cpt["archived_product"] += 1
            elif check_safe_in_spool(safename=safename_product, conf=conf):
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
                    ) = get_bearer_access_token(
                        conf=conf, specific_account=specific_account
                    )
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
                    # path_semphore_token,
                ) = CDS_Odata_download_one_product_v2(
                    session,
                    headers,
                    url=url_product,
                    output_filepath=output_filepath,
                    # semaphore_token_file=path_semphore_token,
                    conf=conf,
                )
                # remove_semaphore_token_file(
                #     token_dir=conf["token_directory"],
                #     login=specific_account,
                #     date_generation_access_token=date_generation_access_token,
                # )
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
    return cpt


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


def add_missing_cdse_hash_ids_in_listing(
    listing_path, display_tqdm=False, email=None, password=None
):
    """
    Add a column of CDSE hash id in a listing of products to download based on the safenames. This is useful for instance for the private data IOC products since the CDSE Odata search does not return the hash id for those products but only the safename. The method is using the same query method as the one used in the CDSE Odata search script (opensearch_private_data_IOC.py) to retrieve the hash id associated to each safename.

    Args:
        listing_path (str):
        display_tqdm (bool): True -> tqdm progress bar for each queries [optional, default=False]
        email (str): email of the CDSE account to use for queries [optional, default None -> use cdsodatacli default behavior]
        password (str): password of the CDSE account to use for queries [optional, default None -> use cdsodatacli default behavior]

    Returns:
        res (pd.DataFrame): dataframe with 2 columns "id" and "safename" containing the hash id provided by CDSE Odata and the safename of the product

    """
    res = pd.DataFrame({"id": [], "safename": []})
    df_raw = pd.read_csv(listing_path, names=["safenames"])
    df_raw = df_raw[df_raw["safenames"].str.contains(".SAFE")]
    list_safe_a = df_raw["safenames"].values
    delta = datetime.timedelta(seconds=1)
    # We generate 8 bytes (16 chars) and slice off the last one to get 15.
    # hash_list_queries = [secrets.token_hex(8)[:15] for _ in range(len(list_safe_a))] # no efficient in this case.
    hash_list_queries = np.tile(["batch_query"], len(list_safe_a))
    # specific for private data IOC (S1D for instance)
    product_types = []
    for ii in range(len(list_safe_a)):
        if list_safe_a[ii].startswith("S1D"):
            product_types.append(list_safe_a[ii][4:14] + "_PRIVATE")

        else:
            product_types.append(list_safe_a[ii][4:14])
    # in product_types API expect for instance IW_GRDH_1S or WV_SLC__1S_PRIVATE.
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
            "geometry": np.tile([WORLDPOLYGON], len(list_safe_a)),
            "collection": np.tile(["SENTINEL-1"], len(list_safe_a)),
            "name": list_safe_a,
            "sensormode": [ExplodeSAFE(jj).mode for jj in list_safe_a],
            "producttype": product_types,
            "Attributes": np.tile([None], len(list_safe_a)),
            # "id_query": np.tile(["dummy2getProducthash"], len(list_safe_a)),
            "id_query": hash_list_queries,
        }
    )

    sea_min_pct = None
    if len(gdf["geometry"]) > 0:
        collected_data_norm = fetch_data(
            gdf,
            min_sea_percent=sea_min_pct,
            display_tqdm=display_tqdm,
            email=email,
            password=password,
        )
        if collected_data_norm is not None:
            res = collected_data_norm[["Id", "Name"]]
            res.rename(columns={"Name": "safename"}, inplace=True)
            res.rename(columns={"Id": "id"}, inplace=True)
    return res


def download_list_product_sequential(
    list_id, list_safename, outputdir, hideProgressBar=False, cdsodatacli_conf_file=None
):
    """

    Parameters
    ----------
    list_id (list) of string could be hash (eg a1e74573-aa77-55d6-a08d-7b6612761819) provided by CDS Odata
    list_safename (list) of string basename of SAFE product (eg. S1A_IW_GRDH_1SDV_20221013T065030_20221013T0650...SAFE)
    outputdir (str) path where product will be stored
    hideProgressBar (bool): True -> no tqdm progress bar
    specific_account (str): default is None [optional]
    cdsodatacli_conf_file (str): path to the cdsodatacli configuration file [optional]

    Returns
    -------

    """
    assert len(list_id) == len(list_safename)
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    logins_group = "logins"
    cpt = defaultdict(int)
    cpt["total_product_to_download"] = len(list_id)
    df = pd.DataFrame(
        {"safe": list_safename, "status": np.zeros(len(list_safename)), "id": list_id}
    )
    df2, cpt = filter_product_already_present(cpt, df, outputdir, cdsodatacli_conf=conf)

    df_products_downloadable = get_sessions_download_available(
        conf,
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
        # path_semaphore_token = df_products_downloadable["token_semaphore"].iloc[ii]

        output_filepath = df_products_downloadable["output_path"].iloc[ii]
        safename_product = df_products_downloadable["safe"].iloc[ii]
        logging.info("start download : %s", safename_product)
        # date_generation_access_token = datetime.datetime.strptime(
        #     os.path.basename(path_semaphore_token).split("_")[4].replace(".txt", ""),
        #     "%Y%m%dt%H%M%S",
        # )
        access_token, date_generation_access_token = get_valid_access_token(login)
        logging.debug("url_product : %s", url_product)
        if access_token is None:

            logging.info("get a new access token")
            (
                access_token,
                date_generation_access_token,
                login,
            ) = get_bearer_access_token(
                conf=conf, specific_account=None, account_group=logins_group
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
            # path_semaphore_token,
        ) = CDS_Odata_download_one_product_v2(
            session,
            headers,
            url=url_product,
            output_filepath=output_filepath,
            # semaphore_token_file=path_semaphore_token,
            conf=conf,
        )
        # remove the token file, there is a check in the method on its validity
        # remove_semaphore_token_file(
        #     token_dir=conf["token_directory"],
        #     login=login,
        #     date_generation_access_token=date_generation_access_token,
        # )
        remove_semaphore_session_file(
            session_dir=conf["active_session_directory"],
            safename=safename_base,
            login=login,
        )
        if status_meaning == "OK":
            all_speeds.append(speed)
            # Using .at with the specific index label is safe and fast
            df_products_downloadable.at[
                df_products_downloadable.index[ii], "status"
            ] = 1
            cpt["successful_download"] += 1
        else:
            df_products_downloadable.at[
                df_products_downloadable.index[ii], "status"
            ] = -1
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


def download_list_product_multithread_v3(
    list_id,
    list_safename,
    outputdir,
    account_group,
    hideProgressBar=False,
    check_on_disk=True,
    cdsodatacli_conf_file=None,
):
    """
    v3 is working as deamon (while loop) multi account round-robin
      and token semaphore files
    In this method is working for a group of account with one or many account.
    Each account can run 4 parallel sessions.

    Parameters
    ----------
    list_id (list): list of satellite product hashs
    list_safename (list): list of product names
    outputdir (str): the directory where to store the product collected
    account_group (str): a group define in the config file with a unique account -> 4 sessions in parallel
    hideProgressBar (bool): True -> no tqdm progress bar in stdout
    check_on_disk (bool): True -> if the product is in the spool dir or in archive dir the download is skipped
    cdsodatacli_conf_file (str): path to the cdsodatacli configuration file [ optional, default is None -> use cdsodatacli default behavior]

    Returns
    -------
        df2 (pd.DataFrame):
    """
    assert len(list_id) == len(list_safename)
    logging.info("check_on_disk : %s", check_on_disk)
    cpt = defaultdict(int)
    cpt["products_in_initial_listing"] = len(list_id)
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    if hideProgressBar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    # status, 0->not treated, -1->error download , 1-> successful download
    df = pd.DataFrame(
        {"safe": list_safename, "status": np.zeros(len(list_safename)), "id": list_id}
    )
    force_download = not check_on_disk
    df2, cpt = filter_product_already_present(
        cpt, df, outputdir, force_download=force_download, cdsodatacli_conf=conf
    )
    t_start_download = time.time()
    logging.info("%s", cpt)
    while_loop = 0
    blacklist = []
    running_futures = set()
    future_to_info = {}
    max_parallel_download = 0
    # retries = defaultdict(int)
    pbar = tqdm(total=len(df2))
    max_parallelism_seek = MAX_SESSION_PER_ACCOUNT * len(conf[account_group])
    # token = get_access_token(email=specific_account, password=account_passwd)
    with (ThreadPoolExecutor(max_workers=max_parallelism_seek) as executor,):

        while (df2["status"] == 0).any():

            while_loop += 1

            subset_to_treat = df2[df2["status"] == 0]
            pbar.set_description(
                f"loop={while_loop} | OK={cpt['successful_download']} | ERR={sum(v for k,v in cpt.items() if k.startswith('status_') and k != 'status_OK')} | todo={len(subset_to_treat)} | //={len(running_futures)}"
            )
            if len(subset_to_treat) == 0:
                logging.info(
                    "All the products have been treated (success or error).Nothing to do, exiting loop"
                )
                break
            # get the 4 download session information that can be submit in //
            df_prod_downloadable = get_sessions_download_available(
                conf,
                subset_to_treat,
                hideProgressBar=True,
                blacklist=blacklist,
                logins_group=account_group,
            )
            urls_index = list(df_prod_downloadable.index)
            logging.debug(
                "while_loop : %s, prod. to treat: %s, %s",
                while_loop,
                len(subset_to_treat),
                cpt,
            )
            # if len(df_prod_downloadable) == 0:
            if len(df_prod_downloadable) == 0:
                logging.debug("no session available wait a bit")
                time.sleep(5)
                continue
            errors_per_account = defaultdict(int)

            currently_downloading = set(
                info["safename"] for info in future_to_info.values()
            )
            # 1) Submit as many futures as possible
            while urls_index and len(running_futures) < max_parallelism_seek:
                url_one_index = urls_index.pop(0)
                # id_product = subset_to_treat['id'].iloc[url_one_index]
                # safename_base = subset_to_treat["safe"].iloc[url_one_index]
                # safename_base = subset_to_treat["safe"].loc[url_one_index]  # label-based
                safename_base = df_prod_downloadable["safe"].loc[url_one_index]
                logintobeused = df_prod_downloadable["login"].loc[url_one_index]
                assert isinstance(safename_base, str)
                if safename_base in currently_downloading:
                    logging.debug("skipping %s already being downloaded", safename_base)
                    continue
                # (
                # access_token,
                # date_generation_access_token,
                # login,
                # path_semaphore_token,
                # ) = get_bearer_access_token(
                #     conf=conf, specific_account=acount_email,
                #     account_group=None
                # )
                # headers = {"Authorization": "Bearer %s" % access_token}
                # session.headers.update(headers)
                # url_product = cdsodatacli_conf_file["URL_download"] % id_product
                # output_path = subset_to_treat['output_path'].iloc[url_one_index]
                # future = executor.submit(download, url)
                # session = df_prod_downloadable["session"].iloc[url_one_index]
                # header = df_prod_downloadable["header"].iloc[url_one_index]
                # url_product = df_prod_downloadable["url"].iloc[url_one_index]
                # output_path = df_prod_downloadable["output_path"].iloc[url_one_index]

                # Corrected lines inside download_list_product_multithread_v3
                session = df_prod_downloadable["session"].loc[url_one_index]
                header = df_prod_downloadable["header"].loc[url_one_index]
                url_product = df_prod_downloadable["url"].loc[url_one_index]
                output_path = df_prod_downloadable["output_path"].loc[url_one_index]
                # path_semaphore_token = df_prod_downloadable["token_semaphore"].loc[
                #     url_one_index
                # ]  # Added .loc

                # session = df_prod_downloadable["session"].loc[url_one_index]
                # header = df_prod_downloadable["header"].loc[url_one_index]
                # url_product = df_prod_downloadable["url"].loc[url_one_index]
                # output_path = df_prod_downloadable["output_path"].loc[url_one_index]
                # path_semaphore_token = df_prod_downloadable["token_semaphore"][
                #     url_one_index
                # ]
                future = executor.submit(
                    CDS_Odata_download_one_product_v2,
                    session,
                    header,
                    url_product,
                    output_path,
                    # path_semaphore_token,
                    conf=conf,
                )
                future_to_info[future] = {
                    "safename": safename_base,
                    "login": logintobeused,
                    # "semaphore_token_file": path_semaphore_token,
                }
                currently_downloading.add(safename_base)  # mettre à jour immédiatement
                # retries[safename_base] += 1
                running_futures.add(future)
            # small check to know what is the maximum download parallelism we can reach
            if len(running_futures) > max_parallel_download:
                max_parallel_download = len(running_futures)
            # 2) Wait for at least one download to finish
            done, running_futures = wait(
                running_futures, timeout=None, return_when=FIRST_COMPLETED
            )

            # 3) Handle completed downloads
            for future in done:
                info = future_to_info.pop(future, {})
                safename_base = info.get("safename", "unknown")
                login_used = info.get("login", "unknown")
                try:
                    # process result
                    (
                        speed,
                        status_meaning,
                        safename_base,
                        # semaphore_token_file,
                    ) = future.result()
                    # future_to_info.pop(future, None)
                except Exception:

                    logging.error(
                        "Unhandled exception for %s: %s",
                        safename_base,
                        traceback.format_exc(),
                    )
                    df2.loc[(df2["safe"] == safename_base), "status"] = -1
                    pbar.update(1)
                    continue

                logging.debug("remove session semaphore for %s", login_used)
                remove_semaphore_session_file(
                    session_dir=conf["active_session_directory"],
                    safename=safename_base,
                    login=login_used,
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
                    errors_per_account[login_used] += 1
                    logging.info(
                        "error found for %s meaning %s", login_used, status_meaning
                    )
                    # df2["status"][df2["safe"] == safename_base] = -1 # download in error
                # if retries[safename_base] > MAX_RETRIES:
                # df2.loc[(df2["safe"] == safename_base),"status"] = -1
                cpt["status_%s" % status_meaning] += 1

                pbar.update(1)

                # except Exception as e:
                #     # handle error
                #     print("Download failed:", e)
            for acco in errors_per_account:
                if errors_per_account[acco] >= MAX_SESSION_PER_ACCOUNT:
                    blacklist.append(acco)
                    logging.info("%s black listed for next loops", acco)
    elapsed_time = time.time() - t_start_download
    logging.info("download over in %f seconds", elapsed_time)
    logging.info("counter: %s", cpt)
    logging.info("maximum parallelism reached : %i", max_parallel_download)
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


def main():
    """
    download data from an existing listing of product
    package as an alias for this method
    Returns
    -------

    """
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    import argparse

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
    parser.add_argument(
        "--cdsodatacli_conf_file",
        required=False,
        default=None,
        help="path to the cdsodatacli configuration file .yml",
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
    conf = get_conf(path_config_file=args.cdsodatacli_conf_file)
    download_list_product(
        list_id=inputdf["id"].values,
        list_safename=inputdf["safename"].values,
        outputdir=args.outputdir,
        hideProgressBar=args.hideProgressBar,
        specific_account=args.login,
        conf=conf,
    )
    elapsed = t0 - time.time()
    logging.info("end of function in %s seconds", elapsed)
