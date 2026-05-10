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
    get_sessions_download_available_s3,
    release_s3_session_after_usage,
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

logger = logging.getLogger(__name__)
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
#         logger.info("Downloading %s" % output_filepath)
#         response = session.get(url, headers=headers, stream=True)
#         total_length = int(int(response.headers.get("content-length")) / 1000 / 1000)
#         logger.debug("total_length : %s Mo", total_length)
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
#     logger.info("time to download this product: %1.1f sec", elapsed_time)
#     speed = total_length / elapsed_time
#     logger.info("average download speed: %1.1fMo/sec", speed)
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
        logger.debug("Downloading %s" % output_filepath)
        response = session.get(url, headers=headers, stream=True)
        status = response.status_code
        status_meaning = response.reason
        # Check for 'Transfer-Encoding: chunked'
        if (
            "Transfer-Encoding" in response.headers
            and response.headers["Transfer-Encoding"] == "chunked"
        ):
            logger.warning(
                "Server is using 'Transfer-Encoding: chunked'. Content length may not be accurate."
            )
        if response.ok:
            total_length = int(
                int(response.headers.get("content-length")) / 1000 / 1000
            )
            logger.debug("total_length : %s Mo", total_length)
            try:
                for chunk in response.iter_content(chunk_size=chunksize):
                    if chunk:
                        f.write(chunk)
            except ChunkedEncodingError:
                status = -1
                status_meaning = "ChunkedEncodingError"
    if (not response.ok or status == -1) and os.path.exists(output_filepath_tmp):
        logger.debug("remove empty file %s", output_filepath_tmp)
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
            logger.error("Failed to move %s: %s", output_filepath_tmp, e)
            if os.path.exists(output_filepath_tmp):
                os.remove(output_filepath_tmp)  # nettoyer quoi qu'il arrive
            status_meaning = "MoveError"

        # except OSError as e:
        #     logger.error("Failed to move %s: %s", output_filepath_tmp, e)
        #     status_meaning = "MoveError"
    logger.debug("time to download this product: %1.1f sec", elapsed_time)
    logger.debug("average download speed: %1.1fMo/sec", speed)
    return speed, status_meaning, safename_base


def cds_s3_download_one_product(
    s3_path,
    s3_credentials,
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
        s3_credentials (dict): with keys 's3-access-key' and 's3-secret'
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
    total_mb = 0
    status_meaning = "unknown"
    t0 = time.time()

    safename_base = os.path.basename(output_filepath).replace(".zip", "")
    output_filepath_tmp = os.path.join(
        conf["pre_spool"], os.path.basename(output_filepath) + ".tmp"
    )
    # s3_credentials = None
    try:
        # s3_credentials, s3_resources = _get_fresh_s3_client(conf, headers=header)
        # s3 = boto3.resource(
        #     "s3",
        #     endpoint_url=conf["s3_endpoint"],
        #     aws_access_key_id=conf["s3_access_key"],
        #     aws_secret_access_key=conf["s3_secret_key"],
        #     region_name=conf.get("s3_region", "default"),
        # )
        s3_resources = boto3.resource(
            "s3",
            endpoint_url=conf.get(
                "s3_endpoint", "https://eodata.dataspace.copernicus.eu"
            ),
            aws_access_key_id=s3_credentials["s3-access-key"],
            aws_secret_access_key=s3_credentials["s3-secret"],
            region_name=conf.get("s3_region", "default"),
        )
        bucket = s3_resources.Bucket(conf["s3_bucket"])

        # List all objects under the SAFE prefix
        objects = list(bucket.objects.filter(Prefix=s3_path))
        if not objects:
            raise FileNotFoundError(f"No S3 objects found under prefix: {s3_path}")

        total_bytes = sum(obj.size for obj in objects)
        total_mb = total_bytes / 1e6
        logger.debug("Total size to download: %.1f Mo", total_mb)

        # For a zipped single-file product, download into tmp then move
        # For a .SAFE folder (multi-file), download each file in place
        if len(objects) == 1:
            obj = objects[0]
            logger.info("object key: %s, size: %.1f Mo", obj.key, obj.size / 1e6)
            logger.debug(
                "Downloading single object %s -> %s", obj.key, output_filepath_tmp
            )
            bucket.download_file(obj.key, output_filepath_tmp)

            elapsed_time = time.time() - t0
            speed = total_mb / elapsed_time

            try:
                shutil.copy2(output_filepath_tmp, output_filepath)
                os.remove(output_filepath_tmp)
                os.chmod(output_filepath, mode=0o0775)
                status_meaning = "Downloaded"
            except Exception as e:
                logger.error("Failed to move %s: %s", output_filepath_tmp, e)
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
                    logger.debug("Downloading %s -> %s", obj.key, local_file)
                    bucket.download_file(obj.key, local_file)

            elapsed_time = time.time() - t0
            speed = total_mb / elapsed_time
            status_meaning = "Downloaded"

    except FileNotFoundError as e:
        logger.error("S3 product not found: %s", e)
        status_meaning = "NotFound"
        elapsed_time = time.time() - t0
    except (BotoCoreError, ClientError) as e:
        logger.error("S3 error while downloading %s: %s", output_filepath, e)
        status_meaning = "S3Error"
        elapsed_time = time.time() - t0
        logger.debug("%s", traceback.format_exc())
        if os.path.exists(output_filepath_tmp):
            os.remove(output_filepath_tmp)

    logger.debug("time to download this product: %1.1f sec", elapsed_time)
    logger.debug("average download speed: %1.1f Mo/sec", speed)
    return speed, elapsed_time, total_mb, status_meaning, safename_base


def filter_product_already_present(
    cpt, df, outputdir, cdsodatacli_conf, force_download=False, extension=".zip"
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
    extension (str): file extension to check for presence on disk, default is ".zip" but can be ".SAFE" if products are already unzipped in the outputdir


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

                logger.debug("url_product : %s", url_product)
                logger.debug(
                    "id_product : %s safename_product : %s",
                    id_product,
                    safename_product,
                )
                all_urls_to_download.append(url_product)

            output_filepath = os.path.join(outputdir, safename_product + extension)
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
    logger.info("check_on_disk : %s", check_on_disk)
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

    logger.info("%s", cpt)
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
        logger.info(
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
                logger.info("remove session semaphore for %s", login)
                remove_semaphore_session_file(
                    session_dir=conf["active_session_directory"],
                    safename=safename_base,
                    login=login,
                )

                # except KeyboardInterrupt:
                #     cpt["interrupted"] += 1
                #     raise ("keyboard interrupt")
                # except:
                #     logger.error("traceback : %s", traceback.format_exc())
                #     speed = np.nan
                #     status_meaning = "DownloadError"

                if status_meaning == "OK":
                    df2.loc[(df2["safe"] == safename_base), "status"] = 1
                    all_speeds.append(speed)
                    cpt["successful_download"] += 1
                else:
                    df2.loc[(df2["safe"] == safename_base), "status"] = -1
                    errors_per_account[login] += 1
                    logger.info("error found for %s meaning %s", login, status_meaning)
                    # df2["status"][df2["safe"] == safename_base] = -1 # download in error
                cpt["status_%s" % status_meaning] += 1

                pbar.update(1)
            for acco in errors_per_account:
                if errors_per_account[acco] >= MAX_SESSION_PER_ACCOUNT:
                    blacklist.append(acco)
                    logger.info("%s black listed for next loops", acco)
    logger.info("download over.")
    logger.info("counter: %s", cpt)
    # safety remove active session, all reamining because of error
    remove_semaphore_session_file(
        session_dir=conf["active_session_directory"],
        safename=None,
        login=None,
    )

    if len(all_speeds) > 0:
        logger.info(
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
    cpt["product_absent_from_local_disks"] = 0
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
        logger.debug("headers: %s", headers)
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

                logger.debug("url_product : %s", url_product)
                logger.debug(
                    "id_product : %s safename_product : %s",
                    id_product,
                    safename_product,
                )
                if (
                    datetime.datetime.today() - date_generation_access_token
                ).total_seconds() >= MAX_VALIDITY_ACCESS_TOKEN:
                    logger.info("get a new access token")
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
                    logger.debug("reuse same access token, still valid.")
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
                #     logger.error(
                #         "impossible to fetch %s from CDS: %s",
                #         url_product,
                #         traceback.format_exc(),
                #     )
    logger.info("download over.")
    logger.info("counter: %s", cpt)
    if len(all_speeds) > 0:
        logger.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )
    return cpt


def test_listing_content(listing_path) -> bool:
    """
    make sure that a listing of products to download respect the following format:
        cdse-hash-id,safename or s3-path,safename

    Arguments:
    ---------
        listing_path (str):
    Returns
    -------
        listing_OK (bool): True -> the listing is OK, False -> the listing is not OK
    """
    fid = open(listing_path)
    first_line = fid.readline()
    second_line = fid.readline()
    listing_OK = False
    if "," in second_line:
        if "SAFE" in second_line.split(",")[1] and "S" in second_line.split(",")[1][0]:
            listing_OK = True
    # check that there is a header in the listing.
    if "safename" in first_line.lower() and (
        "id" in first_line.lower() or "s3_path" in first_line.lower()
    ):
        listing_OK = True
    return listing_OK


def test_csv_content(csv_path) -> bool:
    """
    make sure the columns 'id' 'safename' 'S3Path' are present in the input dataframe csv
    """
    csv_ok = True
    df = pd.read_csv(csv_path, header=0)
    for colneeded in ["id", "safename", "S3Path"]:
        if colneeded not in df.columns:
            csv_ok = False
    return csv_ok


def add_missing_cdse_hash_ids_in_listing(
    listing_path, conf, display_tqdm=False, email=None, password=None
):
    """
    Add columns of CDSE product ID and S3 path in a listing of products to download based on the safenames. This is useful for instance for the private data IOC products since the CDSE Odata search does not return the hash id for those products but only the safename. The method is using the same query method as the one used in the CDSE Odata search script (opensearch_private_data_IOC.py) to retrieve the hash id associated to each safename.

    Args:
        listing_path (str):
        conf (dict): configuration of the lib cdsodatacli (used to know which unit is PRIVATE)
        display_tqdm (bool): True -> tqdm progress bar for each queries [optional, default=False]
        email (str): email of the CDSE account to use for queries [optional, default None -> use cdsodatacli default behavior]
        password (str): password of the CDSE account to use for queries [optional, default None -> use cdsodatacli default behavior]

    Returns:
        res (pd.DataFrame): dataframe with 3 columns "id", "safename", and "S3Path" containing the hash id provided by CDSE Odata and the safename of the product

    """
    res = pd.DataFrame({"id": [], "safename": [], "S3Path": []})
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
        # if list_safe_a[ii].startswith("S1D"):
        if list_safe_a[0:3] in conf["list_sar_unit_private_data"]:
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
            res = collected_data_norm[["Id", "Name", "S3Path"]]
            res.rename(columns={"Name": "safename"}, inplace=True)
            res.rename(columns={"Id": "id"}, inplace=True)
            # remove the /eodata/ at the begining of the S3Path.
            res["S3Path"] = res["S3Path"].apply(lambda x: x.replace("/eodata/", ""))
            assert (
                "S3Path" in res.columns
            ), "S3Path column is missing in the result, check the fetch_data method"
            assert (
                res["S3Path"].iloc[0].startswith("Sentinel-1/")
            ), "S3Path column does not contain expected values, check the fetch_data method"

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
    logger.info("product downloadable: %s", len(df_products_downloadable))
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
        logger.info("start download : %s", safename_product)
        # date_generation_access_token = datetime.datetime.strptime(
        #     os.path.basename(path_semaphore_token).split("_")[4].replace(".txt", ""),
        #     "%Y%m%dt%H%M%S",
        # )
        access_token, date_generation_access_token = get_valid_access_token(login)
        logger.debug("url_product : %s", url_product)
        if access_token is None:

            logger.info("get a new access token")
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
            logger.debug("reuse same access token, still valid.")
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
        #     logger.error(
        #         "impossible to fetch %s from CDS: %s",
        #         url_product,
        #         traceback.format_exc(),
        #     )
    logger.info("download over.")
    logger.info("counter: %s", cpt)
    if len(all_speeds) > 0:
        logger.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )
    return df_products_downloadable


def download_list_product_multithread_v3(
    inputdf,
    outputdir,
    account_group,
    hideprogressbar=False,
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
    inputdf (pd.DataFrame): DataFrame containing product information with columns 'id' and 'safename'
    outputdir (str): the directory where to store the product collected
    account_group (str): a group define in the config file with a unique account -> 4 sessions in parallel
    hideprogressbar (bool): True -> no tqdm progress bar in stdout
    check_on_disk (bool): True -> if the product is in the spool dir or in archive dir the download is skipped
    cdsodatacli_conf_file (str): path to the cdsodatacli configuration file [ optional, default is None -> use cdsodatacli default behavior]

    Returns
    -------
        df2 (pd.DataFrame):
    """
    warnings.warn(
        "download_list_product_multithread_v3() is deprecated and will be removed in a future version. please use download_list_product_multithread_v4",
        DeprecationWarning,
    )
    assert len(inputdf["id"]) == len(inputdf["safename"])
    if "status" not in inputdf.columns:
        inputdf["status"] = np.zeros(len(inputdf["safename"]))
    logger.info("check_on_disk : %s", check_on_disk)
    cpt = defaultdict(int)
    cpt["products_in_initial_listing"] = len(inputdf["id"])
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    if hideprogressbar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    # status, 0->not treated, -1->error download , 1-> successful download

    force_download = not check_on_disk
    df2, cpt = filter_product_already_present(
        cpt,
        inputdf.rename(columns={"safename": "safe"}),
        outputdir,
        force_download=force_download,
        cdsodatacli_conf=conf,
    )
    t_start_download = time.time()
    logger.info("%s", cpt)
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
                logger.info(
                    "All the products have been treated (success or error).Nothing to do, exiting loop"
                )
                break
            # get the 4 download session information that can be submit in //
            df_prod_downloadable = get_sessions_download_available(
                conf,
                subset_to_treat,
                blacklist=blacklist,
                logins_group=account_group,
            )
            urls_index = list(df_prod_downloadable.index)
            logger.debug(
                "while_loop : %s, prod. to treat: %s, %s",
                while_loop,
                len(subset_to_treat),
                cpt,
            )
            # if len(df_prod_downloadable) == 0:
            if len(df_prod_downloadable) == 0:
                logger.debug("no session available wait a bit")
                time.sleep(5)  # no session available wait a bit
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
                    logger.debug("skipping %s already being downloaded", safename_base)
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

                    logger.error(
                        "Unhandled exception for %s: %s",
                        safename_base,
                        traceback.format_exc(),
                    )
                    df2.loc[(df2["safe"] == safename_base), "status"] = -1
                    pbar.update(1)
                    continue

                logger.debug("remove session semaphore for %s", login_used)
                remove_semaphore_session_file(
                    session_dir=conf["active_session_directory"],
                    safename=safename_base,
                    login=login_used,
                )

                # except KeyboardInterrupt:
                #     cpt["interrupted"] += 1
                #     raise ("keyboard interrupt")
                # except:
                #     logger.error("traceback : %s", traceback.format_exc())
                #     speed = np.nan
                #     status_meaning = "DownloadError"
                if status_meaning == "OK":
                    df2.loc[(df2["safe"] == safename_base), "status"] = 1
                    all_speeds.append(speed)
                    cpt["successful_download"] += 1
                else:
                    df2.loc[(df2["safe"] == safename_base), "status"] = -1
                    errors_per_account[login_used] += 1
                    logger.info(
                        "error found for %s reason: %s", login_used, status_meaning
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
                    logger.info("%s black listed for next loops", acco)
    elapsed_time = time.time() - t_start_download
    logger.info("download over in %f seconds", elapsed_time)
    logger.info("counter: %s", cpt)
    logger.info("maximum parallelism reached : %i", max_parallel_download)
    # safety remove active session, all reamining because of error
    remove_semaphore_session_file(
        session_dir=conf["active_session_directory"],
        safename=None,
        login=None,
    )

    if len(all_speeds) > 0:
        logger.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )
    return df2


def format_pbar_description(
    while_loop: int,
    cpt: dict,
    subset_to_treat,
    running_futures: set,
    active_s3_sessions_status: dict,
    speed_window: list,
    time_window: list,
    df2,
) -> str:
    """
    Build the tqdm progress bar description string for download_list_product_multithread_v4.

    Parameters
    ----------
    while_loop          : current iteration count
    cpt                 : counter defaultdict
    subset_to_treat     : df2 rows with status==0
    running_futures     : set of in-flight futures
    active_s3_sessions_status : {login: {session_id: bool}}
    speed_window        : rolling list of recent download speeds (Mo/s)
    time_window         : rolling list of recent elapsed times (s)
    df2                 : full status dataframe (for ETA n_remaining)

    Returns
    -------
    str : formatted description ready for pbar.set_description()
    """
    # --- session dots (your original format) ---
    session_parts = []
    for login, sessions in active_s3_sessions_status.items():
        short = login.split("@")[0][:8]  # "antoine" from full email
        dots = "".join("●" if sessions[sid] else "○" for sid in sorted(sessions))
        session_parts.append(f"{short}:[{dots}]")
    session_view = " ".join(session_parts)

    # --- error count (exclude success keys) ---
    err_count = sum(
        v
        for k, v in cpt.items()
        if k.startswith("status_") and k not in ("status_OK", "status_Downloaded")
    )

    # --- speed / ETA ---
    if speed_window:
        avg_sp = np.mean(speed_window)
        min_sp = np.min(speed_window)
        max_sp = np.max(speed_window)
        avg_t = np.mean(time_window)
        n_remaining = len(df2[df2["status"].isin([0, 2])])
        n_parallel = max(len(running_futures), 1)
        eta_sec = int(avg_t * n_remaining / n_parallel)
        if eta_sec >= 3600:
            eta_str = f"{eta_sec // 3600}h{(eta_sec % 3600) // 60:02d}m"
        else:
            eta_str = f"{eta_sec // 60}m{eta_sec % 60:02d}s"
        perf_str = (
            f"spd avg={avg_sp:.1f} min={min_sp:.1f} max={max_sp:.1f} Mo/s"
            f" | ETA≈{eta_str}"
        )
    else:
        perf_str = "warming up…"

    return (
        f"loop={while_loop}"
        f" | OK={cpt['successful_download']}"
        f" | ERR={err_count}"
        f" | todo={len(subset_to_treat)}"
        f" | //={len(running_futures)}"
        f" | {session_view}"
        f" | {perf_str}"
    )


def process_completed_futures(
    done,
    future_to_info,
    df2,
    pbar,
    all_speeds,
    all_elapsed_time,
    all_total_mb,
    cpt,
    errors_per_account,
    blacklist,
    active_s3_sessions_status,
):
    # 3) Handle completed downloads
    for future in done:
        info = future_to_info.pop(future, {})
        safename_base = info.get("safename", "unknown")
        login_used = info.get("login", "unknown")
        s3_session_id_used = info.get("s3_session_id", "unknown")
        # release the session+account
        active_s3_sessions_status = release_s3_session_after_usage(
            active_s3_sessions_status, login=login_used, session_id=s3_session_id_used
        )
        try:
            # process result
            (
                speed,
                elapsed_time,
                total_mb,
                status_meaning,
                safename_base,
            ) = future.result()
        except Exception:

            logger.error(
                "Unhandled exception for %s: %s",
                safename_base,
                traceback.format_exc(),
            )
            df2.loc[(df2["safe"] == safename_base), "status"] = -1
            pbar.update(1)
            continue
        # no more semaphore session file on disk, everything is in memory with thread lock
        # it means that a given group of account cannot be used in multipe process at the same time
        # logger.debug("remove session semaphore for %s", login_used)
        # remove_semaphore_session_file(
        #     session_dir=conf["active_session_directory"],
        #     safename=safename_base,
        #     login=login_used,
        # )

        # except KeyboardInterrupt:
        #     cpt["interrupted"] += 1
        #     raise ("keyboard interrupt")
        # except:
        #     logger.error("traceback : %s", traceback.format_exc())
        #     speed = np.nan
        #     status_meaning = "DownloadError"
        if status_meaning == "OK" or status_meaning == "Downloaded":
            df2.loc[(df2["safe"] == safename_base), "status"] = 1
            all_speeds.append(speed)
            all_elapsed_time.append(elapsed_time)
            all_total_mb.append(total_mb)
            cpt["successful_download"] += 1
        else:
            df2.loc[(df2["safe"] == safename_base), "status"] = -1
            errors_per_account[login_used] += 1
            logger.info("error found for %s meaning %s", login_used, status_meaning)
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
            logger.info("%s black listed for next loops", acco)
    return (
        done,
        future_to_info,
        df2,
        pbar,
        all_speeds,
        all_elapsed_time,
        all_total_mb,
        cpt,
        errors_per_account,
        blacklist,
        active_s3_sessions_status,
    )


def download_list_product_multithread_v4(
    inputdf,
    outputdir,
    account_group,
    hideprogressbar=False,
    check_on_disk=True,
    cdsodatacli_conf_file=None,
):
    """
    v4 is working as deamon like v3 (while loop) multi account round-robin
      and token semaphore files but using S3 endpoint to download each product
    In this method is working for a group of account with one or many account.
    Each account can run 4 parallel sessions.
    step 1: filter the dataframe containing the raw list of products to download -> remove duplicate and remove products already downloaded
    step 2: create multiple threads to download in parallel (depends on number of account and sessions per account)
    step 3: loop until all the products are treated
    step 3.1: get an account (i.e. S3 credentials) for which one session is free/available for download
    step 3.2: submit future downloads up to the current limit of available sessions
    step 3.3: wait for the first download thread/session to be finished
    step 3.4: clean lock on the session to free the session
    step 4: security lock cleaning (to avoid any orphan busy sessions at the end of the process)
    step 5: print out the download speed and elapsed times.



    Parameters
    ----------
    inputdf (pd.DataFrame): DataFrame containing the products to download with columns "S3Path", "id", and "safename"
    outputdir (str): the directory where to store the product collected
    account_group (str): a group define in the config file with a unique account -> 4 sessions in parallel
    hideprogressbar (bool): True -> no tqdm progress bar in stdout
    check_on_disk (bool): True -> if the product is in the spool dir or in archive dir the download is skipped
    cdsodatacli_conf_file (str): path to the cdsodatacli configuration file [ optional, default is None -> use cdsodatacli default behavior]

    Returns
    -------
        df2 (pd.DataFrame):
    """
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    # initialize a dict to report for the status of s3 sessions involved in the download
    # note: it suppose a given group of logins is not used in multiple python-process at the same time
    active_s3_sessions_status = {}
    for account in conf[account_group]:
        if isinstance(account, str):
            account_tmp = account
        elif isinstance(account, dict):
            account_tmp = list(account)[0]
        else:
            raise ValueError(
                f"Unexpected format for account {account} in group {account_group}"
            )
        active_s3_sessions_status[account_tmp] = {
            key: False for key in range(MAX_SESSION_PER_ACCOUNT)
        }
    assert len(inputdf["S3Path"]) == len(inputdf["safename"])
    if "status" not in inputdf.columns:
        inputdf["status"] = np.zeros(len(inputdf["safename"]))
    logger.info("check_on_disk : %s", check_on_disk)
    cpt = defaultdict(int)
    cpt["products_in_initial_listing"] = len(inputdf["S3Path"])

    if hideprogressbar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    all_elapsed_time = []
    all_total_mb = []
    # Rolling window for speed/ETA — last 10 completed downloads
    _SPEED_WINDOW = 10
    _speed_window: list[float] = []  # Mo/s per completed product
    _time_window: list[float] = []  # elapsed seconds per completed product
    # status, 0->not treated, -1->error download , 1-> successful download
    # df = pd.DataFrame(
    #     {"safe": list_safename, "status": np.zeros(len(list_safename)), "s3path": list_s3path}
    # )
    force_download = not check_on_disk
    df2, cpt = filter_product_already_present(
        cpt,
        inputdf.rename(columns={"safename": "safe"}),
        outputdir,
        force_download=force_download,
        cdsodatacli_conf=conf,
        extension="",
    )
    t_start_download = time.time()
    logger.info("%s", cpt)
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

        # while (df2["status"].isin([0, 2])).any(): # while there are untreated or in-flight products
        while (
            running_futures or (df2["status"] == 0).any()
        ):  # while there are untreated or in-flight products

            while_loop += 1
            subset_to_treat = df2[df2["status"] == 0]
            # pbar.set_description(
            #     f"loop={while_loop} | OK={cpt['successful_download']} | ERR={sum(v for k,v in cpt.items() if k.startswith('status_') and k != 'status_OK' and k != 'status_Downloaded')} | todo={len(subset_to_treat)} | //={len(running_futures)}"
            # )
            pbar.set_description(
                format_pbar_description(
                    while_loop,
                    cpt,
                    subset_to_treat,
                    running_futures,
                    active_s3_sessions_status,
                    _speed_window,
                    _time_window,
                    df2,
                )
            )

            if len(subset_to_treat) > 0:  # submit part
                df_products_ready_for_download, active_s3_sessions_status = (
                    get_sessions_download_available_s3(
                        conf=conf,
                        active_s3_sessions_status=active_s3_sessions_status,
                        subset_to_treat=subset_to_treat,
                        blacklist=blacklist,
                        logins_group=account_group,
                    )
                )

                urls_index = list(df_products_ready_for_download.index)
                logger.debug(
                    "while_loop : %s, prod. to treat: %s, %s",
                    while_loop,
                    len(subset_to_treat),
                    cpt,
                )
                # if len(df_prod_downloadable) == 0:
                if len(df_products_ready_for_download) == 0:
                    logger.debug("no session available wait a bit")
                    time.sleep(5)
                    continue
                errors_per_account = defaultdict(int)

                # currently_downloading = set(
                #     info["safename"] for info in future_to_info.values()
                # )
                # 1) Submit as many futures as possible
                while urls_index:  # and len(running_futures) < max_parallelism_seek:
                    # what constraint the number of submited download is not the max_parallelism but the number of
                    # actual product ready for download depending on number of free S3 sessions
                    # while urls_index and len(running_futures) < len(
                    #     df_products_ready_for_download["safe"]
                    # ): -> cause last product to be not submited
                    url_one_index = urls_index.pop(0)

                    safename_base = df_products_ready_for_download["safe"].loc[
                        url_one_index
                    ]
                    logintobeused = df_products_ready_for_download["login"].loc[
                        url_one_index
                    ]
                    s3_session_id = df_products_ready_for_download["s3_session"].loc[
                        url_one_index
                    ]  # 0 1 2 or 3
                    assert isinstance(safename_base, str)
                    # if safename_base in currently_downloading:
                    # logger.debug("skipping %s already being downloaded", safename_base)
                    # active_s3_sessions_status = release_s3_session_after_usage(
                    #     active_s3_sessions_status,
                    #     login=logintobeused,
                    #     session_id=s3_session_id,
                    # )
                    # continue

                    # header = df_prod_downloadable["header"].loc[url_one_index]
                    output_path = df_products_ready_for_download["output_path"].loc[
                        url_one_index
                    ]
                    s3path = df_products_ready_for_download["S3Path"].loc[url_one_index]
                    s3_credentials = {
                        "s3-access-key": df_products_ready_for_download[
                            "s3_access_key"
                        ].loc[url_one_index],
                        "s3-secret": df_products_ready_for_download["s3_secret"].loc[
                            url_one_index
                        ],
                    }
                    future = executor.submit(
                        cds_s3_download_one_product,
                        s3path,
                        s3_credentials,
                        output_path,
                        conf=conf,
                    )
                    # update df2 status column to reflect the product that are being currently downloaded
                    df2.loc[df2["safe"] == safename_base, "status"] = 2  # in flight
                    future_to_info[future] = {
                        "safename": safename_base,
                        "login": logintobeused,
                        "s3_session_id": s3_session_id,
                        # "semaphore_token_file": path_semaphore_token,
                    }
                    # currently_downloading.add(safename_base)  # mettre à jour immédiatement
                    # retries[safename_base] += 1
                    running_futures.add(future)
                # small check to know what is the maximum download parallelism we can reach
                # if len(running_futures) > max_parallel_download:
                #     max_parallel_download = len(running_futures)
                # 2) Wait for at least one download to finish

            # at each loop wait for at least one download to finish and process completed (can have multiple at the same time)
            done, running_futures = wait(
                running_futures, timeout=None, return_when=FIRST_COMPLETED
            )

            # process completed futures here
            (
                done,
                future_to_info,
                df2,
                pbar,
                all_speeds,
                all_elapsed_time,
                all_total_mb,
                cpt,
                errors_per_account,
                blacklist,
                active_s3_sessions_status,
            ) = process_completed_futures(
                done,
                future_to_info,
                df2,
                pbar,
                all_speeds,
                all_elapsed_time,
                all_total_mb,
                cpt,
                errors_per_account,
                blacklist,
                active_s3_sessions_status,
            )
    elapsed_time = time.time() - t_start_download
    logger.info("download over in %f seconds", elapsed_time)
    logger.info("counter: %s", cpt)
    logger.info("maximum parallelism reached : %i", max_parallel_download)
    # safety remove active session, all reamining because of error
    # remove_semaphore_session_file(
    #     session_dir=conf["active_session_directory"],
    #     safename=None,
    #     login=None,
    # )

    if len(all_speeds) > 0:
        logger.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )
    if len(all_elapsed_time) > 0:
        logger.info(
            "average elapsed time %1.1f s (stdev: %1.1f s)",
            np.mean(all_elapsed_time),
            np.std(all_elapsed_time),
        )
    if len(all_total_mb) > 0:
        logger.info(
            "cumulated size %1.1f Go (average: %1.1f Go)",
            np.sum(all_total_mb) / 1024,
            np.mean(all_total_mb) / 1024,
        )

    # check that all sessions are inactive at the end of download
    for login in active_s3_sessions_status:
        for session_id in active_s3_sessions_status[login]:
            if active_s3_sessions_status[login][session_id] is True:
                logger.warning(
                    "session %s #%s still marked active at end of download — releasing",
                    login,
                    session_id,
                )
                active_s3_sessions_status[login][session_id] = False
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
        logger.basicConfig(
            level=logging.DEBUG, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    else:
        logger.basicConfig(
            level=logging.INFO, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    t0 = time.time()
    # inputs = open(args.listing).readlines()
    inputdf = pd.read_csv(args.listing, names=["id", "safename"], delimiter=",")
    if not os.path.exists(args.outputdir):
        logger.debug("mkdir on %s", args.outputdir)
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
    logger.info("end of function in %s seconds", elapsed)
