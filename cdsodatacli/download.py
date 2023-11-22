import pdb
import requests
import logging
from tqdm import tqdm
import datetime
import time
import os
import shutil
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import traceback
from cdsodatacli.fetch_access_token import get_bearer_access_token
from cdsodatacli.utils import conf, test_safe_archive, test_safe_spool
from collections import defaultdict

chunksize = 4096
MAX_VALIDITY_ACCESS_TOKEN = 600  # sec (defined by CDS API)


def CDS_Odata_download_one_product(session, headers, url, output_filepath):
    """

    Parameters
    ----------
    session (request Obj)
    headers (dict)
    url (str)
    output_filepath (str) full path where to store fetch file

    Returns
    -------
        speed (float): download speed in Mo/second

    """
    t0 = time.time()
    with open(output_filepath, "wb") as f:
        logging.info("Downloading %s" % output_filepath)
        response = session.get(url, headers=headers, stream=True)
        total_length = int(int(response.headers.get("content-length")) / 1000 / 1000)
        logging.debug("total_length : %s Mo", total_length)
        if total_length is None:  # no content length header
            f.write(response.content)
        else:
            dl = 0
            with tqdm(
                total=total_length, disable=bool(os.environ.get("DISABLE_TQDM", False))
            ) as progress_bar:
                for data in tqdm(
                    response.iter_content(chunk_size=chunksize),
                    disable=bool(os.environ.get("DISABLE_TQDM", False)),
                ):
                    dl += len(data)
                    f.write(data)
                    progress_bar.update(chunksize / 1000.0 / 1000.0)  # update progress
    elapsed_time = time.time() - t0
    logging.info("time to download this product: %1.1f sec", elapsed_time)
    speed = total_length / elapsed_time
    logging.info("average download speed: %1.1fMo/sec", speed)
    return speed


def CDS_Odata_download_one_product_v2(session, headers, url, output_filepath):
    """
     v2 is without tqdm
    Parameters
    ----------
    session (request Obj)
    headers (dict)
    url (str)
    output_filepath (str) full path where to store fetch file

    Returns
    -------
        speed (float): download speed in Mo/second

    """
    speed = np.nan
    status_meaning = 'unknown_code'
    t0 = time.time()
    with open(output_filepath, "wb") as f:
        logging.debug("Downloading %s" % output_filepath)
        response = session.get(url, headers=headers, stream=True)
        status = response.status_code
        status_meaning = response.reason
        # if status==200:
        #     status_meaning = 'OK'
        #     speed = total_length / elapsed_time
        # elif status==202:
        #     status_meaning = 'Accepted'
        #     logging.debug('202 (Accepted): Indicates that a batch request has been accepted for processing, but that the processing has not been completed.')
        # elif status==204:
        #     logging.debug('204 (No Content): Indicates that a request has been received and processed successfully by a data service and that the response does not include a response body.')
        #     status_meaning = 'No Content'
        # elif status == 400:
        #     logging.debug('400 (Bad Request): Indicates that the payload, request headers, or request URI provided in a request are not correctly formatted according to the syntax rules defined in this document.')
        #     # status_meaning = 'Bad Request'
        #     status_meaning = 'Unknown query parameter(s).'
        # elif status == 404:
        #     # logging.debug("404 (Not Found): Indicates that a segment in the request URI's Resource Path does not map to an existing resource in the data service. A data service MAY<74> respond with a representation of an empty collection of entities if the request URI addressed a collection of entities.")
        #     # status_meaning = 'Not Found'
        #     status_meaning = 'Unknown collection.'
        # elif status == 405:
        #     logging.debug('405 (Method Not Allowed): Indicates that a request used an HTTP method not supported by the resource identified by the request URI, see Request Types (section 2.2.7).')
        #     status_meaning = 'Method Not Allowed'
        # elif status == 412:
        #     logging.debug('412 (Precondition Failed): Indicates that one or more of the conditions specified in the request headers evaluated to false. This response code is used to indicate an optimistic concurrency check failure, see If-Match (section 2.2.5.5) and If-None-Match (section 2.2.5.6).')
        #     status_meaning = 'Precondition Failed'
        # elif status == 500:
        #     logging.debug('500 (Internal Server Error): Indicates that a request being processed by a data service encountered an unexpected error during processing.')
        #     status_meaning = 'Internal Server Error'
        # else:
        #     status_meaning = 'unknown_code'
        #     logging.error('unkown API OData code status: %s',status)
        #     raise ValueError()
        if response.ok:
            total_length = int(int(response.headers.get("content-length")) / 1000 / 1000)
            logging.debug("total_length : %s Mo", total_length)

            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    if not response.ok:
        logging.debug('remove empty file %s',output_filepath)
        os.remove(output_filepath)
    elapsed_time = time.time() - t0
    if status == 200: # means OK download
        speed = total_length / elapsed_time
    logging.debug("time to download this product: %1.1f sec", elapsed_time)

    logging.debug("average download speed: %1.1fMo/sec", speed)
    return speed,status_meaning


def download_list_product_multithread(
    list_id, list_safename, outputdir, hideProgressBar=False
):
    """

    Parameters
    ----------
    list_id
    list_safename
    outputdir
    hideProgressBar

    Returns
    -------

    """
    assert len(list_id) == len(list_safename)
    cpt = defaultdict(int)
    cpt["total_product_to_download"] = len(list_id)
    access_token, date_generation_access_token = get_bearer_access_token(
        quiet=hideProgressBar
    )
    headers = {"Authorization": "Bearer %s" % access_token}
    logging.debug("headers: %s", headers)
    session = requests.Session()
    session.headers.update(headers)
    all_output_filepath = []
    all_urls_to_download = []
    if hideProgressBar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    for ii in range(len(list_id)):
        safename_product = list_safename[ii]
        if test_safe_archive(safename=safename_product):
            cpt["archived_product"] += 1
        elif test_safe_spool(safename=safename_product):
            cpt["in_spool_product"] += 1
        else:
            cpt["product_absent_from_local_disks"] += 1
            id_product = list_id[ii]
            url_product = conf["URL_download"] % id_product

            logging.debug("url_product : %s", url_product)
            logging.debug(
                "id_product : %s safename_product : %s", id_product, safename_product
            )

            output_filepath = os.path.join(outputdir, safename_product + ".zip")
            all_output_filepath.append(output_filepath)
            all_urls_to_download.append(url_product)
    max_workers = 4  # Number of concurrent connections limit = 4 in https://documentation.dataspace.copernicus.eu/Quotas.html
    with ThreadPoolExecutor(max_workers=max_workers) as executor, tqdm(
        total=len(all_urls_to_download)
    ) as pbar:
        future_to_url = {
            executor.submit(
                CDS_Odata_download_one_product_v2,
                session,
                headers,
                all_urls_to_download[jj],
                all_output_filepath[jj],
            ): (jj)
            for jj in range(len(all_urls_to_download))
        }
        for future in as_completed(future_to_url):
            speed,status_meaning = future.result()
            all_speeds.append(speed)
            pbar.update(1)

        logging.info("download over.")
        logging.info("counter: %s", cpt)
        if len(all_speeds) > 0:
            logging.info(
                "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
                np.mean(all_speeds),
                np.std(all_speeds),
            )


def download_list_product(list_id, list_safename, outputdir, hideProgressBar=False):
    """

    Parameters
    ----------
    list_id (list) of string could be hash (eg a1e74573-aa77-55d6-a08d-7b6612761819) provided by CDS Odata
    list_safename (list) of string basename of SAFE product (eg. S1A_IW_GRDH_1SDV_20221013T065030_20221013T0650...SAFE)
    outputdir (str) path where product will be stored
    hideProgressBar (bool): True -> no tqdm progress bar
    Returns
    -------

    """
    assert len(list_id) == len(list_safename)
    cpt = defaultdict(int)
    cpt["total_product_to_download"] = len(list_id)
    access_token, date_generation_access_token = get_bearer_access_token(
        quiet=hideProgressBar
    )
    headers = {"Authorization": "Bearer %s" % access_token}
    logging.debug("headers: %s", headers)
    session = requests.Session()
    session.headers.update(headers)
    if hideProgressBar:
        os.environ["DISABLE_TQDM"] = "True"
    all_speeds = []
    pbar = tqdm(range(len(list_id)), disable=bool(os.environ.get("DISABLE_TQDM", False)))
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
                "id_product : %s safename_product : %s", id_product, safename_product
            )
            if (
                datetime.datetime.today() - date_generation_access_token
            ).total_seconds() >= MAX_VALIDITY_ACCESS_TOKEN:
                logging.info("get a new access token")
                access_token, date_generation_access_token = get_bearer_access_token()
                headers = {"Authorization": "Bearer %s" % access_token}
                session.headers.update(headers)
            else:
                logging.debug("reuse same access token, still valid.")
            output_filepath = os.path.join(outputdir, safename_product + ".zip")
            try:
                speed,status_meaning = CDS_Odata_download_one_product_v2(
                    session, headers, url=url_product, output_filepath=output_filepath
                )
                if status_meaning=='OK':
                    all_speeds.append(speed)
                    cpt["successful_download"] += 1
                cpt['status_%s'%status_meaning] += 1
            except KeyboardInterrupt:
                cpt['interrupted'] += 1
                raise ("keyboard interrupt")
            except:
                cpt["download_KO"] += 1
                logging.error(
                    "impossible to fetch %s from CDS: %s",
                    url_product,
                    traceback.format_exc(),
                )
    logging.info("download over.")
    logging.info("counter: %s", cpt)
    if len(all_speeds) > 0:
        logging.info(
            "average download speed %1.1f Mo/s (stdev: %1.1f Mo/s)",
            np.mean(all_speeds),
            np.std(all_speeds),
        )


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
    )
    logging.info("end of function")
