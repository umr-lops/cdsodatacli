import pdb
import requests
import logging
from tqdm import tqdm
import datetime
import time
import os
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
    t0 = time.time()
    with open(output_filepath, "wb") as f:
        logging.info("Downloading %s" % output_filepath)
        response = session.get(url, headers=headers, stream=True)
        total_length = int(int(response.headers.get("content-length")) / 1000 / 1000)
        logging.debug("total_length : %s Mo", total_length)

        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    elapsed_time = time.time() - t0
    logging.info("time to download this product: %1.1f sec", elapsed_time)
    speed = total_length / elapsed_time
    logging.info("average download speed: %1.1fMo/sec", speed)
    return speed


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
            cpt["product_asbent_from_local_disks"] += 1
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
            speed = future.result()
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
    for ii in tqdm(
        range(len(list_id)), disable=bool(os.environ.get("DISABLE_TQDM", False))
    ):
        safename_product = list_safename[ii]
        if test_safe_archive(safename=safename_product):
            cpt["archived_product"] += 1
        elif test_safe_spool(safename=safename_product):
            cpt["in_spool_product"] += 1
        else:
            cpt["product_asbent_from_local_disks"] += 1
            id_product = list_id[ii]
            url_product = conf["URL_download"] % id_product
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
                speed = CDS_Odata_download_one_product_v2(
                    session, headers, url=url_product, output_filepath=output_filepath
                )
                all_speeds.append(speed)
                cpt["successful_download"] += 1
            except KeyboardInterrupt:
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
