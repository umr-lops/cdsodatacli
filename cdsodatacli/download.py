import pdb

import requests
import logging
from tqdm import tqdm
import datetime
import time
import os

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

    """
    t0 = time.time()
    with open(output_filepath, "wb") as f:
        logging.info("Downloading %s" % output_filepath)
        # response = requests.get(url, stream=True)
        response = session.get(url, headers=headers, stream=True)
        total_length = int(int(response.headers.get("content-length")) / 1000 / 1000)
        logging.debug("total_length : %s Mo", total_length)
        if total_length is None:  # no content length header
            f.write(response.content)
        else:
            dl = 0
            # total_length = int(total_length)
            # pbar = tqdm(range(total_length))
            with tqdm(total=total_length) as progress_bar:
                for data in tqdm(response.iter_content(chunk_size=chunksize)):
                    dl += len(data)
                    f.write(data)
                    progress_bar.update(chunksize / 1000.0 / 1000.0)  # update progress
                # done = int(50 * dl / total_length)
                # sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)) )
                # sys.stdout.flush()
    elapsed_time = time.time() - t0
    logging.info("time to download this product: %1.1f sec", elapsed_time)
    speed = total_length / elapsed_time
    logging.info("average download speed: %1.1fMo/sec", speed)
    return speed


def download_list_product(list_id, list_safename, outputdir):
    """

    Parameters
    ----------
    list_id (list) of string could be hash (eg a1e74573-aa77-55d6-a08d-7b6612761819) provided by CDS Odata or basename of SAFE product (eg. S1A_IW_GRDH_1SDV_20221013T065030_20221013T0650...SAFE)

    Returns
    -------

    """
    assert len(list_id) == len(list_safename)
    cpt = defaultdict(int)
    cpt["total_product_to_download"] = len(list_id)
    access_token, date_generation_access_token = get_bearer_access_token()
    headers = {"Authorization": "Bearer %s" % access_token}
    logging.debug("headers: %s", headers)
    session = requests.Session()
    session.headers.update(headers)
    for ii in tqdm(range(len(list_id))):
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
                access_token, date_generation_access_token = get_bearer_access_token()
                session.headers.update(headers)
            else:
                logging.debug("reuse same access token, still valid.")
            output_filepath = os.path.join(outputdir, safename_product)
            try:
                CDS_Odata_download_one_product(
                    session, headers, url=url_product, output_filepath=output_filepath
                )
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


def main():
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="download-from-CDS")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="overwrite the existing outputs [default=False]",
        required=False,
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
    )
    logging.info("end of function")
