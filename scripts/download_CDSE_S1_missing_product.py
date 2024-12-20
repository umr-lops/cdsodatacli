"""
Nov 2023
A. Grouazel
script to download Sentinel-1 product into the spool dir
steps:
1) read tickets created by get_CDS_S1_product_listing.py
2) check that the product is still missing
3a) yes -> download
3b) no -> do nothing
4) remove the ticket
"""

import logging
import os
import collections
import time
import glob
import pandas as pd
from tqdm import tqdm
from get_CDS_S1_product_listing import DIR_MISSING_PRODUCT
from cdsodatacli.utils import check_safe_in_spool, check_safe_in_archive
from cdsodatacli.download import download_list_product_multithread_v2
from s1ifr.shared_information import TYPES, sats_acro


def read_missing_product_tickets(mode=None, product=None, unit=None):
    """

    :param mode: str IW WV ...
    :param product: str SLC GRD OCN
    :param unit: S1A ...
    :return:
    """
    pattern_base = "S1*"
    if unit is not None:
        pattern_base = pattern_base.replace("S1", unit)
    logging.debug("pattern_base: %s", pattern_base)
    if mode is not None:
        pattern_base = pattern_base.replace("*", "_" + mode + "_*")
    logging.debug("pattern_base: %s", pattern_base)
    if product is not None:
        pattern_base = pattern_base.replace("*", product + "_*")
    logging.info("pattern : %s", pattern_base)
    lst_ticket = glob.glob(os.path.join(DIR_MISSING_PRODUCT, pattern_base))
    logging.info("Number of tickets to treat: %s", len(lst_ticket))
    to_download_safes = []
    to_download_ids = []
    for ll in tqdm(range(len(lst_ticket))):
        xticket = lst_ticket[ll]
        content = open(xticket, "r").readline()
        hashcode, safename = content.split(",")
        to_download_safes.append(safename)
        to_download_ids.append(hashcode)
    df_dl = pd.DataFrame({"safename": to_download_safes, "id": to_download_ids})
    return df_dl


def check_products_still_missing(df2dl):
    """

    :param df2dl:
    :return:
    """
    cpt = collections.defaultdict(int)
    safe_missing = []
    hash_missing = []
    for ssi in tqdm(range(len(df2dl))):
        safebn = df2dl["safename"].iloc[ssi]
        if check_safe_in_spool(safebn):
            cpt["in_spool"] += 1
            remove_product_ticket(safename=safebn)
        elif check_safe_in_archive(safebn):
            cpt["archived"] += 1
            remove_product_ticket(safename=safebn)
        else:
            cpt["missing"] += 1
            safe_missing.append(safebn)
            hash_missing.append(df2dl["id"].iloc[ssi])
    df2dl_consolidated = pd.DataFrame({"safename": safe_missing, "id": hash_missing})
    logging.info("counter:%s", cpt)
    return df2dl_consolidated


def remove_product_ticket(safename):
    """

    :param safename: (str) base
    :return:
    """
    path_ticket = os.path.join(DIR_MISSING_PRODUCT, safename)
    if os.path.exists(path_ticket):
        os.remove(path_ticket)


def trigger_download(df2dl_consolidated, outputdir, hideProgressBar=True):
    dfdlout = download_list_product_multithread_v2(
        list_id=df2dl_consolidated["id"].values,
        list_safename=df2dl_consolidated["safename"].values,
        outputdir=outputdir,
        hideProgressBar=hideProgressBar,
    )
    if len(dfdlout["safe"]) > 0:
        for uu in range(len(dfdlout)):
            safe_basename = dfdlout["safe"].iloc[uu]
            status_download = dfdlout["status"].iloc[uu]
            if status_download == 1:
                remove_product_ticket(safename=safe_basename)
    logging.info("end of download")


if __name__ == "__main__":
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    import argparse

    parser = argparse.ArgumentParser(description="download_CDSE_list_products")
    start = time.time()
    choice_mode = ["SM", "IW", "EW", "WV"]
    parser.add_argument(
        "-m",
        "--mode",
        action="store",
        choices=choice_mode,
        dest="mode",
        help=" %s [optional]" % choice_mode,
    )
    parser.add_argument(
        "-s",
        "--satellite",
        action="store",
        choices=sats_acro.keys(),
        required=True,
        help="S1A or S1B or ... ",
    )
    parser.add_argument(
        "-f",
        "--format",
        action="store",
        choices=TYPES,
        default=None,
        help=" %s [optional default= None]" % TYPES,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="verbose mode",
    )
    parser.add_argument(
        "-o",
        "--outputdir",
        required=True,
        help="where to store the S1 product to download (spool is used in operation)",
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
    if args.format is not None:
        producttype = args.format[0:3]
    else:
        producttype = None
    df2dl = read_missing_product_tickets(
        mode=args.mode, product=producttype, unit=args.satellite
    )
    df2dl_consolidated = check_products_still_missing(df2dl)
    logging.info("Number of product after consolidation: %s", len(df2dl_consolidated))
    trigger_download(df2dl_consolidated, outputdir=args.outputdir, hideProgressBar=True)
    elapsed = time.time() - t0
    logging.info(
        "It takes %1.1d seconds to complete the sentinel1 data download",
        elapsed,
    )
