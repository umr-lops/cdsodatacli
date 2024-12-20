"""
A Grouazel
Nov 2023
We need a script to know the list of product available and the list of product that needs to be download (including the one that are in spool)
steps:
1) fetch listing of data for a given period from CDSE
2) filter out product already at ifremer
3) split the listing into single files in scratch specific directory

"""
import argparse
import time
import logging
import os
import datetime
import shapely
import geopandas as gpd
from tqdm import tqdm

NOW_STR = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
import collections
#from shared_information import TYPES, WORKING_DIR, sats_acro, DIR_SATWWAVE_SCRATCH
from s1ifr.shared_information import TYPES, WORKING_DIR, sats_acro, DIR_SATWWAVE_SCRATCH
import cdsodatacli
from cdsodatacli.utils import check_safe_in_spool, check_safe_in_archive

DIR_MISSING_PRODUCT = os.path.join(DIR_SATWWAVE_SCRATCH, "to_download_S1_product")


def get_missing_WV_safe(
    startDate,
    stopDate,
    sarUnit="*",
    mode="WV",
    producttype="SLC",
    polarization="1SSV",
    cachedir=None,
):
    """
    list available SAFE on CDSE and compare with existing product at Ifremer
    :param
        startDate datetime
        stopDate datetime
        sarUnit  str S1A for instance
        mode str IW for instance
        producttype str SLC for instance
        polarization str 1SSV for instance
        cachedir str [optional, default=None]
    :return:
    """
    safenames = []
    ids_hash = []
    lonmin = -180
    lonmax = 180
    latmin = -88
    latmax = 88

    poly = shapely.geometry.Polygon(
        [
            (lonmin, latmin),
            (lonmax, latmin),
            (lonmax, latmax),
            (lonmin, latmax),
            (lonmin, latmin),
        ]
    )
    poly = shapely.wkt.loads("POLYGON ((-180 85, 180 85, 180 -85, -180 -85, -180 85))")
    pattern_name = None
    if sarUnit != "*":
        pattern_name = sarUnit
    if polarization is None:
        pass
    else:
        if pattern_name is None:
            pattern_name = polarization
        else:
            pattern_name = pattern_name+'*'+polarization

    gdf = gpd.GeoDataFrame(
        {
            "start_datetime": [startDate],
            "end_datetime": [stopDate],
            "end_datetime": [stopDate],
            "geometry": [poly],
            "collection": ["SENTINEL-1"],
            "name": [pattern_name],
            "sensormode": [mode],
            "producttype": [producttype],
            "Attributes": [None],
        }
    )
    logging.info("cache_dir : %s", cachedir)
    # mode = 'multi'
    collected_data_norm = cdsodatacli.query.fetch_data(
        gdf, min_sea_percent=0, cache_dir=cachedir
    )
    if collected_data_norm is not None:
        safenames = collected_data_norm["Name"].values
        ids_hash = collected_data_norm['Id'].values
    else:
        logging.info("no data found")
    return safenames,ids_hash


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="fetch_list_products")
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
        "--startdate",
        required=True,
        help="start date for fetch YYYYMMDD",
    )
    parser.add_argument(
        "--stopdate",
        required=True,
        help="stopd date for fetch YYYYMMDD",
    )
    parser.add_argument(
        "--polarization",
        required=False,
        default=None,
        choices=["SDV", "SDH", "SSH", "SSV"],
        help="polarisation SDV for instance, default=None (ie any pola)",
    )
    parser.add_argument(
        "--cachedir",
        required=False,
        default=None,
        help="cache directory for CDSE queries [default=None]",
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
    # if args.mode:
    #     modes_sar = [args.mode]
    # else:
    # modes_sar = choice_mode
    # for mode_treated in modes_sar:
    cpt = collections.defaultdict(int)
    logging.info("1/3 scan CDS for mode: %s", args.mode)
    sta = datetime.datetime.strptime(args.startdate, "%Y%m%d")
    sto = datetime.datetime.strptime(args.stopdate, "%Y%m%d")
    list_safe_available,ids_hash = get_missing_WV_safe(
        startDate=sta,
        stopDate=sto,
        mode=args.mode,
        producttype=args.format[0:3],
        polarization=args.polarization,
        cachedir=None,
        sarUnit=args.satellite,
    )
    logging.info("number of SAFE available: %s", len(list_safe_available))
    safe_missing = []
    hash_missing = []
    for ssi in tqdm(range(len(list_safe_available))):
        safebn = list_safe_available[ssi]
        if check_safe_in_spool(safebn):
            cpt["in_spool"] += 1
        elif check_safe_in_archive(safebn):
            cpt["archived"] += 1
        else:
            cpt["missing"] += 1
            safe_missing.append(safebn)
            hash_missing.append(ids_hash[ssi])
    for ssi in tqdm(range(len(safe_missing))):
        safebn = safe_missing[ssi]
        path_product_ticket = os.path.join(DIR_MISSING_PRODUCT, safebn)
        if not os.path.exists(path_product_ticket):
            fid = open(path_product_ticket, "w")
            fid.write(hash_missing[ssi]+','+safebn)
            fid.close()
            cpt["ticket_product_created"] += 1
    cpt["total_product_avail"] = len(list_safe_available)
    logging.info("counters: %s", cpt)
    logging.info(
        "directory where the missing product ticket are stored: %s", DIR_MISSING_PRODUCT
    )
    elapsed = time.time() - start
    logging.info(
        "It takes %1.1d seconds to complete the sentinel1 data scan/comparison/download (notice that sorting data in spols is an independant step)",
        elapsed,
    )
