import pandas as pd
import datetime
import shapely
import geopandas as gpd
import cdsodatacli
import logging
import time
import os
from cdsodatacli.utils import conf

if __name__ == "__main__":
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="highleveltest-fetch_S1_worldwide_IDs")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--startdate",
        default="20230201",
        help="startdate YYYYMMDD",
    )
    parser.add_argument(
        "--stopdate",
        default="20230202",
        help="stopdate YYYYMMDD",
    )
    parser.add_argument(
        "--mode",
        choices=["IW", "SM", "WV", "EW"],
        required=True,
        help="acquisition mode WV, IW, ...",
    )
    parser.add_argument(
        "--product",
        choices=["OCN", "SLC", "GRD"],
        required=True,
        help="acquisition mode OCN, SLC, GRD ...",
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
    pola = "2SSV"
    pola= None
    t0 = time.time()
    gdf = gpd.GeoDataFrame(
        {
            "start_datetime": [datetime.datetime.strptime(args.startdate, "%Y%m%d")],
            "end_datetime": [datetime.datetime.strptime(args.stopdate, "%Y%m%d")],
            "geometry": [
                shapely.wkt.loads(
                    "POLYGON ((-180 90, 180 90, 180 -90, -180 -90, -180 90))"
                )
            ],
            "collection": ["SENTINEL-1"],
            "name": [pola],
            "sensormode": [args.mode],
            "producttype": [args.product],
            "Attributes": [None],
        }
    )

    collected_data_norm = cdsodatacli.query.fetch_data(gdf, min_sea_percent=0)
    outf = os.path.join(
        conf["test_default_output_directory"],
        "test_%s_%s_%s_%s.txt" % (args.mode,args.product,args.startdate, args.stopdate),
    )
    collected_data_norm[["Id", "Name"]].to_csv(outf, header=False, index=False)
    logging.info("outf : %s", outf)
