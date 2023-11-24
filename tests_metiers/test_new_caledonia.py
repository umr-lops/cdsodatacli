
import datetime
import shapely
import geopandas as gpd
import cdsodatacli
import logging
import time
if __name__ == '__main__':
    t0 = time.time()
    import argparse
    parser = argparse.ArgumentParser(description="testsnewrelease")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--cache", action="store_true", default=None,
                        help='cache directory to store answer from CDS as json files [default is None]')
    parser.add_argument('--mode',choices=['seq','multi'],
                        help='mode of query seq-> sequential multi-> multithread [default is sequential]',
                        default='seq',required=False)
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

    sta = datetime.datetime(2014,4,6)
    sta = datetime.datetime(2022,1,6)
    sta = datetime.datetime.today() - datetime.timedelta(days=365)
    sta = datetime.datetime(2023,1,1)
    # sta = datetime.datetime.today() - datetime.timedelta(days=7)
    sto = datetime.datetime.today()
    sto = datetime.datetime(2023,11,13)
    logging.info('from %s to %s',sta,sto)
    # 17° 52′ 46″ S 162° 36′ 31″ E
    # 22° 56′ 36″ S 168° 12′ 02″ E
    latmin = -17.87944444
    lonmin = 162.60861111
    latmax = -22.94333333
    lonmax = 168.20027778
    # poly = shapely.wkt.loads("POLYGON ((162 -19, 169 -19, 169 -23, 162 -23, 162 -19))")
    # cache_dir = '/home1/scratch/agrouaze/cache_cdsodata/'
    # cache_dir = None
    poly = shapely.geometry.Polygon([(lonmin,latmin),(lonmax,latmin),(lonmax,latmax),(lonmin,latmax),(lonmin,latmin)])
    gdf = gpd.GeoDataFrame({
            "start_datetime" : [ sta ],
            "end_datetime"   : [ sto],
            "geometry"   : [ poly],
            "collection"  : [ "SENTINEL-1"],
            "name"        : [ "1SDV"],
            "sensormode"  : [ 'IW'],
            "producttype" : [ 'SLC'],
            "Attributes"  : [ None],
        })
    logging.info('cache_dir : %s',args.cache)
    # mode = 'multi'
    collected_data_norm = cdsodatacli.query.fetch_data(gdf, min_sea_percent=30,cache_dir=args.cache,mode=args.mode)
    logging.info("%s", collected_data_norm)
    elapsed = time.time()-t0
    logging.info('over total time: %1.1f sec',elapsed)

