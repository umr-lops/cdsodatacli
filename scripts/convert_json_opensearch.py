from cdsodatacli.utils import convert_json_opensearch_query_to_listing_safe_4_dowload
import logging
import time
import argparse


def main():

    parser = argparse.ArgumentParser(description="json->txt")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--json", required=True, help="tiff file full path IW SLC")
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
    logging.info("json file: %s", args.json)
    convert_json_opensearch_query_to_listing_safe_4_dowload(json_path=args.json)
    logging.info("done in %1.3f min", (time.time() - t0) / 60.0)


if __name__ == "__main__":
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    main()
