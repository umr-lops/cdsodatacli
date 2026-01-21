#!/usr/bin/env python3
import argparse
import logging
from cdsodatacli.query import core_query_logged


def main():
    parser = argparse.ArgumentParser(
        description="Search S1C/S1D Private IOC using OData API with keyed arguments.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Defining keyed arguments
    parser.add_argument(
        "-t", "--type", required=True, help="Product type (e.g. WV_SLC__1S_PRIVATE)"
    )
    parser.add_argument(
        "-d", "--startdate", required=True, help="Start date (e.g. 2025-01-01T00:00:00)"
    )
    parser.add_argument(
        "-e", "--enddate", required=True, help="End date (e.g. 2025-01-31T23:59:59)"
    )
    parser.add_argument("-o", "--output", required=True, help="Output JSON file path")
    parser.add_argument(
        "-u", "--unit", required=True, help="Satellite Unit Identifier (C or D)"
    )
    parser.add_argument("-p", "--password", required=True, help="CDSE Password")
    parser.add_argument("-e", "--email", required=True, help="CDSE Account Email")
    parser.add_argument(
        "--limit", type=int, default=1000, help="Max records to return (default: 1000)"
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False, help="Enable verbose logging"
    )

    args = parser.parse_args()
    root = logging.getLogger()
    for handler in root.handlers:
        root.removeHandler(handler)
    fmt = "%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s"
    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    else:
        logging.basicConfig(
            level=logging.INFO, format=fmt, datefmt="%d/%m/%Y %H:%M:%S", force=True
        )
    core_query_logged(
        email=args.email,
        password=args.password,
        type=args.type,
        startdate=args.startdate,
        enddate=args.enddate,
        unit=args.unit,
        output=args.output,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
