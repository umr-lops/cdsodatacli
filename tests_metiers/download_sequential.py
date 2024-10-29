# list_ids = ['aa877202-1479-4f06-b2d6-620ee959dc47',
#        'a7d833c4-6b92-4bf8-9f79-0b39add53e16']
# list_safe = ['S1A_WV_SLC__1SSV_20231110T201811_20231110T203308_051159_062BA3_954C.SAFE',
#        'S1A_WV_SLC__1SSV_20231110T234523_20231110T235358_051161_062BB4_B4D0.SAFE']
from cdsodatacli.download import main
import pandas as pd
import logging
import os
import cdsodatacli
from cdsodatacli.download import download_list_product, download_list_product_sequential
from cdsodatacli.utils import conf

# listing = './example_WV_listing.txt'
default_listing = os.path.join(
    os.path.dirname(os.path.dirname(cdsodatacli.__file__)),
    "tests_metiers",
    "example_WV_SLC_listing.txt",
)
if __name__ == "__main__":
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    import argparse
    import pandas as pd

    parser = argparse.ArgumentParser(description="highleveltest-fetch_OCN_WV_IDs")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--listing",
        default=default_listing,
        help="path of the listing of products to download containing (Id,safename) lines",
    )
    parser.add_argument(
        "--outputdir",
        required=True,
        help="pathwhere product will be stored",
    )
    parser.add_argument(
        "--version",type=int, choices=[1, 2], help="version of the sequential download method"
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
    listing = args.listing
    logging.info("listing: %s", listing)
    assert os.path.exists(listing)
    # listing = './example_WV_OCN_listing.txt'
    # outputdir = conf['test_default_output_directory']
    outputdir = args.outputdir
    inputdf = pd.read_csv(listing, names=["id", "safename"], delimiter=",")
    if not os.path.exists(outputdir):
        logging.debug("mkdir on %s", outputdir)
        os.makedirs(outputdir, 0o0775)

    if args.version == 2:
        dfout = download_list_product_sequential(
            list_id=inputdf["id"].values,
            list_safename=inputdf["safename"].values,
            outputdir=outputdir,
            hideProgressBar=False,
        )
    elif args.version == 1:
        specific_account = "Mickael.accensi@ifremer.fr"
        logging.info("specific_account : %s", specific_account)
        download_list_product(
            list_id=inputdf["id"].values,
            list_safename=inputdf["safename"].values,
            outputdir=outputdir,
            hideProgressBar=False,
            specific_account=specific_account,
        )
    logging.info("end of function")
