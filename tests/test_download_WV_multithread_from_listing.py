# list_ids = ['aa877202-1479-4f06-b2d6-620ee959dc47',
#        'a7d833c4-6b92-4bf8-9f79-0b39add53e16']
# list_safe = ['S1A_WV_SLC__1SSV_20231110T201811_20231110T203308_051159_062BA3_954C.SAFE',
#        'S1A_WV_SLC__1SSV_20231110T234523_20231110T235358_051161_062BB4_B4D0.SAFE']
import pandas as pd
import logging
import os
import pytest
import sys
import cdsodatacli
from cdsodatacli.download import download_list_product
from cdsodatacli.utils import conf
from dotenv import load_dotenv

load_dotenv()

# listing = './example_WV_listing.txt'
default_listing = os.path.join(
    os.path.dirname(os.path.dirname(cdsodatacli.__file__)),
    "scripts",
    "example_WV_OCN_listing.txt",
)

@pytest.fixture(scope="session")
def test_secrets():
    login_cdse = os.getenv("DEFAULT_LOGIN_CDSE")
    assert (
        login_cdse is not None
    ), "DEFAULT_LOGIN_CDSE is not defined (.env absent? or SECRETS from github undefined)"
    assert login_cdse == "cprevost@ifremer.fr"

@pytest.mark.skipif(sys.platform == "win32", reason="Test not supported on Windows")
@pytest.mark.parametrize(
    ("listing", "outputdir"),
    [
        (default_listing, conf["test_default_output_directory"]),
    ],
)
def test_download_WV_OCN_SAFE(listing, outputdir):
    if "./" in outputdir:
        outputdir = os.path.abspath(os.path.join(os.getcwd(), outputdir))
  

    login_cdse = os.getenv("DEFAULT_LOGIN_CDSE", None)
    passwd = os.getenv("DEFAULT_PASSWD_CDSE",None)
    print('login_cdse:',login_cdse)
      # for local test -> use the localconfig.yml/config.yml files
    if login_cdse is None or passwd is None:
        print('using cdsodatacli localconfig.yml/config.yml for login')
        default_login = conf.get("default_login", {})
        login_cdse, passwd = list(default_login.items())[0]
    logging.info("listing: %s", listing)
    assert os.path.exists(listing)
    inputdf = pd.read_csv(listing, names=["id", "safename"], delimiter=",")
    # maskok = inputdf["safename"].str.contains("CORRUPTED") == False
    maskok = ~inputdf["safename"].str.contains("CORRUPTED", na=False)
    inputdfclean = inputdf[maskok]
    assert len(inputdfclean["safename"]) == 3
    if not os.path.exists(outputdir):
        logging.debug("mkdir on %s", outputdir)
        os.makedirs(outputdir, 0o0775)
    # download_list_product_multithread_v2(
    #     list_id=inputdfclean["id"].values,
    #     list_safename=inputdfclean["safename"].values,
    #     outputdir=outputdir,
    #     hideProgressBar=False,
    #     account_group='defaultgroup'
    # )
    download_list_product(
        list_id=inputdfclean["id"].values,
        list_safename=inputdfclean["safename"].values,
        outputdir=outputdir,
        specific_account=login_cdse,
        specific_passwd=passwd,
        hideProgressBar=False,
    )

    # assert check_safe_in_outputdir(outputdir=outputdir,safename=inputdfclean['safename'].iloc[0]) is True
    # clear the test  download output directory
    # for ii in range(len(inputdfclean['safename'])):
    #     os.remove(os.path.join(outputdir,inputdfclean['safename'].iloc[ii]+'.zip'))
    # assert check_safe_in_outputdir(outputdir=outputdir, safename=inputdfclean['safename'].iloc[0]) is False
    assert True


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

    test_secrets()
    test_download_WV_OCN_SAFE(listing=args.listing, outputdir=args.outputdir)
    logging.info("end of function")
