import pandas as pd
from collections import defaultdict
import logging
import os
from cdsodatacli.download import filter_product_already_present
from cdsodatacli.utils import get_conf


def entrypoint(outputdir, cdsodatacli_conf_file, list_safename,
     write_listing_2_download=None, dev=None):
    """
    Count present and absent products in a given output directory.
     
    Arguments:
        - outputdir: str, the directory to check for existing products.
        - cdsodatacli_conf_file: str, path to the cdsodatacli configuration file.
        - list_safename: str, path of a Listing containing safe names to check.
        - write_listing_2_download: str, optional, path of the listing of products to download. If not set, the listing will not be created.
        - dev: int, optional, number of products to check for development purposes. Default is 5.
    
    Returns:
        - df2download: DataFrame, listing of products to download.
        - cpt: dict, counts of present and absent products.

    """
    cpt = defaultdict(int)
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    # df = pd.DataFrame(
    #     {"safe": list_safename, "status": np.zeros(len(list_safename))}
    # )
    df = pd.read_csv(list_safename, header=None, names=["safe"])
    if dev is not None:
        logging.info("Development mode enabled: only checking the first %d products.", dev)
        df = df.head(dev)  # Only check the first X products in dev mode
    logging.debug("Initial DataFrame:\n%s", df)
    cpt["products_in_initial_listing"] = len(df)
    df2download, cpt = filter_product_already_present(
        cpt, df, outputdir, force_download=False, cdsodatacli_conf=conf
    )
    for key in cpt:
        logging.info("Number of %s products: %d", key, cpt[key])
    if write_listing_2_download and "preproc-product_absent_from_local_disks" in cpt and cpt["preproc-product_absent_from_local_disks"] > 0:
        # get id and safe name of products to download
        output_columns = ['safe']
        if 'urls' in df2download.columns:
            df2download['id'] = [uu.replace(conf["URL_download"], "") for uu in df2download['urls']]
            output_columns.append('id')
        df2download['safe'] = df2download['outputpath'].apply(lambda x: os.path.basename(x).replace(".zip", ""))
        df2download[output_columns].to_csv(write_listing_2_download, index=False,header=False)
        logging.info("Listing of products to download written to %s", write_listing_2_download)
    return df2download, cpt


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Count present and absent products in a given output directory."
    )
    parser.add_argument(
        "--outputdir",
        type=str,
        required=True,
        help="Output directory to check for existing products.",
    )
    parser.add_argument(
        "--cdsodatacli_conf_file",
        type=str,
        required=True,
        help="Path to the cdsodatacli configuration file.",
    )
    parser.add_argument(
        "--list_safename",
        type=str,
        required=True,
        help="path of a Listing containing safe names to check.",
    )
    parser.add_argument(
        "--loglevel",
        type=str,
        default="INFO",
        help="Logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        '--create-listing-2-download',
        action='store',
        help='Path of the listing of products to download.[ optional, if not set, the listing will not be created]',
        default=None
    )
    parser.add_argument(
        '--dev',
        action='store',
        type=int,
        help='Reduce listing size to a given number of products to check for development purposes. [optional, default is full listing]',
        default=None
    )
    logging.basicConfig(
        level=getattr(logging, parser.parse_args().loglevel.upper(), None),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    args = parser.parse_args()
    entrypoint(args.outputdir, args.cdsodatacli_conf_file, args.list_safename,
               write_listing_2_download=args.create_listing_2_download,
               dev=args.dev)
