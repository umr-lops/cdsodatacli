import pandas as pd
from collections import defaultdict
import logging
from cdsodatacli.download import filter_product_already_present
from cdsodatacli.utils import get_conf


def entrypoint(outputdir, cdsodatacli_conf_file, list_safename):
    cpt = defaultdict(int)
    cpt["products_in_initial_listing"] = len(list_safename)
    conf = get_conf(path_config_file=cdsodatacli_conf_file)
    # df = pd.DataFrame(
    #     {"safe": list_safename, "status": np.zeros(len(list_safename))}
    # )
    df = pd.read_csv(list_safename, header=None, names=["safe"])
    logging.debug("Initial DataFrame:\n%s", df)
    f2, cpt = filter_product_already_present(
        cpt, df, outputdir, force_download=False, cdsodatacli_conf=conf
    )
    for key in cpt:
        logging.info("Number of %s products: %d", key, cpt[key])
    return f2, cpt


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
    logging.basicConfig(
        level=getattr(logging, parser.parse_args().loglevel.upper(), None),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    args = parser.parse_args()
    entrypoint(args.outputdir, args.cdsodatacli_conf_file, args.list_safename)
