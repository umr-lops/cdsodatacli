"""
this script is now deprecated since S3 backend download is using long term S3 credentials from config file

"""

import boto3
import requests
from cdsodatacli.fetch_access_token import get_bearer_access_token
from cdsodatacli.utils import get_conf
import logging


def _get_fresh_s3_client(conf, headers):
    """
    Create a S3 resources (boto3 client) and S3 temporary credentials.

    Args:
        conf (dict): Configuration dictionary containing S3 endpoint, region, and bucket information.
        headers (dict): Headers containing the Bearer token for authentication.


    Returns:
        tuple: A tuple containing the S3 temporary credentials and the boto3 S3 resource object.
    """
    creds_resp = requests.post(
        "https://s3-keys-manager.cloudferro.com/api/user/credentials", headers=headers
    )
    creds_resp.raise_for_status()
    s3_creds = creds_resp.json()
    s3_resources = boto3.resource(
        "s3",
        endpoint_url=conf.get("s3_endpoint", "https://eodata.dataspace.copernicus.eu"),
        aws_access_key_id=s3_creds["access_id"],
        aws_secret_access_key=s3_creds["secret"],
        region_name=conf.get("s3_region", "default"),
    )
    return s3_creds, s3_resources


if __name__ == "__main__":
    # Example usage
    conf = {
        "s3_endpoint": "https://eodata.dataspace.copernicus.eu",
        "s3_region": "default",
    }
    import argparse

    parser = argparse.ArgumentParser(
        description="Test S3 client with temporary access token"
    )
    parser.add_argument(
        "--account", required=False, help="CDSE account to use for token retrieval"
    )
    parser.add_argument(
        "--group",
        default="logins",
        help="Group in config file to select account from (default: 'logins')",
    )
    parser.add_argument(
        "--cdsodatacli_conf_file",
        required=True,
        help="path to the cdsodatacli configuration file .yml ",
    )
    parser.add_argument(
        "--verbose", action="store_true", default=False, help="Enable verbose logging"
    )
    args = parser.parse_args()

    fmt = "%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s"
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format=fmt,
        datefmt="%d/%m/%Y %H:%M:%S",
        force=True,
    )
    account = args.account

    conf = get_conf(path_config_file=args.cdsodatacli_conf_file)
    token, date_generation, login = get_bearer_access_token(
        conf, specific_account=account, specific_psswd=None, account_group=args.group
    )
    s3_creds = None
    headers = {"Authorization": f"Bearer {token}"}
    logging.info(
        "Successfully retrieved temporary access token for account %s", account
    )
    logging.debug(
        "Token details: %s",
        {"token": token, "date_generation": date_generation, "login": login},
    )
    s3_creds, s3_resources = _get_fresh_s3_client(conf, headers)
    print("Successfully created S3 client with temporary credentials.")
    print(s3_creds)
    print(s3_resources)
    if s3_creds is not None:
        # Delete the temporary S3 credentials
        delete_response = requests.delete(
            f"https://s3-keys-manager.cloudferro.com/api/user/credentials/access_id/{s3_creds['access_id']}",
            headers=headers,
        )
        if delete_response.status_code == 204:
            logging.info("Temporary S3 credentials deleted successfully.")
        else:
            logging.error(
                f"Failed to delete temporary S3 credentials. Status code: {delete_response.status_code}"
            )
