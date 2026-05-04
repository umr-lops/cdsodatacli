import boto3
import requests
from cdsodatacli.fetch_access_token import get_bearer_access_token
from cdsodatacli.utils import get_conf
import logging
def _get_fresh_s3_client(conf, headers):
    """Always create a fresh boto3 client with new temporary credentials."""
    creds_resp = requests.post(
        "https://s3-keys-manager.cloudferro.com/api/user/credentials",
        headers=headers
    )
    creds_resp.raise_for_status()
    creds = creds_resp.json()
    return boto3.resource(
        "s3",
        endpoint_url=conf.get("s3_endpoint", "https://eodata.dataspace.copernicus.eu"),
        aws_access_key_id=creds["access_id"],
        aws_secret_access_key=creds["secret"],
        region_name=conf.get("s3_region", "default"),
    )


if __name__ == "__main__":
    # Example usage
    conf = {
        "s3_endpoint": "https://eodata.dataspace.copernicus.eu",
        "s3_region": "default",
    }
    import argparse
    parser = argparse.ArgumentParser(description="Test S3 client with temporary access token")
    parser.add_argument("--account", required=False, help="CDSE account to use for token retrieval")
    parser.add_argument('--group', default='logins', help="Group in config file to select account from (default: 'logins')")
    parser.add_argument('--cdsodatacli_conf_file', required=True,
         help="path to the cdsodatacli configuration file .yml ")
    parser.add_argument("--verbose", action="store_true", default=False, help="Enable verbose logging")
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
    headers = {"Authorization": f"Bearer {token}"}
    logging.info("Successfully retrieved temporary access token for account %s", account)
    logging.debug("Token details: %s", {"token": token, "date_generation": date_generation, "login": login})
    s3_client = _get_fresh_s3_client(conf, headers)
    print("Successfully created S3 client with temporary credentials.")
    print(s3_client)