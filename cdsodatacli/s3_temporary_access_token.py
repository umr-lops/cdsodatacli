import boto3
import requests
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