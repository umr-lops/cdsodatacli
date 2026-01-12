#!/usr/bin/env python3
import argparse
import requests
import json
import sys
import urllib3


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
        "-d", "--date", required=True, help="Start date (e.g. 2025-01-01T00:00:00)"
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

    args = parser.parse_args()

    # --- 1. Authentication ---
    auth_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    auth_data = {
        "client_id": "cdse-public",
        "username": args.email,
        "password": args.password,
        "grant_type": "password",
    }

    print(f"[*] Authenticating for {args.email}...")
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.post(auth_url, data=auth_data, verify=False)
        response.raise_for_status()
        access_token = response.json().get("access_token")
    except Exception as e:
        print(f"[-] Auth Error: {e}")
        if "response" in locals():
            print(f"Details: {response.text}")
        sys.exit(1)

    # --- 2. OData Query Construction ---
    odata_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

    # Using the specific StringAttribute syntax required by CDSE OData
    filters = [
        "Collection/Name eq 'SENTINEL-1'",
        f"ContentDate/End ge {args.date}",
        f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{args.type}')",
        f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'platformSerialIdentifier' and att/OData.CSC.StringAttribute/Value eq '{args.unit}')",
    ]

    odata_filter = " and ".join(filters)

    params = {
        "$filter": odata_filter,
        "$orderby": "ContentDate/Start asc",
        "$top": args.limit,
        "$count": "true",
    }

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    print("[*] Querying CDSE OData...")

    try:
        # Requests automatically handles URL encoding of spaces, quotes, and symbols
        search_res = requests.get(
            odata_url, params=params, headers=headers, verify=False
        )
        search_res.raise_for_status()
        data = search_res.json()

        # --- 3. Save Results ---
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        count = len(data.get("value", []))
        print(f"[+] Success! {count} products found.")
        print(f"[+] Results saved to: {args.output}")

    except Exception as e:
        print(f"[-] Search Error: {e}")
        if "search_res" in locals():
            print(f"Response: {search_res.text}")
        sys.exit(1)

    print("finish")


if __name__ == "__main__":
    main()
