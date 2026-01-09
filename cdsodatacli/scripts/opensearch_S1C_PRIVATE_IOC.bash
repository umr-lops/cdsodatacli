#!/bin/bash
echo "start"

usage() {
  cat <<'USAGE'
Usage: opensearch_S1C_PRIVATE_IOC.bash <productType> <mindate> <outputJsonFile> <sarunit> <passwd_cdse_expert> <email_account_cdse>

Positional arguments:
  productType            Product type (e.g. WV_SLC__1S_PRIVATE)
  mindate                Start date in ISO format, e.g. 2025-01-10T23:45:45Z
  outputJsonFile         File to write the JSON search result to
  sarunit                Platform (e.g. SENTINEL-1)
  passwd_cdse_expert     Password for CDSE expert account
  email_account_cdse     Email for CDSE account

Options:
  -h, --help             Show this help message and exit
USAGE
}

# Vérifier si on demande l'aide ou si le nombre d'arguments est insuffisant
if [[ "$1" == "-h" || "$1" == "--help" || $# -lt 6 ]]; then
  usage
  exit 0
fi

# Assignation correcte des variables (selon l'ordre du help)
productType=$1
mindate=$2
outputJsonFile=$3
sarunit=$4
passwd_cdse_expert=$5
email_account_cdse=$6

echo "email_account_cdse: $email_account_cdse"
# On ne print pas le password par sécurité en prod, mais laissé ici pour debug :
# echo "passwd_cdse_expert: $passwd_cdse_expert"

# 1. Récupération du Token
RESPONSE=$(curl --insecure -s -w "\n__HTTP_STATUS__:%{http_code}" \
                    -d 'client_id=cdse-public' \
                    -d "username=${email_account_cdse}" \
                    -d "password=${passwd_cdse_expert}" \
                    -d 'grant_type=password' \
                    'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token')

http_status=${RESPONSE##*__HTTP_STATUS__:}
body=${RESPONSE%__HTTP_STATUS__:*}

ACCESS_TOKEN=$(printf '%s' "$body" | python3 - <<'PY' 2>/dev/null
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('access_token',''))
except Exception:
    pass
PY
)

if [ -z "$ACCESS_TOKEN" ]; then
  echo "ERROR: Failed to obtain ACCESS_TOKEN (HTTP status: $http_status)." >&2
  echo "Response body:" >&2
  printf '%s\n' "$body" >&2
  exit 1
fi

echo "ACCESS_TOKEN obtained successfully."

# 2. Requête de recherche
maxrec=2000

# Construction de l'URL
URL="https://catalogue.dataspace.copernicus.eu/resto/api/collections/SENTINEL-1/search.json?publishedAfter=${mindate}&exactCount=true&platform=${sarunit}&productType=${productType}&maxRecords=${maxrec}"

echo "Searching: $URL"

curl --insecure -X GET "$URL" \
     -H "Authorization: Bearer $ACCESS_TOKEN" \
     > "$outputJsonFile"

echo "Output written to: $outputJsonFile"
echo "finish"