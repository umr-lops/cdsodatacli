#!/bin/bash
echo start

passwd_cdse_expert=$4
ACCESS_TOKEN=$(curl -d 'client_id=cdse-public' \
                    -d 'username=antoine.grouazel@ifremer.fr' \
                    -d 'password=$passwd_cdse_expert' \
                    -d 'grant_type=password' \
                    'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token' | \
                    python3 -m json.tool | grep "access_token" | awk -F\" '{print $4}')
echo 'ACCESS_TOKEN: '$ACCESS_TOKEN
#productType=WV_OCN__2S_PRIVATE
#productType=WV_SLC__1S_PRIVATE
productType=$1
echo 'productType= '$productType
mindate=$2
echo 'mindate= '$mindate # eg 2025-01-10T23:45:45Z
ouputjsonfile=$3
echo 'output JSON file '$ouputjsonfile
maxrec=2000 #current limit january 2025

curl -X GET "https://catalogue.dataspace.copernicus.eu/resto/api/collections/SENTINEL-1/search.json?publishedAfter=$mindate&exactCount=true&platform=S1C&productType=$productType&maxRecords=$maxrec" \
-H "Authorization: Bearer $ACCESS_TOKEN" \
-d '{
  "query": {
    "match_all": {}
  }
}' > $ouputjsonfile
echo 'output JSON file '$ouputjsonfile
echo finish
