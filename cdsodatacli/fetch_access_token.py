import pdb

from cdsodatacli.utils import conf
import subprocess
import logging
import json
import datetime
def get_bearer_access_token(quiet=True):
    """
    OData access token (validity=600sec)
    Returns
    -------

    """
    url_identity = conf['URL_identity']
    login = conf['login']
    passwd = conf['password']
    if quiet:
        prefix = 'curl -s '
    else:
        prefix = 'curl '
    cmd = prefix+" --location --request POST "+url_identity+" --header 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'grant_type=password' --data-urlencode 'username=%s' --data-urlencode 'password=%s'  --data-urlencode 'client_id=cdse-public'"%(login,passwd)

    logging.debug('cmd: %s',cmd)
    date_generation_access_token = datetime.datetime.today()
    answer_identity = subprocess.check_output(cmd,shell=True)
    logging.debug('answer_identity: %s',answer_identity)
    toto = answer_identity.decode('utf8').replace("'", '"')
    data = json.loads(toto)
    return data['access_token'],date_generation_access_token

