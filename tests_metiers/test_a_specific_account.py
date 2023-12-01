import subprocess
import os
import logging
from cdsodatacli.utils import conf
if __name__ == "__main__":
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    import argparse
    parser = argparse.ArgumentParser(description="highleveltest-test_CDSE_account")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--login",
        required=False,
        help="login CDSE email address [default=all in conf]",
    )
    parser.add_argument(
        "--password",
        required=False,default=None,
        help="password [optional, default is the one from config file]",
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
    if args.login:
        logins = [args.login]
    else:
        logins = conf['logins']
    for logii in logins:
        # print('test',logii)
        if args.password is None:
            passwd = conf['logins'][logii]
        else:
            passwd = args.password
        cmd = "curl -s --location --request POST https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token --header 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'grant_type=password' --data-urlencode 'username=%s' --data-urlencode 'password=%s'  --data-urlencode 'client_id=cdse-public'"%(logii,passwd)
        try:
            print(cmd)
            res = subprocess.check_output(cmd,shell=True).decode()
            #print(res,type(res))
            print(logii,'access_token' in res)
        except:
            print('error for',logii)
