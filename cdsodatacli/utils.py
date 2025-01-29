from yaml import load
import logging
import os
import cdsodatacli
from yaml import CLoader as Loader
import datetime
import pandas as pd
import pdb
import json

local_config_pontential_path = os.path.join(
    os.path.dirname(cdsodatacli.__file__), "localconfig.yml"
)
config_path = os.path.join(os.path.dirname(cdsodatacli.__file__), "config.yml")
if os.path.exists(local_config_pontential_path):
    used_config_path = local_config_pontential_path
else:
    used_config_path = config_path
logging.info("config path that is used: %s", used_config_path)
stream = open(used_config_path, "r")
conf = load(stream, Loader=Loader)


def check_safe_in_spool(safename):
    """

    Parameters
    ----------
    safename (str) basename

    Returns
    -------
        present_in_spool (bool): True -> the product is already in the spool dir

    """
    present_in_spool = False
    for uu in ["", ".zip", "replaced"]:
        if uu == "":
            spool_potential_file = os.path.join(conf["spool"], safename)
        elif uu == ".zip":
            spool_potential_file = os.path.join(conf["spool"], safename + ".zip")
        elif uu == "replaced":
            spool_potential_file = os.path.join(
                conf["spool"], safename.replace(".SAFE", ".zip")
            )
        else:
            raise NotImplemented
        if os.path.exists(spool_potential_file):
            present_in_spool = True
            break
    logging.debug("present_in_spool : %s", present_in_spool)
    return present_in_spool

def check_safe_in_outputdir(outputdir,safename):
    """

    Parameters
    ----------
    safename (str) basename

    Returns
    -------
        present_in_outputdir (bool): True -> the product is already in the spool dir

    """
    present_in_outdir = False
    for uu in ["", ".zip", "replaced"]:
        if uu == "":
            potential_file = os.path.join(outputdir, safename)
        elif uu == ".zip":
            potential_file = os.path.join(outputdir, safename + ".zip")
        elif uu == "replaced":
            potential_file = os.path.join(
                outputdir, safename.replace(".SAFE", ".zip")
            )
        else:
            raise NotImplemented
        if os.path.exists(potential_file):
            present_in_outdir = True
            break
    logging.debug("present_in_spool : %s", present_in_outdir)
    return present_in_outdir


def WhichArchiveDir(safe):
    """
    Args:
        safe (str): safe base name
    """
    logging.debug('safe: %s',safe)
    if 'S1' in safe:
        firstdate = safe[17:32]
    elif 'S2' in safe:
        firstdate = safe[11:26]
    year = firstdate[0:4]
    # try:
    # doy = str(
    #     datetime.datetime.strptime(firstdate, "%Y%m%d").timetuple().tm_yday
    # ).zfill(3)
    doy = datetime.datetime.strptime(firstdate, "%Y%m%dT%H%M%S").strftime('%j')
    sat = safe.split("_")[0]
    if sat == "S1A":
        satdir = "sentinel-1a"
    elif sat == "S1B":
        satdir = "sentinel-1b"
    elif sat == 'S1C':
        satdir = "sentinel-1c"
    elif sat == 'S1D':
        satdir = "sentinel-1d"
    elif sat =='S2B':
        satdir = 'sentinel-2b'
    elif sat =='S2A':
        satdir = 'sentinel-2a'
    else:
        satdir = ""
        logging.error("%s is not a  good satellite name", sat)
    acqui = safe.split("_")[1]
    if acqui[0] == "S":
        acqui = "SM"
    level = safe[12:13]
    subproddir = "L" + level
    subname = safe[6:14]
    litlerep = sat + "_" + acqui + subname
    gooddir = os.path.join(
        conf["archive"], satdir, subproddir, acqui, litlerep, year, doy
    )
    return gooddir


def check_safe_in_archive(safename):
    """

    Parameters
    ----------
    safename (str)

    Returns
    -------
        present_in_archive (bool): True -> the product is already in the archive dir

    """
    present_in_archive = False
    for uu in ["", ".zip", "replaced"]:
        arch_potential_file0 = os.path.join(WhichArchiveDir(safename),safename)
        if uu == "":
            arch_potential_file = arch_potential_file0
        elif uu == ".zip":
            arch_potential_file = arch_potential_file0 + ".zip"
        elif uu == "replaced":
            arch_potential_file = arch_potential_file0.replace(".SAFE", ".zip")
        else:
            raise NotImplemented
        if os.path.exists(arch_potential_file):
            present_in_archive = True
            break
    logging.debug("present_in_archive : %s", present_in_archive)
    if present_in_archive:
        logging.debug('the product is stored in : %s',arch_potential_file)
    return present_in_archive


def convert_json_opensearch_query_to_listing_safe_4_dowload(json_path)->str:
    """

    Parameters
    ----------
    json_path str: full path of the OpenSearch file giving the meta data from the CDSE

    Returns
    -------
        output_txt str: listing with 2 columns: id,safename
    """
    logging.info('input json file: %s',json_path)
    with open(json_path, "r") as f:
        data = json.load(f)
    df = pd.json_normalize(data['features'])
    sub = df[['id','properties.title']]
    sub.drop_duplicates()
    output_txt = json_path.replace('.json','.txt')
    sub.to_csv(output_txt,header=False,index=False)
    logging.info('output_txt : %s',output_txt)
    return output_txt

