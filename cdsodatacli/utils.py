from yaml import load
import logging
import os
import cdsodatacli
from yaml import CLoader as Loader
import datetime
import pdb
local_config_pontential_path = os.path.join(
    os.path.dirname(cdsodatacli.__file__), "localconfig.yml"
)

if os.path.exists(local_config_pontential_path):
    config_path = local_config_pontential_path
else:
    config_path = os.path.join(os.path.dirname(cdsodatacli.__file__), "config.yml")
logging.info("config path: %s", config_path)
stream = open(config_path, "r")
conf = load(stream, Loader=Loader)


def test_safe_spool(safename):
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


def WhichArchiveDir(safe):
    """
    Args:
        safe (str): safe base name
    """

    firstdate = safe[17:25]
    year = firstdate[0:4]
    doy = str(
        datetime.datetime.strptime(firstdate, "%Y%m%d").timetuple().tm_yday
    ).zfill(3)
    sat = safe.split("_")[0]
    if sat == "S1A":
        satdir = "sentinel-1a"
    elif sat == "S1B":
        satdir = "sentinel-1b"
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


def test_safe_archive(safename):
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
