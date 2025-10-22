import os.path

from yaml import CLoader as Loader
from yaml import load
import logging
from cdsodatacli.utils import local_config_pontential_path, config_path


def test_to_make_sure_localconfig_and_config_contains_same_keys():
    all_keys_are_presents = True
    if os.path.exists(local_config_pontential_path) and os.path.exists(config_path):
        stream = open(local_config_pontential_path, "r")
        conflocal = load(stream, Loader=Loader)

        stream = open(config_path, "r")
        defaultconf = load(stream, Loader=Loader)

        # for keyc in conflocal:
        #     assert (
        #         keyc in defaultconf
        #     ), f"Key '{keyc}' is missing from the default configuration"
        #     if keyc not in defaultconf:
        #         all_keys_are_presents = False

        for keyc in defaultconf:
            assert (
                keyc in conflocal
            ), f"Key '{keyc}' is missing from the local configuration"
            if keyc not in conflocal:
                all_keys_are_presents = False
    else:
        logging.info(
            "you don't have localconfig.yml defined for cdsodatacli lib (it is not mandatory but it means you cant download data)"
        )

    assert all_keys_are_presents is True
