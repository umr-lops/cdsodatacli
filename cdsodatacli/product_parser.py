"""
    Author:  Antoine.Grouazel@ifremer.fr
    Purpose:separate information in SAFE name sentinel1
    Creation:  2014-11-28
    Arguments: basename SAFE directory
    note: valid also for Sentinel3 SRAL data
"""

import sys
import logging
import datetime

fields = [
    "satellite",
    "mode",
    "product",
    "level",
    "polarisation",
    "startdate",
    "enddate",
    "absolute_orbit_number",
    "mission_data_take",
    "product_id",
    "kind",
]


class ExplodeSAFE(object):
    """input basename_safe (str) SAFE name
    only (no parent directories before neitheir children files)"""

    def __init__(self, basename_safe):
        if "/" in basename_safe:
            raise Exception("need basename not full path")
        if basename_safe[0:2] == "S1":
            self.safename = basename_safe
            self.satellite = self.safename[0:3]
            self.mode = self.safename[4:6]
            self.product = self.safename[7:11]
            self.level = self.safename[12]
            self.kind = self.safename[13]
            self.polarisation = self.safename[14:16]
            self.startdate = datetime.datetime.strptime(
                self.safename[17:32], "%Y%m%dT%H%M%S"
            )
            self.enddate = datetime.datetime.strptime(
                self.safename[33:48], "%Y%m%dT%H%M%S"
            )
            self.absolute_orbit_number = self.safename[49:55]
            self.duration = (self.enddate - self.startdate).total_seconds()
            self.sensor = "CbandRadar"
            self.mission_data_take = self.safename[56:62]  # datatake id
            self.product_id = self.safename[
                63:67
            ]  # unique id (processing ID) for a given product id( you can have the same for different product_id)
            self.production_status = "operational"
            self.cycle_number = None
            self.relative_orbit_number = None

        elif basename_safe[0:2] == "S2":
            # S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE
            self.safename = basename_safe
            splitted = self.safename.replace(".SAFE", "").split("_")
            # splitted: ['S2A', 'MSIL1C', '20170105T013442', 'N0204', 'R031', 'T53NMJ', '20170105T013443']

            self.satellite = splitted[0]  # S2A / S2B / S2C
            self.sensor = "MultiSpectralImager"
            self.mode = None
            self.kind = None
            self.polarisation = None

            self.product = splitted[1]  # MSIL1C or MSIL2A
            self.level = self.product[3:]  # L1C or L2A (safer than split("L")[1])

            self.startdate = datetime.datetime.strptime(splitted[2], "%Y%m%dT%H%M%S")
            self.enddate = self.startdate  # S2 products are snapshots, no end date

            self.processing_baseline = splitted[3]  # N0204
            self.relative_orbit_number = int(splitted[4][1:])  # R031 -> 31
            self.tile = splitted[5]  # T53NMJ (includes the T prefix)

            self.production_date = datetime.datetime.strptime(
                splitted[6], "%Y%m%dT%H%M%S"
            )  # product discriminator date

            self.duration = 0  # snapshot, no meaningful duration
            self.absolute_orbit_number = None
            self.mission_data_take = None
            self.product_id = splitted[6]  # discriminator used as unique id
            self.cycle_number = None
            self.production_status = "operational"

        elif basename_safe[0:2] == "S3":
            self.safename = basename_safe
            splitos = self.safename.split("_")
            self.satellite = splitos[0]
            self.mode = None
            self.sensor = splitos[1]
            self.product = splitos[3]
            self.duration = splitos[10]
            self.level = splitos[2]
            self.kind = None
            #             self.kind = self.safename[13]
            self.polarisation = None
            #             print splitos[7]
            self.startdate = datetime.datetime.strptime(splitos[7], "%Y%m%dT%H%M%S")
            self.enddate = datetime.datetime.strptime(splitos[9], "%Y%m%dT%H%M%S")
            self.absolute_orbit_number = None
            self.cycle_number = splitos[11]
            self.relative_orbit_number = splitos[12]
            self.mission_data_take = splitos[9]  # product id
            self.product_generating_center = splitos[18]
            self.product_id = splitos[
                10
            ]  # unique id for a given product id( you can have the same for different product_id)
            productions_status_code = {
                "O": "operational",
                "F": "reference",
                "D": "development",
                "R": "reprocessing",
            }
            self.production_status = productions_status_code[splitos[19]]
        return

    def props(self):
        return [i for i in self.__dict__.keys() if i[:1] != "_"]

    def get(self, info):
        #         if info in fields:
        res = getattr(self, info)
        #         else:
        #             logging.error('no field %s in safe name',info)
        #             res = None
        return res


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) > 1:
        safe = sys.argv[1]
    else:
        #     safe = 'S1A_WV_SLC__1SSV_20141113T141141_20141113T143623_003264_003C69_1CDB.SAFE'
        safe = "S3A_SR_2_WAT____20170124T120058_20170124T121058_20170124T140548_0599_013_294______MAR_O_NR_002.SEN3"  # attention fichiers coupe en demi orbit mais une seul numero de cycle
    logging.info("%s", safe)
    obj = ExplodeSAFE(safe)
    print(obj.get("startdate"))
    #     for ff in fields:
    for ff in obj.props():
        val = obj.get(ff)
        logging.debug("info %s => %s", ff, val)
    print("start date=", obj.startdate)
