#!/usr/bin/env python3
import datetime


def guess_s3_path(safebasename: str) -> str:
    """
    Guess the S3 path for a given safebasename.
    This method allows to skip queries to OData API in order to get the S3Path of a product.



    example
    S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE  gives Sentinel-1/SAR/SLC/2019/01/18/S1A_IW_SLC__1SDV_20190118T023637_20190118T023705_025526_02D4A8_08A8.SAFE


    Arguments
    ---------
    safebasename: str, the basename of the SAFE product
    Returns
    -------
    s3path_expected: str, the expected S3 path for the given safebasename


    """
    if safebasename.startswith("S1"):
        firstdate = safebasename[17:32]
        rad_path = "Sentinel-1/SAR/"
        dt = datetime.datetime.strptime(firstdate, "%Y%m%dT%H%M%S")
        if dt < datetime.datetime(2023, 2, 20):
            productype = safebasename.split("_")[2][0:3]  # e.g. SLC
        else:
            productype = safebasename[4:14]  # e.g. IW_SLC__1S

    elif safebasename.startswith("S2"):
        # S2A_MSIL1C_20170105T013442_N0204_R031_T53NMJ_20170105T013443.SAFE
        firstdate = safebasename.split("_")[2]  # 20170105T013442
        dt = datetime.datetime.strptime(firstdate, "%Y%m%dT%H%M%S")
        rad_path = "Sentinel-2/MSI/"
        # product level from field 1: MSIL1C -> L1C, MSIL2A -> L2A
        productype = safebasename.split("_")[1][3:]  # L1C or L2A

    else:
        raise ValueError("safebasename not handled: %s" % safebasename)

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    s3path_expected = f"{rad_path}{productype}/{year}/{month}/{day}/{safebasename}"
    return s3path_expected


if __name__ == "__main__":
    # end 2 end test
    import pandas as pd
    from tqdm import tqdm
    from collections import Counter

    cpt = Counter()
    df = pd.read_csv(
        "/scale/project/lops-siam-sentinel1-workbench/data/ifremer/listing_products/sentinel1/IW/wind_direction/full_train_test_val_unused_IW_SLC_base_safe_JRM_May2025_l2windddir_data_clean_s3path.csv",
        header=0,
    )
    df["s3path_expected"] = df["safename"].apply(guess_s3_path)
    for ii in tqdm(range(len(df["s3path_expected"]))):
        if df["s3path_expected"][ii] == df["S3Path"][ii]:
            cpt["OK"] += 1
        else:
            cpt["KO"] += 1
            print(
                "KO for ii: %s, safename: %s, expected: %s, got: %s"
                % (ii, df["safename"][ii], df["s3path_expected"][ii], df["S3Path"][ii])
            )
    print(f"Results: OK={cpt['OK']}, KO={cpt['KO']}")
