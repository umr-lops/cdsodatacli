import numpy as np
import logging

import cdsodatacli.download as dl
import pandas as pd
import tempfile

# listing = '/scale/project/lops-siam-sentinel1-workbench/data/ifremer/listing_products/sentinel1/IW/wind_direction/dataset_winddirection_iw_slc_safe_basename_only2_JRM_coloc_HY2_to_be_download_and_process.txt'
# listing = '/raid/localscratch/agrouaze/dummylistings1_iterative_tests_ids.txt'
import os


def entrypoint():
    args = parse_args()
    add_ids_to_listing_iterative(
        input_listing=args.input_listing, output_listing=args.output_listing
    )


# def add_ids_to_listing_iterative(input_listing,output_listing=None):
#     """
#     This method aim as a wrapper for add_missing_cdse_hash_ids_in_listing(),
#     Sometimes (condition not clear for now, too many request?) some URLs/queries return error
#     Then thanks to iterative method like this one, we can save a SAFE+ids listing to avoid redoing it
#     Particularly interesting for batch processing, when restarts are needed.

#     :param input_listing: str input listing with only SAFE basenames
#     :param output_listing: str where to store SAFE+ID [optional, default is input_listing+'.ids']

#     Return:
#         output_listing: str: listing containing lines SAFE+ID
#     """


#     if output_listing is None:
#         output_listing = input_listing+'.ids'
#     logging.info('start getting iterative IDs CDSE')
#     logging.info('read input listing: %s',input_listing)
#     os.path.exists(input_listing)

#     df = pd.read_csv(input_listing,names=['safename'])
#     logging.info('Number of lines in the input listing: %s',len(df))
#     df_target = df.assign(id=np.nan)
#     cpt_ids_found = (df_target['id'].isna()==False).sum()
#     logging.info('cpt_ids_found %d',cpt_ids_found)
#     tmplisting = input_listing
#     loop_cpt = 0
#     logging.getLogger().setLevel(logging.CRITICAL)
#     while cpt_ids_found!= len(df_target['id']):
#         loop_cpt += 1
#         dfres = dl.add_missing_cdse_hash_ids_in_listing(listing_path=tmplisting,
#                                                                 display_tqdm=True)

#         df_target = df_target.merge(
#         dfres,
#         on="safename",
#         how="left",
#         suffixes=("", "_ref")
#         )
#         # Update 'id' with 'id_ref' ONLY where 'id' is currently NaN
#         df_target["id"] = df_target["id"].fillna(df_target["id_ref"])
#         df_target = df_target.drop(columns="id_ref")

#         # re write the listing of missing
#         dfmissing = df_target[(df_target['id'].isna()==True)]
#         # tmplisting = '/raid/localscratch/agrouaze/tmp_missing_id_listing_test.txt'
#         tmplisting = os.path.join(tempfile.gettempdir(),
#                               'tmp_missing_id_listing_cdsodatacli.txt')

#         dfmissing['safename'].to_csv(tmplisting,header=False,index=False)
#         cpt_ids_found = (df_target['id'].isna()==False).sum()
#         logging.info('loop %d missing: %d ID found: %d',
#                      loop_cpt,len(dfmissing['safename']),cpt_ids_found)
#         # add a save to disk of partial df SAFE+IDs to restart if stuck
#         tmplisting_safeid = os.path.join(tempfile.gettempdir(),
#                               'tmp_safe_id_listing_cdsodatacli_probably_incomplet.txt')
#         df_target.to_csv(tmplisting_safeid,header=False,index=False)
#         logging.info('save incomplet retrieval SAFE+IDs : %s',tmplisting_safeid)
#     logging.info('loop to get IDs is over.')
#     logging.info('cpt_ids_found %s',cpt_ids_found)
#     df_target
#     os.makedirs(os.path.dirname(output_listing),exist_ok=True)
#     df_target.to_csv(output_listing,header=False,index=False)
#     try:
#         os.chmod(output_listing,0o0644)
#     except PermissionError:
#         logging.error('not allowed to change permission on %s',output_listing)
#     # TODO add a save to disk of the listing SAFE+IDs
#     logging.info('output listing: %s',output_listing)
#     return output_listing


def add_ids_to_listing_iterative(input_listing, output_listing=None):
    """
    This method aim as a wrapper for add_missing_cdse_hash_ids_in_listing(),
    Sometimes (condition not clear for now, too many request?) some URLs/queries return error
    Then thanks to iterative method like this one, we can save a SAFE+ids listing to avoid redoing it
    Particularly interesting for batch processing, when restarts are needed.

    :param input_listing: str input listing with only SAFE basenames
    :param output_listing: str where to store SAFE+ID [optional, default is input_listing+'.ids']

    Return:
        output_listing: str: listing containing lines SAFE+ID
    """
    if output_listing is None:
        output_listing = input_listing + ".ids"
    logging.info("start getting iterative IDs CDSE")
    logging.info("read input listing: %s", input_listing)

    if not os.path.exists(input_listing):
        raise FileNotFoundError(f"{input_listing} not found")

    df = pd.read_csv(input_listing, names=["safename"])

    # Sécurité : On s'assure que le listing d'entrée est unique si ce n'est pas le comportement voulu
    # (optionnel, mais recommandé si votre input est censé être unique)
    # df = df.drop_duplicates(subset=['safename'])

    logging.info("Number of lines in the input listing: %s", len(df))
    df_target = df.assign(id=np.nan)
    # cpt_ids_found = (df_target["id"].isna() == False).sum()
    cpt_ids_found = (not df_target["id"].isna()).sum()
    logging.info("cpt_ids_found %d", cpt_ids_found)

    tmplisting = input_listing
    loop_cpt = 0
    logging.getLogger().setLevel(
        logging.CRITICAL
    )  # Attention, cela coupe les logs globaux

    # On boucle tant qu'on n'a pas tout trouvé
    # NOTE: J'ai ajouté une condition de sécurité (loop_cpt < 10) pour éviter les boucles infinies si un ID n'existe vraiment pas
    while cpt_ids_found != len(df_target["id"]) and loop_cpt < 20:
        loop_cpt += 1

        # Appel à l'API via cdsodatacli
        dfres = dl.add_missing_cdse_hash_ids_in_listing(
            listing_path=tmplisting, display_tqdm=True
        )

        # --- CORRECTION ICI ---
        # Si l'API retourne plusieurs lignes pour le même SAFE, on ne garde que la première.
        if not dfres.empty:
            dfres = dfres.drop_duplicates(subset=["safename"])
        # ----------------------

        df_target = df_target.merge(
            dfres, on="safename", how="left", suffixes=("", "_ref")
        )

        # Mise à jour des IDs
        if "id_ref" in df_target.columns:
            df_target["id"] = df_target["id"].fillna(df_target["id_ref"])
            df_target = df_target.drop(columns="id_ref")

        # Préparation du listing pour la prochaine itération (ceux qui restent NaN)
        # dfmissing = df_target[(df_target["id"].isna() == True)]
        dfmissing = df_target[(df_target["id"].isna())]

        # Si on n'a rien trouvé de nouveau dans cette boucle, inutile de continuer indéfiniment
        new_cpt = (not df_target["id"].isna()).sum()
        if new_cpt == cpt_ids_found and loop_cpt > 1:
            logging.warning(
                "Aucun nouvel ID trouvé lors de cette itération, arrêt de la boucle."
            )
            break
        cpt_ids_found = new_cpt

        if len(dfmissing) == 0:
            break

        tmplisting = os.path.join(
            tempfile.gettempdir(), "tmp_missing_id_listing_cdsodatacli.txt"
        )
        dfmissing["safename"].to_csv(tmplisting, header=False, index=False)

        logging.info(
            "loop %d missing: %d ID found: %d",
            loop_cpt,
            len(dfmissing["safename"]),
            cpt_ids_found,
        )

        # Sauvegarde intermédiaire
        tmplisting_safeid = os.path.join(
            tempfile.gettempdir(),
            "tmp_safe_id_listing_cdsodatacli_probably_incomplet.txt",
        )
        df_target[["id", "safename"]].to_csv(
            tmplisting_safeid, header=False, index=False
        )

    logging.info("loop to get IDs is over.")
    logging.info("Final count found: %s / %s", cpt_ids_found, len(df))

    # --- CORRECTION FINALE (Ceinture et bretelles) ---
    # Si malgré tout des doublons se sont glissés, on force la taille finale
    # pour qu'elle corresponde à la liste d'entrée unique.
    df_target = df_target.drop_duplicates(subset=["safename"])

    # On s'assure que l'ordre et le nombre correspondent au fichier d'entrée original
    # (Merge left initial garantit l'ordre, mais drop_duplicates garde la première occurrence)
    if len(df_target) != len(df):
        logging.warning(
            f"Attention: Output lines ({len(df_target)}) != Input lines ({len(df)})"
        )
    # -------------------------------------------------

    os.makedirs(os.path.dirname(output_listing), exist_ok=True)
    df_target = df_target.reindex(columns=["id", "safename"])
    df_target.to_csv(output_listing, header=False, index=False)

    try:
        os.chmod(output_listing, 0o0644)
    except PermissionError:
        logging.error("not allowed to change permission on %s", output_listing)

    logging.info("output listing: %s", output_listing)
    return output_listing


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(description="example main")
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument(
        "--output-listing",
        help="output listing where the SAFE+id will be written"
        " [optional, default is input-listing+'.ids']",
        required=False,
    )
    parser.add_argument(
        "--input-listing",
        required=True,
        help="input SAFE listing path containing only SAFE basenames",
        type=str,
    )

    args = parser.parse_args()
    fmt = "%(asctime)s %(levelname)s %(filename)s(%(lineno)d) %(message)s"
    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG, format=fmt, datefmt="%d/%m/%Y %H:%M:%S"
        )
    else:
        logging.basicConfig(level=logging.INFO, format=fmt, datefmt="%d/%m/%Y %H:%M:%S")
    return args


if __name__ == "__main__":

    entrypoint()
