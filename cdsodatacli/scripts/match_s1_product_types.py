#!/usr/bin/env python3
"""
Sentinel-1 Product Matchup Tool.

Associate Sentinel-1 SAFE products with another product type of the same
datatake (e.g. SLC -> GRD, GRD -> OCN, OCN -> SLC...).

Two backends are available:

- ``parquet``: the candidates are read from the local daily
  metadata parquet files produced by ``cds-s1-daily-metadata``
  (``s1_YYYY-MM-DD.parquet``). The directory is given by the
  ``s1_daily_metadata_parquet_dir`` key of the cdsodatacli configuration file.
  All the products are matched at once with a vectorized join: no network access.
- ``odata``: one CDSE OData query per product (rate limited).
- ``auto`` (default for the CLI): ``parquet`` if the configured directory
  exists, otherwise fallback to ``odata``.
"""

import requests
from tqdm import tqdm
import logging
import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import pyarrow.parquet as pq

from cdsodatacli.product_parser import ExplodeSAFE
from cdsodatacli.rate_limiter import RateLimiter
from cdsodatacli.retry import retry_with_backoff
from cdsodatacli.utils import get_conf

# ── CONFIGURATION ────────────────────────────────────────────────────────────
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
VALID_PRODUCT_TYPES = ["GRDH", "GRDM", "SLC_", "OCN_", "RAW_"]
VALID_BACKENDS = ["auto", "parquet", "odata"]
PARQUET_DIR_CONFIG_KEY = "s1_daily_metadata_parquet_dir"
PARQUET_COLUMNS = ["Id", "Name", "ContentLength", "PublicationDate"]
PARQUET_READ_WORKERS = 8  # parallel reads of the daily parquet files
MAX_DELTA_SECONDS = 8
MAX_WORKERS = 4  # Threads parallèles
REQUESTS_PER_SECOND = 30  # Basé sur la limite de 2000/min
MAX_BURST = 40

# Rate limiter global
_GLOBAL_RATE_LIMITER = RateLimiter(
    max_requests_per_second=REQUESTS_PER_SECOND, max_burst=MAX_BURST
)


# ── LOGGING SETUP ────────────────────────────────────────────────────────────
def setup_logger(verbose: bool = False) -> logging.Logger:
    logger = logging.getLogger("s1_matchup")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False  # avoid duplicates if the root logger is configured
    return logger


# ── HELPERS ──────────────────────────────────────────────────────────────────
def parse_start_time(product_name: str) -> datetime | None:
    try:
        inst = ExplodeSAFE(product_name)
        return inst.startdate
    except (IndexError, ValueError, AttributeError):
        return None


# ── CORE LOGIC AVEC RATE LIMITING ET RETRY ────────────────────────────────
@retry_with_backoff(max_retries=5, base_delay=1, max_delay=60)
def _query_odata_with_retry(
    query_filter: str, logger: logging.Logger
) -> requests.Response:
    """Effectue une requête OData avec retry et rate limiting."""
    params = {"$filter": query_filter, "$top": 999}
    _GLOBAL_RATE_LIMITER.wait_if_needed()
    logger.debug("requests.get URL: %s params : %s", ODATA_URL, params)
    response = requests.get(ODATA_URL, params=params, timeout=30)
    logger.debug("response raw: %s", response)
    response.raise_for_status()  # Déclenche le retry sur 4xx/5xx
    return response


def find_product_for_safe(
    source_id: str,
    target_type: str,
    logger: logging.Logger,
    delta_distribution: defaultdict,
) -> dict:
    """Version avec rate limiting et retry intégré."""
    try:
        inst = ExplodeSAFE(source_id)
        source_start = inst.startdate
        source_datatake = inst.mission_data_take
        source_absolute_orbit = inst.absolute_orbit_number
        if source_start is None or source_datatake is None:
            return {
                "source_id": source_id,
                "status": "error",
                "note": "Impossible d'extraire les infos du nom",
            }

        platform = source_id.split("_")[0]
        type_token = target_type.rstrip("_").ljust(4, "_")
        leveltarget = target_level(target_type)
        pol_full = f"{leveltarget}S{inst.polarisation}"
        query_filter = (
            f"startswith(Name,'{platform}') and "
            f"contains(Name,'_{type_token}') and "
            f"contains(Name,'_{source_absolute_orbit}_{source_datatake}') and "
            f"contains(Name,'_{pol_full}') and "
            f"not contains(Name,'_COG')"
        )

        logger.debug("OData query filter: %s", query_filter)

        # Requête avec retry automatique
        response = _query_odata_with_retry(query_filter, logger=logger)
        products = response.json().get("value", [])

        if not products:
            return {
                "source_id": source_id,
                "status": "not_found",
                "note": f"No {target_type} product found for DataTake {source_datatake}",
            }

        # Several versions of a product can share the same name except the
        # unique id: prefer the most recently published one.
        products = sorted(
            products, key=lambda p: p.get("PublicationDate") or "", reverse=True
        )
        # Exact match on the start time only (not a substring of the whole name,
        # which also matched a product whose *stop* time equals the source start).
        exact = next(
            (p for p in products if parse_start_time(p["Name"]) == source_start),
            None,
        )

        if exact:
            match = exact
            match_method = "exact_timestamp"
            delta_distribution[0] += 1
        else:
            reference_dt = parse_start_time(source_id)
            match, delta = closest_in_time(reference_dt, products)
            delta_int = int(delta)
            delta_distribution[delta_int] += 1

            if delta > MAX_DELTA_SECONDS:
                return {
                    "source_id": source_id,
                    "status": "not_found",
                    "note": (
                        f"Closest {target_type} product is {delta:.0f}s away "
                        f"(>{MAX_DELTA_SECONDS}s threshold): {match['Name']}"
                    ),
                }
            match_method = "closest_in_time"
            logger.debug(
                "No exact timestamp match; picked closest product (delta=%.0fs): %s",
                delta,
                match["Name"],
            )

        return {
            "source_id": source_id,
            "target_id": match["Id"],
            "target_name": match["Name"],
            "target_type": target_type,
            "match_method": match_method,
            "size_mb": round(match["ContentLength"] / 1024**2, 2),
            "download_url": (
                f"https://download.dataspace.copernicus.eu"
                f"/odata/v1/Products({match['Id']})/$value"
            ),
        }

    except KeyboardInterrupt:
        raise
    except Exception as exc:
        return {"source_id": source_id, "status": "error", "note": str(exc)}


def closest_in_time(
    reference_dt: datetime, candidates: list[dict]
) -> tuple[dict, float]:
    """Trouve le candidat le plus proche en temps (inchangé)."""

    def time_delta(prod):
        dt = parse_start_time(prod["Name"])
        if dt is None:
            return float("inf")
        return abs((dt - reference_dt).total_seconds())

    best = min(candidates, key=time_delta)
    return best, time_delta(best)


# ── PARQUET BACKEND ──────────────────────────────────────────────────────────
# e.g. S1A_IW_GRDH_1SDV_20230726T071112_20230726T071137_049591_05F692_E123.SAFE
SAFE_NAME_RE = (
    r"^(?P<platform>S1[A-Z])_(?P<mode>[A-Z0-9]{2})_(?P<product>[A-Z_]{4})_"
    r"(?P<level>\d)(?P<kind>[A-Z])(?P<polarisation>[A-Z]{2})_"
    r"(?P<start>\d{8}T\d{6})_(?P<stop>\d{8}T\d{6})_"
    r"(?P<orbit>\d{6})_(?P<datatake>[0-9A-F]{6})_(?P<unique_id>[0-9A-F]{4})"
)
MATCH_KEYS = ["platform", "orbit", "datatake", "polarisation"]


def target_level(target_type: str) -> str:
    """Processing level digit found in the SAFE name of a product type."""
    return {"OCN_": "2", "RAW_": "0"}.get(target_type, "1")


def get_parquet_dir(config_file: str | None = None) -> Path:
    """
    Directory of the daily metadata parquet files, read from the configuration.

    Args:
        config_file (str, optional): path of a cdsodatacli YAML configuration
            file. Defaults to localconfig.yml / config.yml of the package.

    Returns:
        Path: directory containing the ``s1_YYYY-MM-DD.parquet`` files.
    """
    conf = get_conf(config_file)
    value = conf.get(PARQUET_DIR_CONFIG_KEY)
    if not value:
        raise KeyError(
            f"'{PARQUET_DIR_CONFIG_KEY}' is not defined in the configuration file"
        )
    return Path(value)


def daily_parquet_path(parquet_dir: Path, day) -> Path:
    """Daily parquet file name, as written by ``cds-s1-daily-metadata``."""
    return Path(parquet_dir) / f"s1_{day.strftime('%Y-%m-%d')}.parquet"


def parse_safe_names(names: pd.Series) -> pd.DataFrame:
    """Vectorized split of Sentinel-1 SAFE names (NaN rows for invalid names)."""
    fields = names.str.extract(SAFE_NAME_RE)
    fields["start_dt"] = pd.to_datetime(
        fields["start"], format="%Y%m%dT%H%M%S", errors="coerce"
    )
    return fields


def read_daily_candidates(
    parquet_dir: Path,
    days: list,
    target_type: str,
    logger: logging.Logger,
) -> tuple[pd.DataFrame, set]:
    """
    Read the products of the target type from the daily parquet files.

    Only the columns Id, Name, ContentLength and PublicationDate are loaded.

    Returns:
        tuple: (candidates DataFrame with the parsed name fields, set of the
        days whose parquet file is missing).
    """
    type_token = target_type.rstrip("_").ljust(4, "_")
    name_token = f"_{type_token}_{target_level(target_type)}S"

    def _read(day):
        path = daily_parquet_path(parquet_dir, day)
        if not path.exists():
            return day, None
        df = pq.read_table(path, columns=PARQUET_COLUMNS).to_pandas()
        return (
            day,
            df[
                df["Name"].str.contains(name_token, regex=False)
                & ~df["Name"].str.contains("_COG", regex=False)
            ],
        )

    frames, missing_days = [], set()
    with ThreadPoolExecutor(max_workers=PARQUET_READ_WORKERS) as executor:
        for day, df in tqdm(
            executor.map(_read, days),
            total=len(days),
            desc="Reading daily parquet",
            unit="day",
        ):
            if df is None:
                missing_days.add(day)
            else:
                frames.append(df)
    if missing_days:
        logger.warning(
            "%d daily parquet file(s) missing in %s (e.g. %s)",
            len(missing_days),
            parquet_dir,
            daily_parquet_path(parquet_dir, min(missing_days)).name,
        )
    if frames:
        cands = pd.concat(frames, ignore_index=True)
    else:
        cands = pd.DataFrame(columns=PARQUET_COLUMNS)
    cands = cands.drop_duplicates(subset="Name").reset_index(drop=True)
    cands = pd.concat([cands, parse_safe_names(cands["Name"].astype(str))], axis=1)
    return cands.dropna(subset=MATCH_KEYS + ["start_dt"]), missing_days


def find_products_in_parquet(
    safe_list: list[str],
    target_type: str,
    parquet_dir: Path,
    logger: logging.Logger,
    delta_distribution: defaultdict,
) -> list[dict]:
    """
    Match all the SAFE of a listing with the local daily metadata parquet files.

    Same matching rules as :func:`find_product_for_safe` (OData backend):
    same platform, absolute orbit, datatake and polarisation, target product
    type, COG products excluded; exact start time first, otherwise closest
    start time within ``MAX_DELTA_SECONDS``.

    Returns:
        list[dict]: one result per input SAFE (same format as the OData backend).
    """
    max_delta = MAX_DELTA_SECONDS
    src = parse_safe_names(pd.Series(safe_list, dtype=object))
    src["source_id"] = safe_list
    src["src_idx"] = range(len(safe_list))
    invalid = src[src["start_dt"].isna() | src["datatake"].isna()]
    src = src.drop(index=invalid.index)

    # Products are stored in the file of the day of their start time: a target
    # can be in the previous/next day file for a source close to midnight.
    margin = pd.Timedelta(seconds=max_delta)
    needed = set((src["start_dt"] - margin).dt.date) | set(
        (src["start_dt"] + margin).dt.date
    )
    days = sorted(needed)
    logger.info("Reading %d daily parquet file(s) from %s", len(days), parquet_dir)
    cands, missing_days = read_daily_candidates(parquet_dir, days, target_type, logger)
    logger.info("%d %s candidate product(s) loaded", len(cands), target_type)

    merged = src[["src_idx", "start", "start_dt"] + MATCH_KEYS].merge(
        cands[PARQUET_COLUMNS + ["start", "start_dt"] + MATCH_KEYS],
        on=MATCH_KEYS,
        how="inner",
        suffixes=("", "_target"),
    )
    merged["exact"] = merged["start"] == merged["start_target"]
    merged["delta"] = (
        (merged["start_dt_target"] - merged["start_dt"]).dt.total_seconds().abs()
    )
    # exact start time <=> delta == 0, so the smallest delta wins. Tie-break:
    # most recently published version of the product, then Name.
    merged["PublicationDate"] = merged["PublicationDate"].fillna("").astype(str)
    best = (
        merged.sort_values(
            ["src_idx", "delta", "PublicationDate", "Name"],
            ascending=[True, True, False, True],
        )
        .drop_duplicates(subset="src_idx", keep="first")
        .set_index("src_idx")
    )

    results: list[dict | None] = [None] * len(safe_list)
    for idx, source_id in zip(invalid["src_idx"], invalid["source_id"]):
        results[idx] = {
            "source_id": source_id,
            "status": "error",
            "note": "Impossible d'extraire les infos du nom",
        }
    for row in src.itertuples(index=False):
        idx = row.src_idx
        if idx not in best.index:
            days_src = {
                (row.start_dt - margin).date(),
                (row.start_dt + margin).date(),
            }
            if days_src & missing_days:
                results[idx] = {
                    "source_id": row.source_id,
                    "status": "error",
                    "note": "Missing daily parquet file(s): "
                    + ", ".join(str(d) for d in sorted(days_src & missing_days)),
                }
            else:
                results[idx] = {
                    "source_id": row.source_id,
                    "status": "not_found",
                    "note": f"No {target_type} product found for DataTake "
                    f"{row.datatake}",
                }
            continue
        match = best.loc[idx]
        if match["exact"]:
            delta_distribution[0] += 1
            match_method = "exact_timestamp"
        else:
            delta = float(match["delta"])
            delta_distribution[int(delta)] += 1
            if delta > max_delta:
                results[idx] = {
                    "source_id": row.source_id,
                    "status": "not_found",
                    "note": (
                        f"Closest {target_type} product is {delta:.0f}s away "
                        f"(>{max_delta}s threshold): {match['Name']}"
                    ),
                }
                continue
            match_method = "closest_in_time"
        results[idx] = {
            "source_id": row.source_id,
            "target_id": match["Id"],
            "target_name": match["Name"],
            "target_type": target_type,
            "match_method": match_method,
            "size_mb": round(int(match["ContentLength"]) / 1024**2, 2),
            "download_url": (
                f"https://download.dataspace.copernicus.eu"
                f"/odata/v1/Products({match['Id']})/$value"
            ),
        }
    return results


# ── CHECKPOINT SYSTEM ──────────────────────────────────────────────────────
def save_checkpoint(processed_ids: list[str], checkpoint_file: Path):
    """Sauvegarde les IDs déjà traités."""
    with checkpoint_file.open("w") as f:
        for pid in processed_ids:
            f.write(f"{pid}\n")


def load_checkpoint(checkpoint_file: Path) -> set[str]:
    """Charge les IDs déjà traités depuis un checkpoint."""
    if not checkpoint_file.exists():
        return set()
    with checkpoint_file.open("r") as f:
        return {line.strip() for line in f if line.strip()}


# ── ENTRYPOINT OPTIMISÉ ────────────────────────────────────────────────────
def entrypoint(
    safe_list: list[str],
    target_type: str,
    output_filename: str,
    logger: logging.Logger,
    checkpoint_dir: str | None = None,
    backend: str = "odata",
    parquet_dir: str | Path | None = None,
    config_file: str | None = None,
) -> list[dict]:
    """
    Match a listing of SAFE with products of another type and write the results.

    Args:
        safe_list (list[str]): SAFE names to match.
        target_type (str): target product type (one of VALID_PRODUCT_TYPES).
        output_filename (str): text file with one matched SAFE name per line.
        logger (logging.Logger): logger.
        checkpoint_dir (str, optional): directory of the checkpoint file.
        backend (str): "odata" (one CDSE query per product), "parquet"
            (local daily metadata parquet files, much faster) or "auto"
            ("parquet" if the parquet directory is configured and exists,
            otherwise "odata").
        parquet_dir (str or Path, optional): parquet directory, overrides the
            ``s1_daily_metadata_parquet_dir`` configuration key.
        config_file (str, optional): cdsodatacli configuration file.

    Returns:
        list[dict]: one result per SAFE processed.
    """
    if backend not in VALID_BACKENDS:
        raise ValueError(f"backend must be one of {VALID_BACKENDS}, got {backend}")

    # Gestion du checkpoint
    checkpoint_file = None
    if checkpoint_dir:
        checkpoint_path = Path(checkpoint_dir)
        checkpoint_path.mkdir(parents=True, exist_ok=True)
        checkpoint_file = (
            checkpoint_path / f"{Path(output_filename).stem}_checkpoint.txt"
        )
        processed_ids = load_checkpoint(checkpoint_file)
        logger.info(
            f"Loaded checkpoint: {len(processed_ids)} products already processed"
        )
        # Filtrer la liste
        safe_list = [s for s in safe_list if s not in processed_ids]

    logger.info(
        "Starting matchup: %d product(s) → target type '%s'",
        len(safe_list),
        target_type,
    )

    results = []
    delta_distribution = defaultdict(int)
    lock = Lock()
    processed = set()

    backend, parquet_dir = resolve_backend(backend, parquet_dir, config_file, logger)
    if backend == "parquet":
        results = find_products_in_parquet(
            safe_list, target_type, parquet_dir, logger, delta_distribution
        )
        processed = set(safe_list)
    else:
        _match_with_odata(
            safe_list,
            target_type,
            logger,
            delta_distribution,
            results,
            processed,
            lock,
            checkpoint_file,
        )

    # Sauvegarde finale du checkpoint
    if checkpoint_file:
        save_checkpoint(list(processed), checkpoint_file)
        logger.info(f"Checkpoint saved to {checkpoint_file}")

    write_results_and_summary(results, output_filename, delta_distribution, logger)
    return results


def resolve_backend(
    backend: str,
    parquet_dir: str | Path | None,
    config_file: str | None,
    logger: logging.Logger,
) -> tuple[str, Path | None]:
    """
    Choose the backend actually used and the parquet directory.

    "parquet" raises if the directory is not configured or does not exist;
    "auto" falls back to "odata" in that case.

    Returns:
        tuple: (backend "parquet" or "odata", parquet directory or None).
    """
    if backend == "odata":
        return "odata", None
    try:
        if parquet_dir is None:
            parquet_dir = get_parquet_dir(config_file)
        parquet_dir = Path(parquet_dir)
        if not parquet_dir.is_dir():
            raise FileNotFoundError(f"parquet directory not found: {parquet_dir}")
    except (KeyError, FileNotFoundError) as exc:
        if backend == "parquet":
            raise
        logger.warning("%s -> fallback to the OData backend (slower)", exc)
        return "odata", None
    logger.info("Using the daily metadata parquet files of %s", parquet_dir)
    return "parquet", parquet_dir


def _match_with_odata(
    safe_list,
    target_type,
    logger,
    delta_distribution,
    results,
    processed,
    lock,
    checkpoint_file,
):
    """OData backend: one rate-limited query per SAFE, in threads."""
    # Barre de progression avec statistiques
    with tqdm(total=len(safe_list), desc="Matching products", unit="product") as pbar:

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    find_product_for_safe,
                    safe_id,
                    target_type,
                    logger,
                    delta_distribution,
                ): safe_id
                for safe_id in safe_list
            }

            for future in as_completed(futures):
                safe_id = futures[future]
                try:
                    res = future.result()
                    with lock:
                        results.append(res)
                        processed.add(safe_id)

                    # Sauvegarde périodique du checkpoint
                    if checkpoint_file and len(processed) % 10 == 0:
                        save_checkpoint(list(processed), checkpoint_file)

                    # Mise à jour de la progress bar
                    found = sum(1 for r in results if "target_name" in r)
                    not_found = sum(
                        1 for r in results if r.get("status") == "not_found"
                    )
                    errors = sum(1 for r in results if r.get("status") == "error")
                    pbar.set_postfix(
                        {
                            "found": found,
                            "not_found": not_found,
                            "errors": errors,
                            "rate": f"{_GLOBAL_RATE_LIMITER.tokens:.1f}",
                        }
                    )

                except Exception as e:
                    logger.error(f"Error processing {safe_id}: {e}")
                    with lock:
                        results.append(
                            {"source_id": safe_id, "status": "error", "note": str(e)}
                        )
                        processed.add(safe_id)

                pbar.update(1)


def write_results_and_summary(results, output_filename, delta_distribution, logger):
    """Write the matched SAFE names (or NOT_FOUND comments) and log a summary."""
    # Écriture des résultats
    output_path = Path(output_filename)
    with output_path.open("w") as fh:
        for r in results:
            if "target_name" in r:
                fh.write(f"{r['target_name']}\n")
            else:
                fh.write(f"# NOT_FOUND: {r['source_id']} — {r.get('note', '')}\n")

    # Log du résumé
    found = sum(1 for r in results if "target_name" in r)
    not_found = sum(1 for r in results if r.get("status") == "not_found")
    errors = sum(1 for r in results if r.get("status") == "error")
    logger.info(
        "Done — %d found, %d not found, %d errors. Results → %s",
        found,
        not_found,
        errors,
        output_path.resolve(),
    )

    if delta_distribution:
        logger.info("Delta-time distribution (seconds → count):")
        for delta_s, count in sorted(delta_distribution.items()):
            label = f"{delta_s}s" if delta_s > 0 else "exact"
            flag = "  ← above threshold" if delta_s > MAX_DELTA_SECONDS else ""
            logger.info("  %6s : %d%s", label, count, flag)


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Match Sentinel-1 SAFE products to a given product type, using the "
            "local daily metadata parquet files (default) or CDSE OData queries."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From an inline list, look for OCN products
  python s1_matchup.py --prodtype OCN_ \\
      --safe S1A_IW_GRDH_1SDV_20230726T071112_... S1A_IW_GRDH_1SDV_20230726T071137_...

  # From a file (one SAFE ID per line), look for SLC products
  python s1_matchup.py --prodtype SLC_ --input-listing my_products.txt

  # Enable checkpointing
  python s1_matchup.py --prodtype OCN_ --input-listing list.txt --checkpoint-dir ./checkpoints

  # Query CDSE OData instead of the local parquet files
  python s1_matchup.py --prodtype GRDH --input-listing list.txt --backend odata

The parquet directory is read from the key 's1_daily_metadata_parquet_dir'
of the configuration file (localconfig.yml, or the file given with --config).
        """,
    )

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--safe",
        nargs="+",
        metavar="SAFE_ID",
        help="One or more Sentinel-1 SAFE product IDs passed directly on the command line.",
    )
    source_group.add_argument(
        "--input-listing",
        metavar="FILE",
        help="Path to a text file with one SAFE product ID per line (blank lines ignored).",
    )

    parser.add_argument(
        "--prodtype",
        required=True,
        choices=VALID_PRODUCT_TYPES,
        metavar="TYPE",
        help=f"Target product type to search for. Choices: {', '.join(VALID_PRODUCT_TYPES)}",
    )
    parser.add_argument(
        "--output",
        required=True,
        metavar="FILE",
        help="Output text file to write results to (one match per line).",
    )
    parser.add_argument(
        "--checkpoint-dir",
        metavar="DIR",
        help="Directory to store checkpoint files for resuming interrupted runs.",
    )
    parser.add_argument(
        "--backend",
        choices=VALID_BACKENDS,
        default="auto",
        help="Source of the candidate products: local daily metadata parquet "
        "files (fast), CDSE OData queries, or auto (default): parquet if "
        "'s1_daily_metadata_parquet_dir' is configured and exists, else odata.",
    )
    parser.add_argument(
        "--config",
        metavar="FILE",
        default=None,
        help="cdsodatacli YAML configuration file (default: localconfig.yml or "
        "config.yml of the package). Used for 's1_daily_metadata_parquet_dir'.",
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Enable dev mode with reduced number of SAFE to treat.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )

    return parser.parse_args()


def load_listing(filepath: str, logger: logging.Logger) -> list[str]:
    path = Path(filepath)
    if not path.exists():
        logger.error("Input listing file not found: %s", filepath)
        raise SystemExit(1)
    lines = [lili.strip() for lili in path.read_text().splitlines() if lili.strip()]
    logger.info("Loaded %d product ID(s) from %s", len(lines), filepath)
    return lines


def main():
    args = parse_args()
    logger = setup_logger(verbose=args.verbose)

    if args.input_listing:
        safe_list = load_listing(args.input_listing, logger)
    else:
        safe_list = [s.strip() for s in args.safe if s.strip()]

    if args.dev:
        safe_list = safe_list[:5]
        logger.info("Dev mode enabled: limiting to first %d products", len(safe_list))

    entrypoint(
        safe_list=safe_list,
        target_type=args.prodtype,
        output_filename=args.output,
        logger=logger,
        checkpoint_dir=args.checkpoint_dir,
        backend=args.backend,
        config_file=args.config,
    )


if __name__ == "__main__":
    main()
