#!/usr/bin/env python3
"""
GET PRODUCTS METADATA - CLI tool to retrieve metadata from SAFE names or Product IDs

Simplified version: only --input-listing as input, CSV output only.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
import geopandas as gpd
from shapely.ops import unary_union
from tqdm import tqdm

# Import des composants existants
from cdsodatacli.fetch_access_token import get_access_token
from cdsodatacli.rate_limiter import RateLimiter
from cdsodatacli.retry import retry_with_backoff
from cdsodatacli.product_parser import ExplodeSAFE

# ── CONSTANTES ──────────────────────────────────────────────────────────────
DEFAULT_BATCH_SIZE = 15
DEFAULT_MAX_WORKERS = 4
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 5
DEFAULT_REQUESTS_PER_SECOND = 30
DEFAULT_MAX_BURST = 50

ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

# Modes valides pour Sentinel-1 (incluant les modes strip-map)
VALID_MODES_S1 = ["IW", "EW", "SM", "WV", "S1", "S2", "S3", "S4", "S5", "S6"]

# Rate limiter global
_GLOBAL_RATE_LIMITER = RateLimiter(
    max_requests_per_second=DEFAULT_REQUESTS_PER_SECOND, max_burst=DEFAULT_MAX_BURST
)


# ── LOGGING ────────────────────────────────────────────────────────────────
def setup_logger(verbose: bool = False) -> logging.Logger:
    """Configure et retourne le logger principal."""
    logger = logging.getLogger("get_products_metadata")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# ── VALIDATION DES SAFE NAMES ────────────────────────────────────────────
def validate_safe_name(safe_name: str) -> Dict:
    """Valide et parse un nom de produit SAFE en utilisant ExplodeSAFE."""
    result = {
        "safe_name": safe_name,
        "is_valid": False,
        "error": None,
        "parsed": None,
        "mission": None,
        "mode": None,
        "product_type": None,
        "polarization": None,
        "start_timestamp": None,
        "end_timestamp": None,
        "datatake": None,
        "orbit": None,
        "processing_id": None,
    }

    safe_name = safe_name.strip()
    if not safe_name:
        result["error"] = "Empty SAFE name"
        return result

    try:
        parsed = ExplodeSAFE(safe_name)
        result["parsed"] = parsed

        result["mission"] = getattr(parsed, "satellite", None)
        result["mode"] = getattr(parsed, "mode", None)
        result["product_type"] = getattr(parsed, "product", None)
        result["polarization"] = getattr(parsed, "polarisation", None)
        result["start_timestamp"] = getattr(parsed, "startdate", None)
        result["end_timestamp"] = getattr(parsed, "enddate", None)
        result["datatake"] = getattr(parsed, "mission_data_take", None)
        result["orbit"] = getattr(parsed, "absolute_orbit_number", None)
        result["processing_id"] = getattr(parsed, "product_id", None)

        # Nettoyer product_type
        if result["product_type"]:
            result["product_type"] = result["product_type"].rstrip("_")

        # Normaliser les valeurs vides
        for key in ["processing_id", "datatake", "orbit"]:
            if result.get(key) == "":
                result[key] = None

        if result["start_timestamp"] is None:
            result["error"] = "Missing start timestamp"
            return result

        # Validation supplémentaire pour Sentinel-1
        if result["mission"] and result["mission"].startswith("S1"):
            # FIX 2: Support des modes strip-map (S1, S2, S3, S4, S5, S6)
            if result["mode"] not in VALID_MODES_S1:
                result["error"] = (
                    f"Invalid mode: {result['mode']} (expected {VALID_MODES_S1})"
                )
                return result

            valid_product_types = ["GRD", "SLC", "RAW", "OCN"]
            product_base = result["product_type"][:3] if result["product_type"] else ""
            if product_base not in valid_product_types:
                result["error"] = (
                    f"Invalid product type: {result['product_type']} (expected {valid_product_types})"
                )
                return result

        result["is_valid"] = True

    except Exception as e:
        error_msg = str(e)
        if "unconverted data remains" in error_msg:
            result["error"] = f"Invalid SAFE name format: {error_msg}"
        elif "need basename not full path" in error_msg:
            result["error"] = "Full path provided, need basename only"
        else:
            result["error"] = f"ExplodeSAFE parsing error: {error_msg}"

    return result


def validate_safe_names(
    safe_names: List[str], logger: logging.Logger
) -> Tuple[List[str], List[Dict]]:
    """Valide une liste de noms SAFE."""
    valid = []
    invalid = []

    for name in safe_names:
        result = validate_safe_name(name)
        if result["is_valid"]:
            valid.append(name)
        else:
            invalid.append(result)
            logger.warning(f"Invalid SAFE name: {name} - {result['error']}")

    return valid, invalid


def deduplicate_safe_names(safe_names: List[str]) -> List[str]:
    """Supprime les doublons d'une liste de noms SAFE."""
    seen = set()
    unique = []

    for name in safe_names:
        if name not in seen:
            seen.add(name)
            unique.append(name)

    return unique


# ── DÉTECTION DU TYPE D'IDENTIFIANT ──────────────────────────────────────
def detect_identifier_type(identifier: str) -> str:
    """Détecte si un identifiant est un SAFE name ou un Product ID."""
    identifier = identifier.strip()

    # FIX 1: Ignorer les lignes d'en-tête comme "Name", "SAFE", etc.
    header_keywords = ["name", "safe", "product", "id", "uuid", "identifier"]
    if identifier.lower() in header_keywords:
        return "header"

    # UUID pattern
    if len(identifier) == 36 and identifier.count("-") == 4:
        parts = identifier.split("-")
        if all(len(p) == 8 or len(p) == 4 or len(p) == 12 for p in parts):
            return "product_id"

    if identifier.startswith(("S1", "S2", "S3", "S4")):
        return "safe_name"

    return "unknown"


# ── CONSTRUCTION DES URLs ──────────────────────────────────────────────────
def create_urls_from_safe_names(
    safe_names: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
    logger: Optional[logging.Logger] = None,
) -> List[Dict]:
    """Construit les URLs OData à partir d'une liste de noms SAFE."""
    if logger is None:
        logger = logging.getLogger(__name__)

    if not safe_names:
        logger.warning("Empty SAFE names list")
        return []

    unique_names = deduplicate_safe_names(safe_names)
    logger.info(
        f"Creating URLs for {len(unique_names)} unique SAFE names (batch size: {batch_size})"
    )

    batches = []

    for i in range(0, len(unique_names), batch_size):
        batch_names = unique_names[i : i + batch_size]
        batch_id = i // batch_size

        name_filters = []
        for name in batch_names:
            escaped_name = name.replace("'", "''")
            name_filters.append(f"Name eq '{escaped_name}'")

        filter_str = " or ".join(name_filters)
        url = f"{ODATA_URL}?$filter={filter_str}&$top={batch_size}&$expand=Attributes"

        url_length = len(url)
        if url_length > 2048:
            logger.warning(
                f"Batch {batch_id} URL length ({url_length} chars) exceeds 2048. "
                f"Consider reducing batch size (currently {len(batch_names)} names)."
            )

        batches.append(
            {
                "url": url,
                "safe_names": batch_names,
                "batch_id": batch_id,
                "num_products": len(batch_names),
                "url_length": url_length,
            }
        )

        logger.debug(
            f"Batch {batch_id}: {len(batch_names)} names, URL length: {url_length}"
        )

    logger.info(f"Created {len(batches)} batch(es)")
    return batches


def create_urls_from_product_ids(
    product_ids: List[str],
    batch_size: int = 50,
    logger: Optional[logging.Logger] = None,
) -> List[Dict]:
    """Construit les URLs OData à partir d'une liste de Product IDs."""
    if logger is None:
        logger = logging.getLogger(__name__)

    if not product_ids:
        logger.warning("Empty Product IDs list")
        return []

    unique_ids = list(set(product_ids))
    logger.info(
        f"Creating URLs for {len(unique_ids)} unique Product IDs (batch size: {batch_size})"
    )

    batches = []

    for i in range(0, len(unique_ids), batch_size):
        batch_ids = unique_ids[i : i + batch_size]
        batch_id = i // batch_size

        id_filters = []
        for pid in batch_ids:
            escaped_pid = pid.replace("'", "''")
            id_filters.append(f"Id eq '{escaped_pid}'")

        filter_str = " or ".join(id_filters)
        url = f"{ODATA_URL}?$filter={filter_str}&$top={batch_size}&$expand=Attributes"

        batches.append(
            {
                "url": url,
                "product_ids": batch_ids,
                "batch_id": batch_id,
                "num_products": len(batch_ids),
                "url_length": len(url),
            }
        )

    logger.info(f"Created {len(batches)} batch(es)")
    return batches


# ── CACHE ──────────────────────────────────────────────────────────────────
def get_cache_filename(url: str, cache_dir: Optional[str] = None) -> str:
    """Génère un nom de fichier de cache basé sur le MD5 de l'URL."""
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    if cache_dir:
        return os.path.join(cache_dir, url_hash + ".json")
    return url_hash + ".json"


# ── RÉCUPÉRATION DES DONNÉES ──────────────────────────────────────────────
@retry_with_backoff(max_retries=5, base_delay=1, max_delay=60)
def _fetch_url_with_retry(url: str, headers: Optional[Dict], timeout: int = 30) -> Dict:
    """Effectue une requête HTTP avec retry et rate limiting."""
    _GLOBAL_RATE_LIMITER.wait_if_needed()
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_metadata_for_batch(
    batch_info: Dict,
    headers: Optional[Dict] = None,
    cache_dir: Optional[str] = None,
    timeout: int = DEFAULT_TIMEOUT,
    logger: Optional[logging.Logger] = None,
) -> Tuple[Dict, Optional[pd.DataFrame]]:
    """Récupère les métadonnées pour un batch."""
    if logger is None:
        logger = logging.getLogger(__name__)

    url = batch_info["url"]
    batch_id = batch_info["batch_id"]

    json_data = None
    cached = False

    if cache_dir:
        cache_file = get_cache_filename(url, cache_dir)
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    json_data = json.load(f)
                cached = True
                logger.debug(f"Batch {batch_id}: Cache hit")
            except Exception as e:
                logger.warning(f"Batch {batch_id}: Failed to read cache: {e}")

    if json_data is None:
        try:
            logger.debug(f"Batch {batch_id}: Fetching from CDSE")
            json_data = _fetch_url_with_retry(url, headers, timeout)
            logger.debug(f"Batch {batch_id}: Successfully fetched")

            if cache_dir:
                os.makedirs(cache_dir, exist_ok=True)
                cache_file = get_cache_filename(url, cache_dir)
                with open(cache_file, "w") as f:
                    json.dump(json_data, f)
                logger.debug(f"Batch {batch_id}: Cached to {cache_file}")

        except requests.exceptions.Timeout:
            logger.error(f"Batch {batch_id}: Timeout after {timeout}s")
            batch_info["error"] = "timeout"
            return batch_info, None
        except requests.exceptions.HTTPError as e:
            logger.error(f"Batch {batch_id}: HTTP error: {e}")
            batch_info["error"] = f"http_error_{e.response.status_code}"
            return batch_info, None
        except Exception as e:
            logger.error(f"Batch {batch_id}: Unexpected error: {e}")
            batch_info["error"] = str(e)
            return batch_info, None

    if "value" in json_data and json_data["value"]:
        df = pd.DataFrame.from_dict(json_data["value"])
        batch_info["num_found"] = len(df)
        batch_info["cached"] = cached
        return batch_info, df
    else:
        logger.debug(f"Batch {batch_id}: No products found")
        batch_info["num_found"] = 0
        batch_info["cached"] = cached
        return batch_info, None


# ── POST-TRAITEMENT ──────────────────────────────────────────────────────
def remove_duplicates(safes_ori: pd.DataFrame) -> pd.DataFrame:
    """Supprime les doublons de produits SAFE basé sur le nom."""
    start_time = time.time()
    safes_sort = safes_ori.sort_values("ModificationDate", ascending=False)
    safes_dedup = safes_sort.drop_duplicates(subset=["Name"])
    processing_time = time.time() - start_time
    nb_duplicate = len(safes_ori) - len(safes_dedup)

    logger = logging.getLogger(__name__)
    logger.info(f"Removed {nb_duplicate} duplicate(s)")
    logger.info(f"remove_duplicates processing time: {processing_time:.1f}s")
    return safes_dedup


def multy_to_poly(collected_data: Optional[pd.DataFrame] = None) -> gpd.GeoDataFrame:
    """Convertit les Footprints WKT en géométries Shapely et unifie les MultiPolygons."""
    start_time = time.time()
    logger = logging.getLogger(__name__)

    if collected_data is None or collected_data.empty:
        logger.warning("No data to convert")
        return gpd.GeoDataFrame()

    # Extraire la géométrie du Footprint
    collected_data["geometry"] = (
        collected_data["Footprint"].str.split(";", expand=True)[1].str.strip().str[:-1]
    )
    collected_data["geometry"] = gpd.GeoSeries.from_wkt(collected_data["geometry"])
    collected_data = gpd.GeoDataFrame(
        collected_data, geometry="geometry", crs="EPSG:4326"
    )

    # Convertir MultiPolygon en Polygon (unary_union)
    collected_data["geometry"] = collected_data["geometry"].apply(
        lambda geo: unary_union(geo.geoms) if geo.geom_type == "MultiPolygon" else geo
    )
    collected_data = gpd.GeoDataFrame(
        collected_data, geometry="geometry", crs="EPSG:4326"
    )
    collected_data.dropna(subset=["Id"], inplace=True)

    processing_time = time.time() - start_time
    logger.info(f"multy_to_poly processing time: {processing_time:.1f}s")
    return collected_data


def post_process_data(
    df: pd.DataFrame, logger: Optional[logging.Logger] = None
) -> gpd.GeoDataFrame:
    """
    Applique tout le post-traitement aux données récupérées.

    Étapes:
    1. remove_duplicates()
    2. multy_to_poly()
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if df is None or df.empty:
        logger.warning("No data to post-process")
        return gpd.GeoDataFrame()

    logger.info(f"Starting post-processing on {len(df)} products")

    # 1. Supprimer les doublons
    df_dedup = remove_duplicates(df)

    # 2. Convertir les géométries
    gdf = multy_to_poly(df_dedup)

    logger.info(f"Post-processing complete: {len(gdf)} products")
    return gdf


# ── ORCHESTRATION PRINCIPALE ──────────────────────────────────────────────
def fetch_all_batches(
    batches: List[Dict],
    headers: Optional[Dict] = None,
    cache_dir: Optional[str] = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    timeout: int = DEFAULT_TIMEOUT,
    logger: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, Dict]:
    """Exécute toutes les requêtes en parallèle avec barre de progression."""
    if logger is None:
        logger = logging.getLogger(__name__)

    stats = {
        "total_batches": len(batches),
        "successful": 0,
        "failed": 0,
        "cached": 0,
        "products_found": 0,
        "products_expected": sum(b["num_products"] for b in batches),
        "errors": [],
        "elapsed_time": 0.0,
    }

    if not batches:
        logger.warning("No batches to process")
        return pd.DataFrame(), stats

    all_dfs = []
    start_time = time.time()

    with tqdm(total=len(batches), desc="Fetching batches", unit="batch") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    fetch_metadata_for_batch, batch, headers, cache_dir, timeout, logger
                ): batch
                for batch in batches
            }

            for future in as_completed(futures):
                batch = futures[future]
                try:
                    updated_batch, df = future.result()

                    if df is not None and not df.empty:
                        all_dfs.append(df)
                        stats["successful"] += 1
                        stats["products_found"] += len(df)
                    else:
                        stats["failed"] += 1
                        if "error" in updated_batch:
                            stats["errors"].append(
                                {
                                    "batch_id": updated_batch["batch_id"],
                                    "error": updated_batch["error"],
                                }
                            )

                    if updated_batch.get("cached", False):
                        stats["cached"] += 1

                except Exception as e:
                    logger.error(
                        f"Batch {batch.get('batch_id', 'unknown')} failed: {e}"
                    )
                    stats["failed"] += 1
                    stats["errors"].append(
                        {"batch_id": batch.get("batch_id", "unknown"), "error": str(e)}
                    )

                pbar.update(1)
                pbar.set_postfix(
                    {
                        "found": stats["products_found"],
                        "ok": stats["successful"],
                        "fail": stats["failed"],
                        "cache": stats["cached"],
                    }
                )

    stats["elapsed_time"] = time.time() - start_time

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(f"Total products found: {len(final_df)}")
        return final_df, stats
    else:
        logger.warning("No products found in any batch")
        return pd.DataFrame(), stats


# ── LOAD INPUT LIST ──────────────────────────────────────────────────────
def load_listing(filepath: str, logger: logging.Logger) -> List[str]:
    """Charge une liste d'identifiants depuis un fichier texte."""
    path = Path(filepath)
    if not path.exists():
        logger.error(f"Input listing file not found: {filepath}")
        raise FileNotFoundError(f"File not found: {filepath}")

    lines = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    logger.info(f"Loaded {len(lines)} lines from {filepath}")
    return lines


# ── CHECKPOINT SYSTEM ────────────────────────────────────────────────────
def save_checkpoint(processed_ids: List[str], checkpoint_file: Path):
    """Sauvegarde les IDs déjà traités."""
    with checkpoint_file.open("w") as f:
        for pid in processed_ids:
            f.write(f"{pid}\n")


def load_checkpoint(checkpoint_file: Path) -> Set[str]:
    """Charge les IDs déjà traités depuis un checkpoint."""
    if not checkpoint_file.exists():
        return set()
    with checkpoint_file.open("r") as f:
        return {line.strip() for line in f if line.strip()}


# ── CLI ──────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Get metadata for SAFE products from CDSE OData API.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From a file with SAFE names or Product IDs
  cdsodatacli get-products-metadata --input-listing safes.txt -o results.csv

  # With checkpoint
  cdsodatacli get-products-metadata --input-listing big_list.txt --checkpoint-dir ./ckpt -o results.csv

  # With cache
  cdsodatacli get-products-metadata --input-listing safes.txt --cache-dir ./cache -o results.csv

  # With authentication
  cdsodatacli get-products-metadata --input-listing safes.txt --email user@email.com --password pass -o results.csv
        """,
    )

    # Source des identifiants
    parser.add_argument(
        "--input-listing",
        required=True,
        metavar="FILE",
        help="Path to a text file with one SAFE name or Product UUID per line",
    )

    # Arguments de contrôle
    parser.add_argument(
        "--output", "-o", required=True, metavar="FILE", help="Output CSV file path"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of SAFE names per OData query (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Maximum parallel threads (default: {DEFAULT_MAX_WORKERS})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )

    # Arguments de résilience
    parser.add_argument(
        "--cache-dir", metavar="DIR", help="Directory for caching OData responses"
    )
    parser.add_argument("--no-cache", action="store_true", help="Disable caching")
    parser.add_argument(
        "--checkpoint-dir",
        metavar="DIR",
        help="Directory for checkpoint files (resume interrupted runs)",
    )

    # Authentification
    parser.add_argument("--email", help="CDSE account email")
    parser.add_argument("--password", help="CDSE account password")

    # Debug
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    parser.add_argument(
        "--dev", action="store_true", help="Dev mode: limit to first 5 identifiers"
    )

    return parser.parse_args()


# ── MAIN ──────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    logger = setup_logger(verbose=args.verbose)

    # Variables pour le checkpoint
    checkpoint_file = None

    try:
        # 1. Charger les identifiants
        raw_lines = load_listing(args.input_listing, logger)

        # FIX 1: Filtrer les lignes d'en-tête
        identifiers = []
        headers_skipped = 0
        for line in raw_lines:
            id_type = detect_identifier_type(line)
            if id_type == "header":
                headers_skipped += 1
                logger.debug(f"Skipping header line: {line}")
            else:
                identifiers.append(line)

        if headers_skipped > 0:
            logger.info(f"Skipped {headers_skipped} header line(s)")

        # Dev mode
        if args.dev:
            identifiers = identifiers[:5]
            logger.info(f"Dev mode: limited to {len(identifiers)} identifiers")

        # 2. Détecter le type et filtrer
        safe_names = []
        product_ids = []
        unknown = []

        for identifier in identifiers:
            id_type = detect_identifier_type(identifier)
            if id_type == "safe_name":
                safe_names.append(identifier)
            elif id_type == "product_id":
                product_ids.append(identifier)
            else:
                unknown.append(identifier)
                logger.warning(f"Unknown identifier type: {identifier}")

        if unknown:
            logger.warning(f"{len(unknown)} identifier(s) could not be recognized")

        # 3. Valider les SAFE names
        valid_safes, invalid_safes = validate_safe_names(safe_names, logger)

        if invalid_safes:
            logger.warning(f"{len(invalid_safes)} SAFE name(s) invalid")

        if not valid_safes and not product_ids:
            logger.error("No valid identifiers to process")
            sys.exit(1)

        # 4. Construire les URLs
        batches = []

        if valid_safes:
            logger.info(f"Processing {len(valid_safes)} valid SAFE names")
            safe_batches = create_urls_from_safe_names(
                valid_safes, args.batch_size, logger
            )
            batches.extend(safe_batches)

        if product_ids:
            logger.info(f"Processing {len(product_ids)} Product IDs")
            id_batches = create_urls_from_product_ids(
                product_ids, args.batch_size * 3, logger
            )
            batches.extend(id_batches)

        if not batches:
            logger.error("No batches created")
            sys.exit(1)

        # 5. Checkpoint (si activé)
        processed_set = set()
        if args.checkpoint_dir:
            checkpoint_path = Path(args.checkpoint_dir)
            checkpoint_path.mkdir(parents=True, exist_ok=True)
            checkpoint_file = (
                checkpoint_path / f"{Path(args.output).stem}_checkpoint.txt"
            )
            processed_set = load_checkpoint(checkpoint_file)
            if processed_set:
                logger.info(
                    f"Loaded checkpoint: {len(processed_set)} products already processed"
                )
            else:
                logger.info("No checkpoint found, starting fresh")

        # 6. Authentification
        headers = None
        if args.email and args.password:
            logger.info(f"Authenticating as {args.email}...")
            headers = get_access_token(args.email, args.password)
            logger.info("Authentication successful")

        # 7. Exécuter les requêtes
        cache_dir = None if args.no_cache else args.cache_dir
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)
            logger.info(f"Cache directory: {cache_dir}")

        logger.info(f"Fetching data with {args.max_workers} workers...")
        df, stats = fetch_all_batches(
            batches=batches,
            headers=headers,
            cache_dir=cache_dir,
            max_workers=args.max_workers,
            timeout=args.timeout,
            logger=logger,
        )

        # 8. Post-traitement (automatique)
        if df is not None and not df.empty:
            logger.info(
                "Applying post-processing (deduplication + geometry conversion)..."
            )
            gdf = post_process_data(df, logger)

            # 9. Sauvegarder en CSV avec géométrie WKT
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Sauvegarder la géométrie en WKT pour le CSV
            gdf["geometry_wkt"] = gdf.geometry.to_wkt()
            gdf.drop(columns=["geometry"], inplace=True)
            gdf.to_csv(output_path, index=False)

            logger.info(f"Results saved to {output_path}")

            # Log des statistiques
            logger.info(
                f"Found {len(gdf)} products out of {stats['products_expected']} expected"
            )
            logger.info(
                f"Stats: {stats['successful']} successful batches, {stats['failed']} failed"
            )
            logger.info(f"Cache hits: {stats['cached']}")
            logger.info(f"Total elapsed time: {stats['elapsed_time']:.2f}s")

            if stats["products_found"] > 0:
                avg_time = stats["elapsed_time"] / stats["products_found"]
                logger.info(f"Average time per product: {avg_time*1000:.1f}ms")
                logger.info(
                    f"Throughput: {stats['products_found']/stats['elapsed_time']:.1f} products/s"
                )

            if stats["errors"]:
                logger.warning(f"Errors: {len(stats['errors'])}")
                for err in stats["errors"][:5]:
                    logger.warning(f"  Batch {err['batch_id']}: {err['error']}")
                if len(stats["errors"]) > 5:
                    logger.warning(f"  ... and {len(stats['errors']) - 5} more errors")

            # Sauvegarder le checkpoint
            if checkpoint_file:
                found_names = gdf["Name"].tolist() if "Name" in gdf.columns else []
                if found_names:
                    save_checkpoint(found_names, checkpoint_file)
                    logger.info(f"Checkpoint saved to {checkpoint_file}")

        else:
            logger.warning("No data retrieved")
            Path(args.output).touch()

        # Résumé final
        logger.info("\n" + "=" * 70)
        logger.info("📊 FINAL SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Total products requested: {stats['products_expected']}")
        logger.info(f"Products found: {stats['products_found']}")
        if stats["products_expected"] > 0:
            logger.info(
                f"Success rate: {stats['products_found']/stats['products_expected']*100:.1f}%"
            )
        logger.info(f"Batches: {stats['successful']} OK, {stats['failed']} failed")
        logger.info(f"Cache hits: {stats['cached']}")
        logger.info(f"Total time: {stats['elapsed_time']:.2f}s")
        if stats["products_found"] > 0:
            logger.info(
                f"Throughput: {stats['products_found']/stats['elapsed_time']:.1f} products/s"
            )
        logger.info("=" * 70)

    except KeyboardInterrupt:
        logger.info("\n⏹️  Interrupted by user")
        if checkpoint_file and "df" in locals() and df is not None and not df.empty:
            found_names = df["Name"].tolist() if "Name" in df.columns else []
            if found_names:
                save_checkpoint(found_names, checkpoint_file)
                logger.info(f"Checkpoint saved to {checkpoint_file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
