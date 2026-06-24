#!/usr/bin/env python3
"""
Sentinel-1 Product Matchup Tool - Version optimisée avec rate limiting et checkpoint.
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
from cdsodatacli.product_parser import ExplodeSAFE
from cdsodatacli.rate_limiter import RateLimiter
from cdsodatacli.retry import retry_with_backoff

# ── CONFIGURATION ────────────────────────────────────────────────────────────
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
VALID_PRODUCT_TYPES = ["GRDH", "GRDM", "SLC_", "OCN_", "RAW_"]
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
        leveltarget = "2" if target_type == "OCN_" else "1"
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

        # Stratégie de matching (inchangée)
        start_time_str = source_start.strftime("%Y%m%dT%H%M%S")
        exact = next((p for p in products if start_time_str in p["Name"]), None)

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
) -> list[dict]:
    """Version avec progress bar, checkpoint et rate limiting."""

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

    # Sauvegarde finale du checkpoint
    if checkpoint_file:
        save_checkpoint(list(processed), checkpoint_file)
        logger.info(f"Checkpoint saved to {checkpoint_file}")

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

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match Sentinel-1 SAFE products to a given product type via OData.",
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
    )


if __name__ == "__main__":
    main()
