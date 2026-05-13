#!/usr/bin/env python3
"""
Sentinel-1 Product Matchup Tool
Finds a target product type for a given list of Sentinel-1 SAFE product IDs.
"""

import requests
import time
from tqdm import tqdm
import logging
import argparse
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from cdsodatacli.product_parser import ExplodeSAFE

# ── CONFIGURATION ────────────────────────────────────────────────────────────
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

VALID_PRODUCT_TYPES = ["GRDH", "GRDM", "SLC_", "OCN_", "RAW_"]


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
    except (IndexError, ValueError, AttributeError):  # Added AttributeError
        return None


# def parse_safe_info(product_name: str):
#     """Retourne (start_datetime, datatake_hex) ou (None, None) en cas d'échec."""
#     try:
#         inst = ExplodeSAFE(product_name)
#         # Exemple d'attributs disponibles (à adapter selon votre lib)
#         return inst.startdate, getattr(inst, 'mission_data_take', None)
#     except Exception:
#         return None, None


MAX_DELTA_SECONDS = 8


def closest_in_time(
    reference_dt: datetime, candidates: list[dict]
) -> tuple[dict, float]:
    """
    Return (candidate, delta_seconds) for the product closest in time to reference_dt.

    Args:
        reference_dt (datetime): The reference datetime to compare against.
        candidates (list of dict): List of product dicts, each with a "Name" key containing the product name to parse for datetime.
    Returns:
        tuple: (best_candidate_dict, delta_seconds) where best_candidate_dict is the dict from candidates with the closest datetime, and delta_seconds is the absolute difference in seconds between reference_dt and the

    """

    def time_delta(prod):
        # dt = parse_start_time(prod["Name"])
        tmpinst = ExplodeSAFE(prod["Name"])
        dt = tmpinst.startdate
        # dt, _ = parse_safe_info(prod["Name"])
        if dt is None:
            return float("inf")
        return abs((dt - reference_dt).total_seconds())

    best = min(candidates, key=time_delta)
    return best, time_delta(best)


# ── CORE LOGIC ───────────────────────────────────────────────────────────────
def find_product_for_safe(
    source_id: str,
    target_type: str,
    logger: logging.Logger,
    delta_distribution: defaultdict,
) -> dict:
    """
    Finds a target-type product for a given source Sentinel-1 product ID.
    Uses DataTake ID as the primary anchor, then exact timestamp match,
    then closest-in-time as fallback.
    """
    try:
        inst = ExplodeSAFE(source_id)
        source_start = inst.startdate
        source_datatake = inst.mission_data_take
        source_absolute_orbit = inst.absolute_orbit_number
        if source_start is None or source_datatake is None:
            logger.error("Failed to parse start date or datatake from %s", source_id)
            return {
                "source_id": source_id,
                "status": "error",
                "note": "Impossible d'extraire les infos du nom",
            }

        platform = source_id.split("_")[0]  # ou inst.platform
        type_token = target_type.rstrip("_").ljust(4, "_")
        pol_full = f"{inst.level}S{inst.polarisation}"
        query_filter = (
            f"startswith(Name,'{platform}') and "
            f"contains(Name,'_{type_token}') and "
            f"contains(Name,'_{source_absolute_orbit}_{source_datatake}') and "
            f"contains(Name,'_{pol_full}') and "
            f"not contains(Name,'_COG')"
        )
        # ─────────────────────────────────────────────────────────────────────
        params = {"$filter": query_filter, "$top": 50}

        logger.debug("OData query filter: %s", query_filter)

        resp = requests.get(ODATA_URL, params=params, timeout=30)
        if resp.status_code != 200:
            return {
                "source_id": source_id,
                "status": "error",
                "note": f"HTTP {resp.status_code}: {resp.text[:200]}",
            }

        products = resp.json().get("value", [])

        if not products:
            logger.debug(
                "No products found for %s with filter: %s", source_id, query_filter
            )
            return {
                "source_id": source_id,
                "status": "not_found",
                "note": f"No {target_type} product found for DataTake {source_datatake}",
            }

        # ── Match strategy ───────────────────────────────────────────────────
        # 1. Exact start-time match (same slice)
        start_time_str = source_start.strftime("%Y%m%dT%H%M%S")
        exact = next((p for p in products if start_time_str in p["Name"]), None)

        if exact:
            match = exact
            match_method = "exact_timestamp"
            delta_distribution[0] += 1

        else:
            # 2. Closest-in-time within the same DataTake
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
                "No exact timestamp match; picked closest product " "(delta=%.0fs): %s",
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


# ── ENTRYPOINT ───────────────────────────────────────────────────────────────
def entrypoint(
    safe_list: list[str],
    target_type: str,
    output_filename: str,
    logger: logging.Logger,
) -> list[dict]:
    logger.info(
        "Starting matchup: %d product(s) → target type '%s'",
        len(safe_list),
        target_type,
    )
    results = []
    delta_distribution: defaultdict[int, int] = defaultdict(int)
    pbar = tqdm(safe_list, desc="Matching products", unit="product")
    try:

        for safe_id in pbar:
            pbar.set_description(f" match  delta sec count: {delta_distribution}")
            safe_id = safe_id.strip()
            if not safe_id:
                continue

            logger.debug("Processing %s", safe_id)
            res = find_product_for_safe(
                safe_id, target_type, logger, delta_distribution
            )
            results.append(res)

            if "target_name" in res:
                logger.debug(
                    "  ✓ Found (%s): %s  [%.1f MB]",
                    res["match_method"],
                    res["target_name"],
                    res["size_mb"],
                )
            else:
                logger.debug(
                    "  ✗ Failed for %s — %s", safe_id, res.get("note", "unknown error")
                )

            time.sleep(0.5)  # Be polite to the OData endpoint

    except KeyboardInterrupt:
        logger.warning(
            "Interrupted by user after %d product(s) processed.", len(results)
        )

    # ── Write results ────────────────────────────────────────────────────────
    output_path = Path(output_filename)
    with output_path.open("w") as fh:
        for r in results:
            if "target_name" in r:
                fh.write(f"{r['target_name']}\n")
            else:
                fh.write(f"# NOT_FOUND: {r['source_id']} — {r.get('note', '')}\n")

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

    # ── Delta distribution ────────────────────────────────────────────────────
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

  # Enable verbose/debug logging
  python s1_matchup.py --prodtype OCN_ --input-listing list.txt --verbose
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
    )


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
