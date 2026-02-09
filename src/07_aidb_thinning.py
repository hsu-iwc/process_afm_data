"""
07_aidb_thinning.py — Add Thinning Disturbance Matrices to AIDB
================================================================
For each unique thinning removal percentage found in the disturbance events,
creates a scaled disturbance matrix in the AIDB using the existing
aidb_disturbance_manager module.

Disturbance type names follow: "XX.XX% commercial thinning"
(e.g., "34.72% commercial thinning")

Usage:
    python 07_aidb_thinning.py --aidb-path /path/to/aidb.accdb [--dry-run]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Import the existing AIDB manager from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from aidb_disturbance_manager import ensure_disturbances_exist

from config import OUTPUT_DIR


def get_unique_thinning_pcts(events_or_csv):
    """
    Extract unique thinning removal percentages from disturbance events.

    Parameters:
        events_or_csv: DataFrame of disturbance events, or path to disturbance_events.csv

    Returns:
        sorted list of unique percentages (as floats, e.g. 34.72)
    """
    if isinstance(events_or_csv, (str, Path)):
        events = pd.read_csv(events_or_csv)
    else:
        events = events_or_csv

    thin_events = events[events["disturbance_type"].isin(["1st_Thin", "2nd_Thin"])]
    pcts = thin_events["pct_volume_removed"].dropna().unique()
    return sorted(pcts)


def build_disturbance_specs(pcts):
    """
    Build disturbance specs list for ensure_disturbances_exist().

    Each thinning percentage becomes a "XX.XX% commercial thinning" entry.
    Also includes standard types (clearcut, planting, site prep).
    """
    specs = []

    # Standard disturbance types (should already exist in AIDB)
    specs.append({"name": "97% clear-cut"})
    specs.append({"name": "Planting"})

    # Thinning disturbance types
    for pct in pcts:
        name = f"{pct:.2f}% commercial thinning"
        specs.append({
            "name": name,
            "percent": pct / 100.0,  # Convert to decimal
            "category": "commercial",
        })

    return specs


def run(aidb_path, events_or_csv=None, dry_run=False):
    """
    Main entry point.

    Parameters:
        aidb_path: Path to AIDB .mdb or .accdb file
        events_or_csv: DataFrame or path to disturbance_events.csv
                       If None, reads from OUTPUT_DIR/disturbance_events.csv
        dry_run: If True, only report what would be created
    """
    print("=" * 60)
    print("07_aidb_thinning: Adding thinning disturbance matrices to AIDB")
    print("=" * 60)

    if events_or_csv is None:
        events_or_csv = OUTPUT_DIR / "disturbance_events.csv"

    pcts = get_unique_thinning_pcts(events_or_csv)
    print(f"\n  Unique thinning removal %: {len(pcts)}")
    for p in pcts:
        print(f"    {p:.2f}%")

    specs = build_disturbance_specs(pcts)
    print(f"\n  Disturbance specs to process: {len(specs)}")

    result = ensure_disturbances_exist(
        db_path=str(aidb_path),
        disturbance_specs=specs,
        dry_run=dry_run,
    )

    # Build mapping: pct -> disturbance type name (for use in other modules)
    pct_to_dist_name = {}
    for pct in pcts:
        name = f"{pct:.2f}% commercial thinning"
        pct_to_dist_name[pct] = name

    # Write mapping CSV
    mapping_df = pd.DataFrame([
        {"pct_volume_removed": pct, "disturbance_type_name": name}
        for pct, name in pct_to_dist_name.items()
    ])
    mapping_path = OUTPUT_DIR / "thinning_disturbance_mapping.csv"
    mapping_df.to_csv(mapping_path, index=False)
    print(f"\n  Wrote thinning-to-disturbance mapping: {mapping_path}")

    return result, pct_to_dist_name


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add thinning disturbances to AIDB")
    parser.add_argument("--aidb-path", required=True, help="Path to AIDB .mdb or .accdb file")
    parser.add_argument("--dry-run", action="store_true", help="Report without modifying AIDB")
    parser.add_argument("--events-csv", default=None, help="Path to disturbance_events.csv")
    args = parser.parse_args()

    run(
        aidb_path=args.aidb_path,
        events_or_csv=args.events_csv,
        dry_run=args.dry_run,
    )
