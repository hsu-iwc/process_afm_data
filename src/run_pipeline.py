"""
run_pipeline.py — Full GCBM Input Processing Pipeline
======================================================
Runs steps 01-08 in sequence, passing data between modules.

Usage:
    python run_pipeline.py [--aidb-path /path/to/aidb.accdb] [--dry-run] [--skip-aidb]
"""

import argparse
import sys
from pathlib import Path

# Ensure src/ is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import OUTPUT_DIR


def main(aidb_path=None, dry_run=False, skip_aidb=False):
    print("=" * 60)
    print("IWC Boothill — GCBM Input Processing Pipeline")
    print("=" * 60)

    # Step 1: Ingest all source data
    from importlib import import_module
    ingest = import_module("01_ingest")
    data = ingest.ingest_all()

    # Step 2: Assign classifiers
    classifiers = import_module("02_classifiers")
    stands, classifier_values = classifiers.run(
        data["spatial"], data["condition_initial"], data["yields1"]
    )

    # Step 3: Build yield curves
    yield_curves = import_module("03_yield_curves")
    curves = yield_curves.run(
        stands, data["yields1"], data["yields2"], data["yields3"]
    )

    # Step 4: Build starting inventory
    inventory = import_module("04_inventory")
    inv = inventory.run(data["spatial"], stands)

    # Step 5: Build disturbance layers
    disturbances = import_module("05_disturbances")
    events, events_geo = disturbances.run(
        data["schedule"], data["spatial"], data["yields1"], data["yields3"], data["yields2"],
        condition_initial=data["condition_initial"],
    )

    # Step 6: Build transition rules
    transitions = import_module("06_transitions")
    rules = transitions.run(events, stands)

    # Step 7: Add thinning disturbances to AIDB (optional)
    if not skip_aidb:
        if aidb_path is None:
            print("\n" + "=" * 60)
            print("07_aidb_thinning: SKIPPED (no --aidb-path provided)")
            print("  Run separately: python 07_aidb_thinning.py --aidb-path <path>")
            print("=" * 60)
        else:
            aidb_mod = import_module("07_aidb_thinning")
            aidb_mod.run(
                aidb_path=aidb_path,
                events_or_csv=events,
                dry_run=dry_run,
            )
    else:
        print("\n07_aidb_thinning: SKIPPED (--skip-aidb)")

    # Step 8: Generate tiler config
    tiler = import_module("08_tiler_config")
    tiler.run()

    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"Outputs in: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IWC Boothill GCBM Input Pipeline")
    parser.add_argument("--aidb-path", default=None, help="Path to AIDB .mdb or .accdb file")
    parser.add_argument("--dry-run", action="store_true", help="AIDB: report without modifying")
    parser.add_argument("--skip-aidb", action="store_true", help="Skip AIDB step entirely")
    args = parser.parse_args()

    main(
        aidb_path=args.aidb_path,
        dry_run=args.dry_run,
        skip_aidb=args.skip_aidb,
    )
