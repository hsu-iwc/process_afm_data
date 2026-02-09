"""
04_inventory.py — Starting Inventory Spatial Layer
====================================================
Joins spatial geometry with classifier assignments and initial age
to produce the GCBM starting inventory as a GeoPackage.

Each stand polygon gets:
  species, origin, si_class, growth_period, mgmt_trajectory,
  initial_age, area_ha, delay=0, land_class=FL,
  historical_disturbance_type, last_pass_disturbance_type, geometry
"""

import geopandas as gpd
import pandas as pd

from config import (
    OUTPUT_DIR,
    CLASSIFIER_NAMES,
    HISTORICAL_DISTURBANCE_MAP,
)


def build_inventory(spatial, stands):
    """
    Build starting inventory GeoDataFrame.

    Parameters:
        spatial: GeoDataFrame from load_spatial() — has geometry
        stands: DataFrame from 02_classifiers — has classifier assignments

    Returns:
        GeoDataFrame ready for export
    """
    print("=" * 60)
    print("04_inventory: Building starting inventory spatial layer")
    print("=" * 60)

    # Merge classifier assignments onto spatial geometry
    # stands has stand_key; spatial has STAND_KEY
    inv = spatial[["STAND_KEY", "STAND_AGE", "AREA_HA", "IS_FOREST", "geometry"]].copy()
    inv = inv.rename(columns={"STAND_KEY": "stand_key", "STAND_AGE": "initial_age", "AREA_HA": "area_ha"})

    # stand_key is the merge key AND a classifier; only select non-key classifiers
    non_key_clfs = [c for c in CLASSIFIER_NAMES if c != "stand_key"]
    stands_sub = stands[["stand_key"] + non_key_clfs].drop_duplicates(subset=["stand_key"])

    inv = inv.merge(stands_sub, on="stand_key", how="left")

    # Fixed attributes
    inv["delay"] = 0
    inv["land_class"] = "FL"

    # Historical and last-pass disturbance types based on origin
    inv["historical_disturbance_type"] = inv["origin"].map(
        lambda o: HISTORICAL_DISTURBANCE_MAP.get(o, ("Wildfire", "Wildfire"))[0]
    )
    inv["last_pass_disturbance_type"] = inv["origin"].map(
        lambda o: HISTORICAL_DISTURBANCE_MAP.get(o, ("Wildfire", "Wildfire"))[1]
    )

    # Filter: exclude non-forest stands from GCBM inventory
    forest_inv = inv[inv["IS_FOREST"]].copy()
    nonforest = inv[~inv["IS_FOREST"]]

    print(f"\n  Total stands: {len(inv)}")
    print(f"  Forest (included): {len(forest_inv)}")
    print(f"  Non-forest (excluded): {len(nonforest)}")

    # Select output columns (stand_key is already in CLASSIFIER_NAMES)
    out_cols = (
        CLASSIFIER_NAMES +
        ["initial_age", "area_ha", "delay", "land_class",
         "historical_disturbance_type", "last_pass_disturbance_type",
         "geometry"]
    )
    forest_inv = forest_inv[out_cols]

    return forest_inv


def write_inventory(gdf):
    """Write inventory to GeoPackage."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "inventory.gpkg"
    gdf.to_file(out_path, driver="GPKG")
    print(f"\n  Wrote {out_path} ({len(gdf)} features)")
    return out_path


def run(spatial, stands):
    """Main entry point."""
    inv = build_inventory(spatial, stands)
    write_inventory(inv)
    return inv


if __name__ == "__main__":
    from _01_ingest import ingest_all
    from _02_classifiers import run as run_classifiers
    data = ingest_all()
    stands, _ = run_classifiers(data["spatial"], data["condition_initial"], data["yields1"])
    run(data["spatial"], stands)
