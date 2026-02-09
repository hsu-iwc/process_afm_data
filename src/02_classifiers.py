"""
02_classifiers.py — GCBM Classifier Design
============================================
Defines 5 classifiers that uniquely map each stand to its yield curve:
  1. species        — condition Species / spatial DOMSPECLAB
  2. origin         — condition Origin / spatial ORIGIN_CODE
  3. si_class       — SITE_INDEX rounded to nearest 5
  4. growth_period  — current vs post_regen
  5. mgmt_trajectory — T1-X-T2-Y-F1-A-F2-B treatment schedule key

Outputs classifiers.csv in SIT format.
"""

import numpy as np
import pandas as pd

from config import (
    OUTPUT_DIR,
    CLASSIFIER_NAMES,
    SI_CLASS_INTERVAL,
    GROWTH_PERIOD_CURRENT,
    GROWTH_PERIOD_POST_REGEN,
)


def round_si(si_value):
    """Round site index to nearest SI_CLASS_INTERVAL and format as SIxx."""
    if pd.isna(si_value) or si_value == 0:
        return "SI0"
    rounded = int(round(si_value / SI_CLASS_INTERVAL) * SI_CLASS_INTERVAL)
    return f"SI{rounded}"


def assign_classifiers(spatial, condition_initial, yields1):
    """
    Assign classifier values to each stand based on the condition file
    and spatial data.

    Parameters:
        spatial: GeoDataFrame from load_spatial()
        condition_initial: DataFrame of PERIOD=0 rows from load_condition()
        yields1: DataFrame from load_yields1()

    Returns:
        DataFrame with one row per stand and classifier columns.
    """
    print("=" * 60)
    print("02_classifiers: Assigning classifiers to stands")
    print("=" * 60)

    # Start with spatial stand keys
    stands = spatial[["STAND_KEY", "DOMSPECLAB", "ORIGIN_CODE", "SITE_INDEX",
                       "STAND_AGE", "IS_FOREST", "AREA_HA"]].copy()
    stands = stands.rename(columns={"STAND_KEY": "stand_key"})

    # Merge condition initial data to get richer species/origin info
    cond = condition_initial[["stand_key", "Species", "Origin", "GrowType",
                               "SI", "Thin1", "Thin2", "Fert1", "Fert2",
                               "TreatmentType", "ManagementType"]].copy()
    # Drop potential duplicates in condition (shouldn't happen at PERIOD=0)
    cond = cond.drop_duplicates(subset=["stand_key"])

    stands = stands.merge(cond, on="stand_key", how="left")

    # --- Classifier 1: species ---
    # Prefer condition Species over spatial DOMSPECLAB (condition has finer codes like COLB)
    stands["species"] = stands["Species"].fillna(stands["DOMSPECLAB"])

    # --- Classifier 2: origin ---
    # Prefer condition Origin over spatial ORIGIN_CODE
    stands["origin"] = stands["Origin"].fillna(stands["ORIGIN_CODE"])

    # --- Classifier 3: si_class ---
    # Use condition SI where available, else spatial SITE_INDEX
    stands["si_raw"] = stands["SI"].fillna(stands["SITE_INDEX"])
    stands["si_class"] = stands["si_raw"].apply(round_si)

    # --- Classifier 4: growth_period ---
    # All stands start in "current" (initial rotation)
    stands["growth_period"] = GROWTH_PERIOD_CURRENT

    # --- Classifier 5: mgmt_trajectory ---
    # Build from Thin1/Thin2/Fert1/Fert2 in condition (0 if missing)
    for col in ["Thin1", "Thin2", "Fert1", "Fert2"]:
        stands[col] = stands[col].fillna(0).astype(int)

    stands["mgmt_trajectory"] = stands.apply(
        lambda r: f"T1-{r['Thin1']}-T2-{r['Thin2']}-F1-{r['Fert1']}-F2-{r['Fert2']}",
        axis=1,
    )

    # Non-forest stands get NOGROW trajectory
    stands.loc[~stands["IS_FOREST"], "mgmt_trajectory"] = "T1-0-T2-0-F1-0-F2-0"

    # --- Summary ---
    print(f"\n  Stands classified: {len(stands)}")
    print(f"  Unique species: {stands['species'].nunique()} — {sorted(stands['species'].dropna().unique())}")
    print(f"  Unique origins: {stands['origin'].nunique()} — {sorted(stands['origin'].dropna().unique())}")
    print(f"  Unique SI classes: {stands['si_class'].nunique()} — {sorted(stands['si_class'].unique())}")
    print(f"  Unique trajectories: {stands['mgmt_trajectory'].nunique()}")

    return stands


def build_classifier_csv(stands):
    """
    Build classifiers.csv in SIT format.

    SIT classifiers format:
      - Header section listing each classifier and its possible values
      - Then a data section with one row per classifier combo
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "classifiers.csv"

    # Collect unique values per classifier
    classifier_values = {}
    for clf_name in CLASSIFIER_NAMES:
        vals = sorted(stands[clf_name].dropna().unique())
        classifier_values[clf_name] = vals

    # Write the header block
    lines = []
    for i, clf_name in enumerate(CLASSIFIER_NAMES, 1):
        vals = classifier_values[clf_name]
        lines.append(f"_CLASSIFIER,{clf_name}")
        for v in vals:
            lines.append(f"{v},{v}")

    # Write header
    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"\n  Wrote {out_path}")
    print(f"  {len(CLASSIFIER_NAMES)} classifiers defined")
    for clf_name in CLASSIFIER_NAMES:
        print(f"    {clf_name}: {len(classifier_values[clf_name])} values")

    return classifier_values


def run(spatial, condition_initial, yields1):
    """Main entry point."""
    stands = assign_classifiers(spatial, condition_initial, yields1)
    classifier_values = build_classifier_csv(stands)
    return stands, classifier_values


if __name__ == "__main__":
    from ingest_01 import ingest_all
    data = ingest_all()
    stands, clf_vals = run(data["spatial"], data["condition_initial"], data["yields1"])
