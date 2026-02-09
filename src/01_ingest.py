"""
01_ingest.py — Data Ingestion & Validation
==========================================
Loads all source data into a consistent internal format and runs
cross-source validation checks.

Outputs a dict of DataFrames accessible to downstream modules.
"""

import re
import sys
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd

from config import (
    SHAPEFILE,
    YIELDS1_CSV,
    YIELDS2_CSV,
    YIELDS3_CSV,
    CONDITION_XLSX,
    SCHEDULE_XLSX,
    SCHEDULE_SHEET,
    SCHEDULE_COLUMNS,
    ACRES_TO_HA,
    ORIGIN_LONG_TO_CODE,
    NON_DISTURBANCE_ACTIONS,
    ACTION_TO_DISTURBANCE,
)


# =============================================================================
# SPATIAL DATA
# =============================================================================

def load_spatial(path=SHAPEFILE):
    """Load the shapefile, compute AREA_HA, flag non-forest stands."""
    # pyogrio handles the 0000/00/00 date issue that trips fiona
    gdf = gpd.read_file(path, engine="pyogrio")

    # Validate CRS
    if gdf.crs is None:
        raise ValueError("Shapefile has no CRS defined")
    if gdf.crs.to_epsg() != 4326:
        print(f"  WARNING: CRS is {gdf.crs}, expected EPSG:4326. Reprojecting.")
        gdf = gdf.to_crs(epsg=4326)

    # Compute area in hectares
    gdf["AREA_HA"] = gdf["GIS_AREA"] * ACRES_TO_HA

    # Map long-form ORIGIN to short code (PY/NN/ONO)
    gdf["ORIGIN_CODE"] = gdf["ORIGIN"].map(ORIGIN_LONG_TO_CODE)

    # Flag non-forest stands (DOM_SPEC='Undefined' / DOMSPECLAB='UD', typically age/SI=0)
    gdf["IS_FOREST"] = ~(
        (gdf["DOMSPECLAB"] == "UD")
        | (gdf["ORIGIN"] == "Open")
    )

    print(f"  Spatial: {len(gdf)} stands loaded")
    print(f"    Forest: {gdf['IS_FOREST'].sum()}, Non-forest: {(~gdf['IS_FOREST']).sum()}")
    print(f"    CRS: {gdf.crs}")
    return gdf


# =============================================================================
# YIELD CSV PARSING
# =============================================================================

def _parse_iwc_id(iwc_id):
    """
    Parse an iwc_id string into components.

    Yields1/Yields3 format:
        BH1427-1-1-TPA-XX-BA-XX-T1-0-T2-0-F1-0-F2-0
        -> stand_key=BH1427-1-1, thin1=0, thin2=0, fert1=0, fert2=0

    Yields2 (regen) format:
        SI100-1-U-LB-TPA-XX-BA-XX-T1-14-T2-19-F1-15-F2-20
        -> si_value=100, species_code=LB, thin1=14, thin2=19, fert1=15, fert2=20
    """
    parts = iwc_id.split("-")

    # Extract T1, T2, F1, F2 from the tail
    # Find indices of these markers
    t1_idx = parts.index("T1")
    t2_idx = parts.index("T2")
    f1_idx = parts.index("F1")
    f2_idx = parts.index("F2")

    thin1 = int(parts[t1_idx + 1])
    thin2 = int(parts[t2_idx + 1])
    fert1 = int(parts[f1_idx + 1])
    fert2 = int(parts[f2_idx + 1])

    result = {
        "thin1": thin1,
        "thin2": thin2,
        "fert1": fert1,
        "fert2": fert2,
    }

    # Determine if this is a regen curve (SI prefix) or stand-specific
    if parts[0].startswith("SI"):
        result["si_value"] = int(parts[0][2:])
        # Species code is after the stocking indicator (U)
        # Format: SI100-1-U-LB-TPA-...
        result["species_code"] = parts[3]
        result["stand_key"] = None
        result["is_regen"] = True
    else:
        # Stand-specific: BH1427-1-1-TPA-...
        # stand_key is everything before TPA
        tpa_idx = parts.index("TPA")
        result["stand_key"] = "-".join(parts[:tpa_idx])
        result["is_regen"] = False

    return result


def _fix_pipe_values(df, age_cols):
    """Handle pipe-delimited values in yield columns (take first value = pre-thin)."""
    for col in age_cols:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda x: float(str(x).split("|")[0]) if pd.notna(x) and "|" in str(x) else x
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_yields1(path=YIELDS1_CSV):
    """Load Yields1 (stand-specific base yield curves)."""
    df = pd.read_csv(path)
    age_cols = [str(i) for i in range(1, 79)]

    # Parse iwc_id components
    parsed = df["iwc_id"].apply(_parse_iwc_id)
    df["stand_key"] = parsed.apply(lambda x: x["stand_key"])
    df["thin1"] = parsed.apply(lambda x: x["thin1"])
    df["thin2"] = parsed.apply(lambda x: x["thin2"])
    df["fert1"] = parsed.apply(lambda x: x["fert1"])
    df["fert2"] = parsed.apply(lambda x: x["fert2"])

    # Build a trajectory string for matching
    df["mgmt_trajectory"] = df.apply(
        lambda r: f"T1-{r['thin1']}-T2-{r['thin2']}-F1-{r['fert1']}-F2-{r['fert2']}", axis=1
    )

    print(f"  Yields1: {len(df)} rows, {df['iwc_id'].nunique()} unique iwc_ids, "
          f"{df['stand_key'].nunique()} unique stands")
    return df


def load_yields2(path=YIELDS2_CSV):
    """Load Yields2 (regeneration SI-based yield curves)."""
    df = pd.read_csv(path)
    age_cols = [str(i) for i in range(1, 51)]

    parsed = df["iwc_id"].apply(_parse_iwc_id)
    df["si_value"] = parsed.apply(lambda x: x.get("si_value"))
    df["species_code"] = parsed.apply(lambda x: x.get("species_code"))
    df["thin1"] = parsed.apply(lambda x: x["thin1"])
    df["thin2"] = parsed.apply(lambda x: x["thin2"])
    df["fert1"] = parsed.apply(lambda x: x["fert1"])
    df["fert2"] = parsed.apply(lambda x: x["fert2"])

    df["mgmt_trajectory"] = df.apply(
        lambda r: f"T1-{r['thin1']}-T2-{r['thin2']}-F1-{r['fert1']}-F2-{r['fert2']}", axis=1
    )

    print(f"  Yields2: {len(df)} rows, {df['iwc_id'].nunique()} unique iwc_ids")
    print(f"    SI values: {sorted(df['si_value'].unique())}")
    print(f"    Species: {sorted(df['species_code'].dropna().unique())}")
    return df


def load_yields3(path=YIELDS3_CSV):
    """Load Yields3 (thinning simulation yield curves)."""
    df = pd.read_csv(path)
    age_cols = [str(i) for i in range(1, 79)]

    # Fix pipe-delimited values
    df = _fix_pipe_values(df, age_cols)

    parsed = df["iwc_id"].apply(_parse_iwc_id)
    df["stand_key"] = parsed.apply(lambda x: x["stand_key"])
    df["thin1"] = parsed.apply(lambda x: x["thin1"])
    df["thin2"] = parsed.apply(lambda x: x["thin2"])
    df["fert1"] = parsed.apply(lambda x: x["fert1"])
    df["fert2"] = parsed.apply(lambda x: x["fert2"])

    df["mgmt_trajectory"] = df.apply(
        lambda r: f"T1-{r['thin1']}-T2-{r['thin2']}-F1-{r['fert1']}-F2-{r['fert2']}", axis=1
    )

    print(f"  Yields3: {len(df)} rows, {df['iwc_id'].nunique()} unique iwc_ids, "
          f"{df['stand_key'].nunique()} unique stands")
    return df


# =============================================================================
# CONDITION FILE
# =============================================================================

def load_condition(path=CONDITION_XLSX):
    """Load condition file, extract period-0 initial conditions."""
    df = pd.read_excel(path, sheet_name="Condition")

    # Rename StandID -> stand_key for consistency
    df = df.rename(columns={"StandID": "stand_key"})

    # Extract period 0 as initial conditions
    initial = df[df["PERIOD"] == 0].copy()

    print(f"  Condition: {len(df)} rows, {df['stand_key'].nunique()} unique stands")
    print(f"    Species: {sorted(df['Species'].unique())}")
    print(f"    Origins: {sorted(df['Origin'].unique())}")
    print(f"    Period 0 rows (initial conditions): {len(initial)}")
    return df, initial


# =============================================================================
# MANAGEMENT SCHEDULE
# =============================================================================

def load_schedule(path=SCHEDULE_XLSX, sheet=SCHEDULE_SHEET):
    """Load management schedule, build event table."""
    df = pd.read_excel(path, sheet_name=sheet)

    # Rename theme columns to semantic names
    df = df.rename(columns=SCHEDULE_COLUMNS)

    # Separate disturbance actions from non-disturbance actions
    df["is_disturbance"] = ~df["ACTION"].isin(NON_DISTURBANCE_ACTIONS)
    df["disturbance_type"] = df["ACTION"].map(ACTION_TO_DISTURBANCE)

    n_dist = df["is_disturbance"].sum()
    n_non = (~df["is_disturbance"]).sum()

    print(f"  Schedule: {len(df)} rows, {df['stand_key'].nunique()} unique stands")
    print(f"    Disturbance actions: {n_dist} ({df[df['is_disturbance']]['ACTION'].value_counts().to_dict()})")
    print(f"    Non-disturbance actions: {n_non} ({df[~df['is_disturbance']]['ACTION'].value_counts().to_dict()})")
    print(f"    Year range: {df['YEAR'].min()}-{df['YEAR'].max()}")
    return df


# =============================================================================
# CROSS-SOURCE VALIDATION
# =============================================================================

def validate(spatial, yields1, condition_initial, schedule):
    """Run cross-source validation checks."""
    print("\n=== VALIDATION REPORT ===\n")
    issues = 0

    spatial_keys = set(spatial["STAND_KEY"])
    forest_keys = set(spatial[spatial["IS_FOREST"]]["STAND_KEY"])
    yields1_keys = set(yields1["stand_key"].unique())
    condition_keys = set(condition_initial["stand_key"].unique())
    schedule_keys = set(schedule["stand_key"].unique())

    # 1. Forest stands in spatial but missing from yields1
    missing_yields = forest_keys - yields1_keys
    if missing_yields:
        print(f"  [WARN] {len(missing_yields)} forest stands in spatial but missing from Yields1:")
        for k in sorted(missing_yields)[:10]:
            print(f"         {k}")
        if len(missing_yields) > 10:
            print(f"         ... and {len(missing_yields) - 10} more")
        issues += 1
    else:
        print("  [OK] All forest stands have Yields1 curves")

    # 2. Stands in schedule but missing from spatial
    missing_spatial = schedule_keys - spatial_keys
    if missing_spatial:
        print(f"  [WARN] {len(missing_spatial)} stands in schedule but missing from spatial:")
        for k in sorted(missing_spatial):
            print(f"         {k}")
        issues += 1
    else:
        print("  [OK] All schedule stands found in spatial")

    # 3. Stands in spatial but missing from condition
    missing_condition = spatial_keys - condition_keys
    if missing_condition:
        print(f"  [INFO] {len(missing_condition)} stands in spatial but missing from condition:")
        for k in sorted(missing_condition)[:10]:
            print(f"         {k}")
        if len(missing_condition) > 10:
            print(f"         ... and {len(missing_condition) - 10} more")
        issues += 1
    else:
        print("  [OK] All spatial stands found in condition file")

    # 4. Age/SI comparison between spatial and condition (for shared stands)
    shared_keys = spatial_keys & condition_keys
    if shared_keys:
        spatial_sub = spatial[spatial["STAND_KEY"].isin(shared_keys)][
            ["STAND_KEY", "STAND_AGE", "SITE_INDEX"]
        ].set_index("STAND_KEY")
        cond_sub = condition_initial[condition_initial["stand_key"].isin(shared_keys)][
            ["stand_key", "AGE", "SI"]
        ].set_index("stand_key")

        merged = spatial_sub.join(cond_sub, how="inner")
        age_mismatch = merged[merged["STAND_AGE"] != merged["AGE"]]
        si_mismatch = merged[merged["SITE_INDEX"] != merged["SI"]]

        if len(age_mismatch) > 0:
            print(f"  [INFO] {len(age_mismatch)} stands have age mismatch (spatial vs condition)")
        else:
            print("  [OK] No age mismatches between spatial and condition")

        if len(si_mismatch) > 0:
            print(f"  [INFO] {len(si_mismatch)} stands have SI mismatch (spatial vs condition)")
        else:
            print("  [OK] No SI mismatches between spatial and condition")

    # Summary
    if issues == 0:
        print("\n  All validation checks passed.")
    else:
        print(f"\n  {issues} validation issue(s) found — review above.")

    print()


# =============================================================================
# MAIN
# =============================================================================

def ingest_all():
    """Load all data sources and run validation. Returns dict of DataFrames."""
    print("=" * 60)
    print("01_ingest: Loading all source data")
    print("=" * 60)

    print("\nLoading spatial data...")
    spatial = load_spatial()

    print("\nLoading yield curves...")
    yields1 = load_yields1()
    yields2 = load_yields2()
    yields3 = load_yields3()

    print("\nLoading condition file...")
    condition, condition_initial = load_condition()

    print("\nLoading management schedule...")
    schedule = load_schedule()

    validate(spatial, yields1, condition_initial, schedule)

    return {
        "spatial": spatial,
        "yields1": yields1,
        "yields2": yields2,
        "yields3": yields3,
        "condition": condition,
        "condition_initial": condition_initial,
        "schedule": schedule,
    }


if __name__ == "__main__":
    data = ingest_all()
