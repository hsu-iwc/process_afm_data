"""
05_disturbances.py — Disturbance Layers
=========================================
Extracts disturbance events from the management schedule, calculates
thinning volume removal percentages, detects partial (split-year) clearcuts,
and builds spatial disturbance layers.

Partial clearcuts: When a harvest constraint splits a clearcut across multiple
years, intermediate events become "XX.XX% clearcut" (area proportion using
condition file AREA as denominator). The final event in each rotation cluster
remains a standard "Clearcut" which triggers the yield curve transition.

Outputs:
  - disturbances.gpkg (single file with year column)
  - disturbance_events.csv in SIT format
"""

import numpy as np
import pandas as pd
import geopandas as gpd

from config import (
    OUTPUT_DIR,
    CLASSIFIER_NAMES,
    ACTION_TO_DISTURBANCE,
    NON_DISTURBANCE_ACTIONS,
    SIM_START_YEAR,
    SIM_END_YEAR,
    M3_ACRE_TO_M3_HA,
    MAX_AGE_YIELDS1,
    MAX_AGE_YIELDS2,
    SI_CLASS_INTERVAL,
)


# =============================================================================
# 6a: EXTRACT DISTURBANCE EVENTS
# =============================================================================

def extract_disturbance_events(schedule):
    """
    Filter schedule to disturbance-type actions only and build event table.
    Also tag each thinning event as 1st or 2nd rotation based on whether
    a prior clearcut exists for that stand in the schedule.
    """
    dist = schedule[schedule["is_disturbance"]].copy()

    events = dist[[
        "stand_key", "YEAR", "ACTION", "disturbance_type", "AGE", "AREA",
        "species", "origin", "grow_type", "si",
        "thin1", "thin2", "fert1", "fert2",
        "treatment_type", "management_type",
    ]].copy()

    events = events.rename(columns={"YEAR": "year", "AGE": "age", "AREA": "area"})

    # Tag rotation: for each thinning event, check if a clearcut happened
    # earlier in the schedule for the same stand
    events = events.sort_values(["stand_key", "year"])
    events["rotation"] = 1  # default: 1st rotation

    for sk in events["stand_key"].unique():
        sk_events = events[events["stand_key"] == sk].sort_values("year")
        cc_seen = False
        for idx, row in sk_events.iterrows():
            if row["disturbance_type"] == "Clearcut":
                cc_seen = True
            elif row["disturbance_type"] in ("1st_Thin", "2nd_Thin") and cc_seen:
                events.loc[idx, "rotation"] = 2

    print(f"  Disturbance events: {len(events)}")
    print(f"    By type: {events['disturbance_type'].value_counts().to_dict()}")
    print(f"    Year range: {events['year'].min()}-{events['year'].max()}")

    thin_events = events[events["disturbance_type"].isin(["1st_Thin", "2nd_Thin"])]
    r1 = (thin_events["rotation"] == 1).sum()
    r2 = (thin_events["rotation"] == 2).sum()
    print(f"    Thinning rotation split: {r1} 1st-rotation, {r2} 2nd-rotation")

    return events


# =============================================================================
# 6a2: CLASSIFY PARTIAL (SPLIT-YEAR) CLEARCUTS
# =============================================================================

# Stands to skip from partial CC detection (known data issues)
_PARTIAL_CC_SKIP = set()


def classify_partial_clearcuts(events, condition_initial):
    """
    Detect split-year clearcuts and reclassify intermediate events.

    When a harvest constraint splits a clearcut across multiple years for the
    same stand, the intermediate events become "XX.XX% clearcut" and the final
    event in each rotation cluster stays as "Clearcut".

    Uses the condition file AREA as the denominator for % calculation.
    """
    # Build condition area lookup
    cond_area = condition_initial.set_index("stand_key")["AREA"].to_dict()

    cc = events[events["disturbance_type"] == "Clearcut"].copy()
    if len(cc) == 0:
        return events

    # Find stands with multiple CC events
    cc_counts = cc.groupby("stand_key").size()
    multi_cc_stands = set(cc_counts[cc_counts > 1].index) - _PARTIAL_CC_SKIP

    if not multi_cc_stands:
        print("  No split-year clearcuts detected.")
        return events

    # For each multi-CC stand, cluster events into rotations (gap > 10yr = new rotation)
    n_reclassified = 0

    for sk in sorted(multi_cc_stands):
        stand_area = cond_area.get(sk)
        if stand_area is None or stand_area <= 0:
            continue

        sk_cc = cc[cc["stand_key"] == sk].sort_values("year")
        sk_indices = sk_cc.index.tolist()
        sk_years = sk_cc["year"].values
        sk_areas = sk_cc["area"].values

        # Cluster into rotations by year gap
        clusters = [[0]]
        for i in range(1, len(sk_years)):
            if sk_years[i] - sk_years[i - 1] > 10:
                clusters.append([i])
            else:
                clusters[-1].append(i)

        for cluster in clusters:
            if len(cluster) <= 1:
                continue  # single event = standard Clearcut, no change

            cluster_area = sum(sk_areas[i] for i in cluster)
            # Only reclassify if the cluster has genuine partial events
            # (individual events < ~95% of stand area)
            has_partial = any(sk_areas[i] / stand_area < 0.95 for i in cluster)
            if not has_partial:
                continue

            # All but the last event → "XX.XX% clearcut"
            for i in cluster[:-1]:
                idx = sk_indices[i]
                pct = round(sk_areas[i] / stand_area * 100, 2)
                events.loc[idx, "disturbance_type"] = f"{pct}% clearcut"
                n_reclassified += 1

            # Last event stays "Clearcut" — triggers transition

    if n_reclassified > 0:
        print(f"  Partial clearcuts: {n_reclassified} events reclassified across "
              f"{len(multi_cc_stands)} stands")

        # Print summary of unique partial CC percentages
        partial_events = events[events["disturbance_type"].str.endswith("% clearcut")]
        pcts = partial_events["disturbance_type"].str.replace("% clearcut", "").astype(float)
        print(f"    Unique partial CC percentages: {sorted(pcts.unique())}")
    else:
        print("  No split-year clearcuts requiring reclassification.")

    return events


# =============================================================================
# 6b: CALCULATE THINNING VOLUME REMOVAL %
# =============================================================================

def _get_volume_at_age(yields_df, stand_key, trajectory, product, age, max_age=MAX_AGE_YIELDS1):
    """Look up volume at a specific age from a yields DataFrame (keyed by stand_key)."""
    age_col = str(int(age))
    if int(age) < 1 or int(age) > max_age:
        return 0.0

    sub = yields_df[
        (yields_df["stand_key"] == stand_key)
        & (yields_df["mgmt_trajectory"] == trajectory)
        & (yields_df["Product"] == product)
    ]

    if len(sub) == 0:
        return None

    val = sub.iloc[0][age_col]
    if pd.isna(val):
        return 0.0
    return float(val)


def _get_regen_volume_at_age(yields2, si_class, species_code, trajectory, product, age):
    """Look up volume at a specific age from Yields2 (keyed by SI + species)."""
    age_col = str(int(age))
    max_age = MAX_AGE_YIELDS2
    if int(age) < 1 or int(age) > max_age:
        return 0.0

    sub = yields2[
        (yields2["si_value"] == si_class)
        & (yields2["species_code"] == species_code)
        & (yields2["mgmt_trajectory"] == trajectory)
        & (yields2["Product"] == product)
    ]

    if len(sub) == 0:
        return None

    val = sub.iloc[0][age_col]
    if pd.isna(val):
        return 0.0
    return float(val)


def _species_to_regen_code(species):
    """Map condition species to Yields2 regen species code."""
    mapping = {
        "LB": "LB", "LL": "LL", "SL": "SL",
        "COLB": "LB", "COLL": "LL", "COSL": "SL",
        "CSLB": "LB", "CSLL": "LL", "CSSL": "SL",
        "PH": "LB", "HH": "LB", "SH": "LB",
    }
    return mapping.get(species, "LB")


def _round_si(si_value):
    """Round SI to nearest interval used in Yields2."""
    if pd.isna(si_value) or si_value == 0:
        return 50  # default fallback
    rounded = int(round(si_value / SI_CLASS_INTERVAL) * SI_CLASS_INTERVAL)
    return max(50, min(100, rounded))  # clamp to Yields2 range


def calc_thinning_pct(events, yields1, yields3, yields2):
    """
    Calculate thinning volume removal percentage for each thinning event.

    For 1st-rotation thins: uses Yields3/Yields1 (stand-specific curves)
    For 2nd-rotation thins: uses Yields2 (SI-based regen curves)

    Schedule columns thin1/thin2 reflect state BEFORE the action:
    - At aHTHIN1: thin1=0 (hasn't happened), actual thin age = AGE column
    - At aHTHIN2: thin1=<prior 1st thin age>, actual thin age = AGE column
    """
    thin_events = events[events["disturbance_type"].isin(["1st_Thin", "2nd_Thin"])].copy()

    if len(thin_events) == 0:
        print("  No thinning events found.")
        events["pct_volume_removed"] = np.nan
        return events

    pct_list = []
    n_warnings = 0

    for idx, evt in thin_events.iterrows():
        sk = evt["stand_key"]
        age = int(evt["age"])
        prior_thin1 = int(evt["thin1"])
        fert1 = int(evt["fert1"])
        fert2 = int(evt["fert2"])
        rotation = evt["rotation"]
        si_raw = evt["si"]
        species = evt["species"]

        pre_p = None
        rem_p = None
        pre_h = None
        rem_h = 0.0

        if evt["disturbance_type"] == "1st_Thin":
            actual_thin1_age = age
            no_thin_traj = f"T1-0-T2-0-F1-{fert1}-F2-{fert2}"
            thinned_traj = f"T1-{actual_thin1_age}-T2-0-F1-{fert1}-F2-{fert2}"

            if rotation == 2:
                # 2nd rotation: use Yields2 (regen curves by SI + species)
                si_rounded = _round_si(si_raw)
                regen_sp = _species_to_regen_code(species)

                pre_p = _get_regen_volume_at_age(yields2, si_rounded, regen_sp, no_thin_traj, "P_TOP4M3PA", age)
                rem_p = _get_regen_volume_at_age(yields2, si_rounded, regen_sp, thinned_traj, "qP_TOP4M3PA", age)
                pre_h = _get_regen_volume_at_age(yields2, si_rounded, regen_sp, no_thin_traj, "H_TOP4M3PA", age)
            else:
                # 1st rotation: use Yields3 then Yields1 (stand-specific)
                pre_p = _get_volume_at_age(yields3, sk, no_thin_traj, "P_TOP4M3PA", age)
                if pre_p is None:
                    pre_p = _get_volume_at_age(yields1, sk, no_thin_traj, "P_TOP4M3PA", age)

                rem_p = _get_volume_at_age(yields3, sk, thinned_traj, "qP_TOP4M3PA", age)
                if rem_p is None:
                    rem_p = _get_volume_at_age(yields1, sk, thinned_traj, "qP_TOP4M3PA", age)

                pre_h = _get_volume_at_age(yields3, sk, no_thin_traj, "H_TOP4M3PA", age)
                if pre_h is None:
                    pre_h = _get_volume_at_age(yields1, sk, no_thin_traj, "H_TOP4M3PA", age)

        elif evt["disturbance_type"] == "2nd_Thin":
            actual_thin2_age = age
            t1_only_traj = f"T1-{prior_thin1}-T2-0-F1-{fert1}-F2-{fert2}"
            thinned_traj = f"T1-{prior_thin1}-T2-{actual_thin2_age}-F1-{fert1}-F2-{fert2}"

            if rotation == 2:
                si_rounded = _round_si(si_raw)
                regen_sp = _species_to_regen_code(species)

                pre_p = _get_regen_volume_at_age(yields2, si_rounded, regen_sp, t1_only_traj, "P_TOP4M3PA", age)
                rem_p = _get_regen_volume_at_age(yields2, si_rounded, regen_sp, thinned_traj, "qP_TOP4M3PA", age)
                pre_h = _get_regen_volume_at_age(yields2, si_rounded, regen_sp, t1_only_traj, "H_TOP4M3PA", age)
            else:
                pre_p = _get_volume_at_age(yields3, sk, t1_only_traj, "P_TOP4M3PA", age)
                if pre_p is None:
                    pre_p = _get_volume_at_age(yields1, sk, t1_only_traj, "P_TOP4M3PA", age)

                rem_p = _get_volume_at_age(yields3, sk, thinned_traj, "qP_TOP4M3PA", age)
                if rem_p is None:
                    rem_p = _get_volume_at_age(yields1, sk, thinned_traj, "qP_TOP4M3PA", age)

                pre_h = _get_volume_at_age(yields3, sk, t1_only_traj, "H_TOP4M3PA", age)
                if pre_h is None:
                    pre_h = _get_volume_at_age(yields1, sk, t1_only_traj, "H_TOP4M3PA", age)

        # Calculate percentage
        pre_p = pre_p if pre_p is not None else 0.0
        pre_h = pre_h if pre_h is not None else 0.0
        rem_p = rem_p if rem_p is not None else 0.0
        rem_h = rem_h if rem_h is not None else 0.0

        total_pre = pre_p + pre_h
        total_rem = rem_p + rem_h

        if total_pre > 0:
            pct = round(total_rem / total_pre * 100, 2)
        else:
            pct = 0.0
            n_warnings += 1
            if n_warnings <= 5:
                print(f"    [WARN] Zero pre-thin volume for {sk} at age {age} (rotation {rotation})")

        pct_list.append(pct)

    if n_warnings > 5:
        print(f"    ... and {n_warnings - 5} more zero-volume warnings")

    thin_events["pct_volume_removed"] = pct_list

    # Merge back
    events = events.merge(
        thin_events[["pct_volume_removed"]],
        left_index=True, right_index=True, how="left",
    )

    # Standard clearcuts: 97% removal
    events.loc[events["disturbance_type"] == "Clearcut", "pct_volume_removed"] = 97.0
    # Partial clearcuts: use the area-based percentage
    partial_mask = events["disturbance_type"].str.endswith("% clearcut", na=False)
    for idx in events[partial_mask].index:
        pct_str = events.loc[idx, "disturbance_type"].replace("% clearcut", "")
        events.loc[idx, "pct_volume_removed"] = float(pct_str)
    # Site prep: 0% volume removal (preparatory, non-harvest)
    events.loc[events["disturbance_type"] == "Site_Prep", "pct_volume_removed"] = 0.0

    # Statistics
    for dt in ["1st_Thin", "2nd_Thin"]:
        sub = thin_events[thin_events["disturbance_type"] == dt]
        if len(sub) > 0:
            nonzero = sub[sub["pct_volume_removed"] > 0]
            print(f"\n  {dt}: {len(sub)} events, {len(nonzero)} with calculated %")
            if len(nonzero) > 0:
                print(f"    Range: {nonzero['pct_volume_removed'].min():.1f}% - {nonzero['pct_volume_removed'].max():.1f}%")
                print(f"    Mean:  {nonzero['pct_volume_removed'].mean():.1f}%")

    return events


# =============================================================================
# 6c: BUILD SPATIAL DISTURBANCE LAYERS
# =============================================================================

def build_spatial_disturbance_layers(events, spatial):
    """
    Join disturbance events to stand polygons and write a single GeoPackage
    with a year column. Also outputs disturbance_events.csv in SIT format.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Join events to spatial geometry
    geom = spatial[["STAND_KEY", "geometry"]].copy()
    geom = geom.rename(columns={"STAND_KEY": "stand_key"})

    events_geo = events.merge(geom, on="stand_key", how="left")
    events_geo = gpd.GeoDataFrame(events_geo, geometry="geometry", crs=spatial.crs)

    # Write single GeoPackage with year column
    out_path = OUTPUT_DIR / "disturbances.gpkg"
    out_cols = ["stand_key", "year", "disturbance_type", "pct_volume_removed", "geometry"]
    events_geo[out_cols].to_file(out_path, driver="GPKG")
    n_years = events_geo["year"].nunique()
    print(f"\n  Wrote {out_path} ({len(events_geo)} events across {n_years} years)")

    # Write SIT-format disturbance events CSV
    sit_events = events.copy()
    sit_events["timestep"] = sit_events["year"] - SIM_START_YEAR + 1

    sit_cols = ["timestep", "stand_key", "disturbance_type", "pct_volume_removed",
                "year", "age", "area"]
    sit_out = OUTPUT_DIR / "disturbance_events.csv"
    sit_events[sit_cols].to_csv(sit_out, index=False)
    print(f"  Wrote {sit_out} ({len(sit_events)} events)")

    return events_geo


# =============================================================================
# MAIN
# =============================================================================

def run(schedule, spatial, yields1, yields3, yields2, condition_initial=None):
    """Main entry point."""
    print("=" * 60)
    print("05_disturbances: Building disturbance layers")
    print("=" * 60)

    events = extract_disturbance_events(schedule)

    # Classify partial (split-year) clearcuts before calculating %
    if condition_initial is not None:
        events = classify_partial_clearcuts(events, condition_initial)

    events = calc_thinning_pct(events, yields1, yields3, yields2)
    events_geo = build_spatial_disturbance_layers(events, spatial)

    return events, events_geo


if __name__ == "__main__":
    from _01_ingest import ingest_all
    data = ingest_all()
    run(data["schedule"], data["spatial"], data["yields1"], data["yields3"], data["yields2"],
        condition_initial=data["condition_initial"])
