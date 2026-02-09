"""
03_yield_curves.py — Yield Curve Formatting
=============================================
Converts IWC yield data to GCBM merchantable volume curves.

- Unit conversion: m³/acre × 2.47105 = m³/ha
- Per (stand_key × mgmt_trajectory), produces softwood + hardwood rows
- growth_period=current  → Yields1/Yields3 (stand-specific, ALL trajectory variants)
- growth_period=post_regen → Yields2 (SI-based regen curves, ALL trajectory variants)

The model transitions stands between trajectories via transition rules:
  unthinned (T1-0-T2-0) → post-1st-thin (T1-X-T2-0) → post-2nd-thin (T1-X-T2-Y)

Post-thin volume adjustment: GCBM preserves volume across pools, so the disturbance
matrix is the sole removal mechanism. We add qP_TOP4M3PA (removed volume) back to
post-thin softwood curves so GCBM doesn't double-count the removal.

Output: yield_curves.csv — one row per (classifier combo × pool), columns = ages 0–78
"""

import re

import numpy as np
import pandas as pd

from config import (
    OUTPUT_DIR,
    M3_ACRE_TO_M3_HA,
    MAX_AGE_YIELDS1,
    MAX_AGE_YIELDS2,
    GROWTH_PERIOD_CURRENT,
    GROWTH_PERIOD_POST_REGEN,
    CLASSIFIER_NAMES,
    SI_CLASS_INTERVAL,
)


def _age_cols(max_age):
    """Return list of age column names as strings."""
    return [str(i) for i in range(1, max_age + 1)]


def _extract_volume_by_age(df, product, max_age):
    """
    Extract a single product's volume-by-age array from a yields DataFrame.
    Returns rows for that product with age columns as floats, converted to m³/ha.
    """
    age_cols = _age_cols(max_age)
    sub = df[df["Product"] == product].copy()
    for col in age_cols:
        sub[col] = pd.to_numeric(sub[col], errors="coerce").fillna(0.0) * M3_ACRE_TO_M3_HA
    return sub


def _round_si(si_value):
    """Round SI to nearest interval used in Yields2."""
    if pd.isna(si_value) or si_value == 0:
        return 50
    rounded = int(round(si_value / SI_CLASS_INTERVAL) * SI_CLASS_INTERVAL)
    return max(50, min(100, rounded))


def _parse_thin_ages(trajectory):
    """Parse T1 and T2 ages from a trajectory string like T1-19-T2-0-F1-0-F2-0."""
    m = re.search(r"T1-(\d+)-T2-(\d+)", trajectory)
    if not m:
        return 0, 0
    return int(m.group(1)), int(m.group(2))


def _compute_qp_adjustment(trajectory, qp_lookup, max_age):
    """
    Compute the constant qP volume to add back to a post-thin softwood curve.

    For T1-X-T2-0: add qP at age X from this variant
    For T1-X-T2-Y: add qP from T1-X-T2-0 at age X (1st thin)
                    + qP from T1-X-T2-Y at age Y (2nd thin)

    Parameters:
        trajectory: mgmt_trajectory string (e.g. "T1-19-T2-0-F1-0-F2-0")
        qp_lookup: dict of trajectory -> age_array (pre-filtered to relevant stand/SI)
        max_age: length of age arrays

    Returns:
        float: total qP volume to add back (in m³/ha, already converted)
    """
    thin1_age, thin2_age = _parse_thin_ages(trajectory)

    if thin1_age == 0 and thin2_age == 0:
        return 0.0  # no-thin variant, no adjustment

    total_qp = 0.0

    if thin1_age > 0:
        # Get qP for 1st thin from the T1-X-T2-0 variant
        t1_only_traj = re.sub(r"T2-\d+", "T2-0", trajectory)
        qp_arr = qp_lookup.get(t1_only_traj)
        if qp_arr is not None and thin1_age <= max_age:
            total_qp += qp_arr[thin1_age - 1]  # age is 1-indexed

    if thin2_age > 0:
        # Get qP for 2nd thin from the T1-X-T2-Y variant (this variant)
        qp_arr = qp_lookup.get(trajectory)
        if qp_arr is not None and thin2_age <= max_age:
            total_qp += qp_arr[thin2_age - 1]

    return total_qp


_SPECIES_TO_REGEN = {
    "LB": "LB", "LL": "LL", "SL": "SL",
    "COLB": "LB", "COLL": "LL", "COSL": "SL",
    "CSLB": "LB", "CSLL": "LL", "CSSL": "SL",
    "PH": "LB", "HH": "LB", "SH": "LB",
}


def build_current_yield_curves(stands, yields1, yields3):
    """
    Build yield curves for growth_period=current stands.

    For each forest stand, emit curves for EVERY trajectory variant available
    in Yields1/Yields3 for that stand_key. This ensures the model has the
    unthinned, post-1st-thin, and post-2nd-thin curves to transition between.
    """
    max_age = MAX_AGE_YIELDS1
    age_cols = _age_cols(max_age)

    pine_y1 = _extract_volume_by_age(yields1, "P_TOP4M3PA", max_age)
    hw_y1 = _extract_volume_by_age(yields1, "H_TOP4M3PA", max_age)
    pine_y3 = _extract_volume_by_age(yields3, "P_TOP4M3PA", max_age)
    hw_y3 = _extract_volume_by_age(yields3, "H_TOP4M3PA", max_age)
    # qP for post-thin volume adjustment
    qp_y1 = _extract_volume_by_age(yields1, "qP_TOP4M3PA", max_age)
    qp_y3 = _extract_volume_by_age(yields3, "qP_TOP4M3PA", max_age)

    # Build lookup: (stand_key, trajectory) -> age array
    # Yields3 overrides Yields1 for the same key
    pine_lookup = {}
    hw_lookup = {}
    qp_lookup = {}

    for _, row in pine_y1.iterrows():
        key = (row["stand_key"], row["mgmt_trajectory"])
        pine_lookup[key] = row[age_cols].values.astype(float)
    for _, row in hw_y1.iterrows():
        key = (row["stand_key"], row["mgmt_trajectory"])
        hw_lookup[key] = row[age_cols].values.astype(float)
    for _, row in qp_y1.iterrows():
        qp_lookup.setdefault(row["stand_key"], {})[row["mgmt_trajectory"]] = row[age_cols].values.astype(float)

    # Yields3 overrides
    for _, row in pine_y3.iterrows():
        key = (row["stand_key"], row["mgmt_trajectory"])
        pine_lookup[key] = row[age_cols].values.astype(float)
    for _, row in hw_y3.iterrows():
        key = (row["stand_key"], row["mgmt_trajectory"])
        hw_lookup[key] = row[age_cols].values.astype(float)
    for _, row in qp_y3.iterrows():
        qp_lookup.setdefault(row["stand_key"], {})[row["mgmt_trajectory"]] = row[age_cols].values.astype(float)

    # Collect all available trajectories per stand_key
    all_keys = set(pine_lookup.keys()) | set(hw_lookup.keys())
    stand_trajectories = {}
    for sk, traj in all_keys:
        stand_trajectories.setdefault(sk, set()).add(traj)

    # For each forest stand, emit a curve row for every available trajectory
    forest_stands = stands[stands["IS_FOREST"]].drop_duplicates(subset=["stand_key"]).copy()
    stand_info = forest_stands.set_index("stand_key")[
        [c for c in CLASSIFIER_NAMES if c not in ("stand_key", "mgmt_trajectory", "growth_period")]
    ].to_dict("index")

    rows = []
    stands_with_curves = set()
    stands_missing = set()
    n_adjusted = 0

    for sk, info in stand_info.items():
        trajectories = stand_trajectories.get(sk)
        if trajectories is None:
            stands_missing.add(sk)
            continue

        stands_with_curves.add(sk)

        for traj in sorted(trajectories):
            pine_arr = pine_lookup.get((sk, traj), np.zeros(max_age)).copy()
            hw_arr = hw_lookup.get((sk, traj), np.zeros(max_age))

            # Post-thin volume adjustment: add qP back to softwood curve
            sk_qp = qp_lookup.get(sk, {})
            qp_adj = _compute_qp_adjustment(traj, sk_qp, max_age)
            if qp_adj > 0:
                pine_arr += qp_adj
                n_adjusted += 1

            clf_vals = {
                "stand_key": sk,
                "growth_period": GROWTH_PERIOD_CURRENT,
                "mgmt_trajectory": traj,
                **info,
            }

            sw_row = {**clf_vals, "leading_species": "Softwood"}
            for i, col in enumerate(age_cols):
                sw_row[col] = pine_arr[i]
            rows.append(sw_row)

            hw_row = {**clf_vals, "leading_species": "Hardwood"}
            for i, col in enumerate(age_cols):
                hw_row[col] = hw_arr[i]
            rows.append(hw_row)

    if stands_missing:
        print(f"  [WARN] {len(stands_missing)} forest stands missing from yield tables")
        for sk in sorted(stands_missing)[:5]:
            print(f"         {sk}")
        if len(stands_missing) > 5:
            print(f"         ... and {len(stands_missing) - 5} more")

    result = pd.DataFrame(rows)
    n_curves = len(result) // 2
    print(f"  Current yield curves: {len(result)} rows ({n_curves} curves "
          f"across {len(stands_with_curves)} stands)")
    print(f"  Post-thin qP adjustment applied to {n_adjusted} softwood curves")
    return result


def build_regen_yield_curves(stands, yields2):
    """
    Build yield curves for growth_period=post_regen.

    For each forest stand, emit regen curves for ALL trajectory variants
    available in Yields2 for that stand's SI class + regen species.
    This ensures post-clearcut stands have unthinned, post-1st-thin,
    and post-2nd-thin regen curves to transition between.
    """
    max_age = MAX_AGE_YIELDS2
    age_cols = _age_cols(max_age)

    pine_y2 = _extract_volume_by_age(yields2, "P_TOP4M3PA", max_age)
    hw_y2 = _extract_volume_by_age(yields2, "H_TOP4M3PA", max_age)
    # qP for post-thin volume adjustment
    qp_y2 = _extract_volume_by_age(yields2, "qP_TOP4M3PA", max_age)

    # Build lookup: (si_value, species_code, trajectory) -> age array
    pine_lookup = {}
    hw_lookup = {}
    qp_lookup = {}

    for _, row in pine_y2.iterrows():
        key = (row["si_value"], row["species_code"], row["mgmt_trajectory"])
        pine_lookup[key] = row[age_cols].values.astype(float)
    for _, row in hw_y2.iterrows():
        key = (row["si_value"], row["species_code"], row["mgmt_trajectory"])
        hw_lookup[key] = row[age_cols].values.astype(float)
    for _, row in qp_y2.iterrows():
        outer_key = (row["si_value"], row["species_code"])
        qp_lookup.setdefault(outer_key, {})[row["mgmt_trajectory"]] = row[age_cols].values.astype(float)

    # Collect all available trajectories per (si_value, species_code)
    all_keys = set(pine_lookup.keys()) | set(hw_lookup.keys())
    si_sp_trajectories = {}
    for si, sp, traj in all_keys:
        si_sp_trajectories.setdefault((si, sp), set()).add(traj)

    # For each forest stand, emit regen curves for all trajectories
    # available for its SI + regen species
    forest_stands = stands[stands["IS_FOREST"]].drop_duplicates(subset=["stand_key"]).copy()
    full_age_cols = _age_cols(MAX_AGE_YIELDS1)

    rows = []
    n_adjusted = 0
    for _, stand in forest_stands.iterrows():
        sk = stand["stand_key"]
        species = stand["species"]
        regen_sp = _SPECIES_TO_REGEN.get(species, "LB")
        si_rounded = _round_si(stand.get("si_raw", stand.get("SI", 0)))

        trajectories = si_sp_trajectories.get((si_rounded, regen_sp))
        if trajectories is None:
            # Try adjacent SI classes
            for offset in [5, -5, 10, -10]:
                trajectories = si_sp_trajectories.get((si_rounded + offset, regen_sp))
                if trajectories is not None:
                    si_rounded = si_rounded + offset
                    break
        if trajectories is None:
            continue

        # qP sub-lookup for this SI+species
        si_sp_qp = qp_lookup.get((si_rounded, regen_sp), {})

        clf_base = {
            "stand_key": sk,
            "species": species,
            "origin": stand["origin"],
            "si_class": stand["si_class"],
            "growth_period": GROWTH_PERIOD_POST_REGEN,
        }

        for traj in sorted(trajectories):
            pine_arr = pine_lookup.get((si_rounded, regen_sp, traj), np.zeros(max_age))
            hw_arr = hw_lookup.get((si_rounded, regen_sp, traj), np.zeros(max_age))

            # Post-thin volume adjustment: add qP back to softwood curve
            qp_adj = _compute_qp_adjustment(traj, si_sp_qp, max_age)

            # Pad to MAX_AGE_YIELDS1 for consistency
            pine_full = np.zeros(MAX_AGE_YIELDS1)
            hw_full = np.zeros(MAX_AGE_YIELDS1)
            pine_full[:max_age] = pine_arr
            hw_full[:max_age] = hw_arr
            if max_age < MAX_AGE_YIELDS1:
                pine_full[max_age:] = pine_arr[-1]
                hw_full[max_age:] = hw_arr[-1]

            if qp_adj > 0:
                pine_full += qp_adj
                n_adjusted += 1

            clf_vals = {**clf_base, "mgmt_trajectory": traj}

            sw_row = {**clf_vals, "leading_species": "Softwood"}
            for i, col in enumerate(full_age_cols):
                sw_row[col] = pine_full[i]
            rows.append(sw_row)

            hw_row = {**clf_vals, "leading_species": "Hardwood"}
            for i, col in enumerate(full_age_cols):
                hw_row[col] = hw_full[i]
            rows.append(hw_row)

    result = pd.DataFrame(rows)
    n_curves = len(result) // 2
    n_stands = result["stand_key"].nunique() if len(result) > 0 else 0
    print(f"  Regen yield curves: {len(result)} rows ({n_curves} curves "
          f"across {n_stands} stands)")
    print(f"  Post-thin qP adjustment applied to {n_adjusted} regen softwood curves")
    return result


def deduplicate_curves(df):
    """
    Group rows with identical classifier combos + volume trajectories
    under a single yield_curve_id.
    """
    age_cols = _age_cols(MAX_AGE_YIELDS1)
    avail_age_cols = [c for c in age_cols if c in df.columns]

    # Create a hash of the volume trajectory for dedup
    df["_vol_hash"] = df[avail_age_cols].apply(
        lambda row: hash(tuple(np.round(row.values, 4))), axis=1
    )

    # Group by classifiers + leading_species + volume hash
    group_cols = CLASSIFIER_NAMES + ["leading_species", "_vol_hash"]
    deduped = df.drop_duplicates(subset=group_cols).copy()
    deduped["yield_curve_id"] = range(1, len(deduped) + 1)
    deduped = deduped.drop(columns=["_vol_hash"])
    df = df.drop(columns=["_vol_hash"])

    print(f"  Deduplicated: {len(df)} -> {len(deduped)} unique curves")
    return deduped


def write_yield_curves(df):
    """Write yield_curves.csv."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "yield_curves.csv"
    df.to_csv(out_path, index=False)
    print(f"  Wrote {out_path}")
    return out_path


def run(stands, yields1, yields2, yields3):
    """Main entry point."""
    print("=" * 60)
    print("03_yield_curves: Converting yield tables to GCBM format")
    print("=" * 60)

    current = build_current_yield_curves(stands, yields1, yields3)
    regen = build_regen_yield_curves(stands, yields2)

    combined = pd.concat([current, regen], ignore_index=True)
    print(f"\n  Total yield curve rows: {len(combined)}")

    deduped = deduplicate_curves(combined)
    write_yield_curves(deduped)

    return deduped


if __name__ == "__main__":
    from _01_ingest import ingest_all
    from _02_classifiers import run as run_classifiers
    data = ingest_all()
    stands, _ = run_classifiers(data["spatial"], data["condition_initial"], data["yields1"])
    run(stands, data["yields1"], data["yields2"], data["yields3"])
