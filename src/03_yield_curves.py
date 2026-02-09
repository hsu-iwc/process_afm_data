"""
03_yield_curves.py — Yield Curve Formatting
=============================================
Converts IWC yield data to GCBM merchantable volume curves.

- Unit conversion: m³/acre × 2.47105 = m³/ha
- Per classifier combination, produces softwood + hardwood rows
- growth_period=current  → Yields1 (stand-specific)
- growth_period=post_regen → Yields2 (SI-based regen curves)
- Yields3 used when actual thin age differs from Yields1 embedded timing

Output: yield_curves.csv — one row per (classifier combo × pool), columns = ages 0–78
"""

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


def build_current_yield_curves(stands, yields1, yields3):
    """
    Build yield curves for growth_period=current stands.

    Each stand's curve comes from Yields1, keyed by the full iwc_id which
    encodes stand_key + management trajectory. If Yields3 has a matching
    entry with a different thin timing, it overrides Yields1.

    Returns DataFrame with classifier columns + age columns (m³/ha).
    """
    max_age = MAX_AGE_YIELDS1
    age_cols = _age_cols(max_age)

    # Get the pine and hardwood volume products from Yields1
    pine_y1 = _extract_volume_by_age(yields1, "P_TOP4M3PA", max_age)
    hw_y1 = _extract_volume_by_age(yields1, "H_TOP4M3PA", max_age)

    # Also get Yields3 products (thinning sim overrides)
    pine_y3 = _extract_volume_by_age(yields3, "P_TOP4M3PA", max_age)
    hw_y3 = _extract_volume_by_age(yields3, "H_TOP4M3PA", max_age)

    # Index Y1 by (stand_key, mgmt_trajectory)
    pine_y1["_key"] = pine_y1["stand_key"] + "|" + pine_y1["mgmt_trajectory"]
    hw_y1["_key"] = hw_y1["stand_key"] + "|" + hw_y1["mgmt_trajectory"]

    # Index Y3 similarly
    pine_y3["_key"] = pine_y3["stand_key"] + "|" + pine_y3["mgmt_trajectory"]
    hw_y3["_key"] = hw_y3["stand_key"] + "|" + hw_y3["mgmt_trajectory"]

    # Build lookup dicts: _key -> age array
    pine_y1_lookup = {}
    for _, row in pine_y1.iterrows():
        pine_y1_lookup[row["_key"]] = row[age_cols].values.astype(float)

    hw_y1_lookup = {}
    for _, row in hw_y1.iterrows():
        hw_y1_lookup[row["_key"]] = row[age_cols].values.astype(float)

    pine_y3_lookup = {}
    for _, row in pine_y3.iterrows():
        pine_y3_lookup[row["_key"]] = row[age_cols].values.astype(float)

    hw_y3_lookup = {}
    for _, row in hw_y3.iterrows():
        hw_y3_lookup[row["_key"]] = row[age_cols].values.astype(float)

    # For each stand with growth_period=current, find its yield curve
    current_stands = stands[
        (stands["growth_period"] == GROWTH_PERIOD_CURRENT) & stands["IS_FOREST"]
    ].copy()

    rows = []
    missing = set()
    for _, stand in current_stands.iterrows():
        sk = stand["stand_key"]
        traj = stand["mgmt_trajectory"]
        key = f"{sk}|{traj}"

        # Try Yields3 first (thinning sim override), then Yields1
        pine_arr = pine_y3_lookup.get(key, pine_y1_lookup.get(key))
        hw_arr = hw_y3_lookup.get(key, hw_y1_lookup.get(key))

        if pine_arr is None and hw_arr is None:
            # Try the no-treatment baseline from Yields1
            baseline_key = f"{sk}|T1-0-T2-0-F1-0-F2-0"
            pine_arr = pine_y1_lookup.get(baseline_key)
            hw_arr = hw_y1_lookup.get(baseline_key)

        if pine_arr is None and hw_arr is None:
            missing.add(sk)
            continue

        if pine_arr is None:
            pine_arr = np.zeros(max_age)
        if hw_arr is None:
            hw_arr = np.zeros(max_age)

        clf_vals = {c: stand[c] for c in CLASSIFIER_NAMES}

        # Softwood row
        sw_row = {**clf_vals, "leading_species": "Softwood"}
        for i, col in enumerate(age_cols):
            sw_row[col] = pine_arr[i]
        rows.append(sw_row)

        # Hardwood row
        hw_row = {**clf_vals, "leading_species": "Hardwood"}
        for i, col in enumerate(age_cols):
            hw_row[col] = hw_arr[i]
        rows.append(hw_row)

    if missing:
        print(f"  [WARN] {len(missing)} stands missing from yield tables (no curve found)")
        for sk in sorted(missing)[:5]:
            print(f"         {sk}")

    result = pd.DataFrame(rows)
    print(f"  Current yield curves: {len(result)} rows ({len(result)//2} stands)")
    return result


def build_regen_yield_curves(stands, yields2):
    """
    Build yield curves for growth_period=post_regen.

    These use Yields2 (generic SI-based regen curves) keyed by
    si_class + species (mapped to LB/LL/SL) + mgmt_trajectory.

    Returns DataFrame with classifier columns + age columns (m³/ha).
    """
    max_age = MAX_AGE_YIELDS2
    age_cols = _age_cols(max_age)

    pine_y2 = _extract_volume_by_age(yields2, "P_TOP4M3PA", max_age)
    hw_y2 = _extract_volume_by_age(yields2, "H_TOP4M3PA", max_age)

    # Index by (si_value, species_code, mgmt_trajectory)
    pine_y2["_key"] = (
        "SI" + pine_y2["si_value"].astype(str) + "|"
        + pine_y2["species_code"] + "|"
        + pine_y2["mgmt_trajectory"]
    )
    hw_y2["_key"] = (
        "SI" + hw_y2["si_value"].astype(str) + "|"
        + hw_y2["species_code"] + "|"
        + hw_y2["mgmt_trajectory"]
    )

    pine_lookup = {}
    for _, row in pine_y2.iterrows():
        pine_lookup[row["_key"]] = row[age_cols].values.astype(float)

    hw_lookup = {}
    for _, row in hw_y2.iterrows():
        hw_lookup[row["_key"]] = row[age_cols].values.astype(float)

    # Map condition species to regen species code
    # CO-prefixed species (cutover) convert to base: COLB->LB, COLL->LL, COSL->SL
    # CS-prefixed: CSLB->LB, CSLL->LL, CSSL->SL
    # PH, HH, SH default to LB (loblolly replanting is common in SE US)
    _species_to_regen = {
        "LB": "LB", "LL": "LL", "SL": "SL",
        "COLB": "LB", "COLL": "LL", "COSL": "SL",
        "CSLB": "LB", "CSLL": "LL", "CSSL": "SL",
        "PH": "LB", "HH": "LB", "SH": "LB",
    }

    # We generate regen curves for each unique (species, origin, si_class, mgmt_trajectory)
    # combo that exists among forest stands.
    forest = stands[stands["IS_FOREST"]].copy()
    combos = forest[CLASSIFIER_NAMES].drop_duplicates()

    rows = []
    for _, combo in combos.iterrows():
        regen_sp = _species_to_regen.get(combo["species"], "LB")
        si = combo["si_class"]
        # For regen, use a baseline trajectory (T1-0-T2-0-F1-0-F2-0) or same trajectory
        traj = combo["mgmt_trajectory"]

        key = f"{si}|{regen_sp}|{traj}"
        pine_arr = pine_lookup.get(key)
        hw_arr = hw_lookup.get(key)

        # Fall back to no-treatment baseline
        if pine_arr is None:
            baseline_key = f"{si}|{regen_sp}|T1-0-T2-0-F1-0-F2-0"
            pine_arr = pine_lookup.get(baseline_key)
            hw_arr = hw_lookup.get(baseline_key)

        if pine_arr is None:
            pine_arr = np.zeros(max_age)
        if hw_arr is None:
            hw_arr = np.zeros(max_age)

        # Pad to MAX_AGE_YIELDS1 columns for consistency (flat-line after max_age)
        pine_full = np.zeros(MAX_AGE_YIELDS1)
        hw_full = np.zeros(MAX_AGE_YIELDS1)
        pine_full[:max_age] = pine_arr
        hw_full[:max_age] = hw_arr
        # Extend last value for ages beyond Yields2 range
        if max_age < MAX_AGE_YIELDS1:
            pine_full[max_age:] = pine_arr[-1]
            hw_full[max_age:] = hw_arr[-1]

        full_age_cols = _age_cols(MAX_AGE_YIELDS1)

        clf_vals = {c: combo[c] for c in CLASSIFIER_NAMES}
        clf_vals["growth_period"] = GROWTH_PERIOD_POST_REGEN

        sw_row = {**clf_vals, "leading_species": "Softwood"}
        for i, col in enumerate(full_age_cols):
            sw_row[col] = pine_full[i]
        rows.append(sw_row)

        hw_row = {**clf_vals, "leading_species": "Hardwood"}
        for i, col in enumerate(full_age_cols):
            hw_row[col] = hw_full[i]
        rows.append(hw_row)

    result = pd.DataFrame(rows)
    print(f"  Regen yield curves: {len(result)} rows ({len(result)//2} combos)")
    return result


def deduplicate_curves(df):
    """
    Group stands with identical volume trajectories under a single yield_curve_id.
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

    # Also build mapping back to original: drop _vol_hash from original too
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
