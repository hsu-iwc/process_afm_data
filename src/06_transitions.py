"""
06_transitions.py — Post-Disturbance Transition Rules
======================================================
Defines what yield curve a stand moves to after each disturbance type.

After Clearcut:
  - growth_period: current -> post_regen
  - species may change (e.g. COLB -> LB after replanting)
  - age resets to 0

After 1st/2nd Thin:
  - mgmt_trajectory changes (e.g. T1-0-T2-0 -> T1-19-T2-0)
  - age does NOT reset (stand continues growing on post-thin curve)

After Site Prep:
  - No yield curve change (preparatory action)

Output: transition_rules.csv in SIT format.
"""

import pandas as pd

from config import (
    OUTPUT_DIR,
    CLASSIFIER_NAMES,
    GROWTH_PERIOD_CURRENT,
    GROWTH_PERIOD_POST_REGEN,
)


# Species transitions after clearcut (replanting to commercial species)
# CO-prefix = Cutover, replant to base; CS-prefix = similar
_CLEARCUT_SPECIES_MAP = {
    "LB": "LB", "LL": "LL", "SL": "SL",
    "COLB": "LB", "COLL": "LL", "COSL": "SL",
    "CSLB": "LB", "CSLL": "LL", "CSSL": "SL",
    "PH": "LB",  # Pine/Hardwood replanted to Loblolly
    "HH": "LB",  # Hard hardwood sites typically replanted to Loblolly
    "SH": "LB",  # Soft hardwood sites typically replanted to Loblolly
}


def build_transition_rules(events, stands):
    """
    Build transition rules from disturbance events.

    Parameters:
        events: DataFrame from 05_disturbances (with disturbance_type, thin ages, etc.)
        stands: DataFrame from 02_classifiers (with current classifier assignments)

    Returns:
        DataFrame with SIT transition rule columns.
    """
    print("=" * 60)
    print("06_transitions: Building transition rules")
    print("=" * 60)

    rules = []

    # Get unique disturbance events with their classifier context
    for _, evt in events.iterrows():
        sk = evt["stand_key"]
        dist_type = evt["disturbance_type"]

        # Look up current classifiers for this stand
        stand_row = stands[stands["stand_key"] == sk]
        if len(stand_row) == 0:
            continue
        stand_row = stand_row.iloc[0]

        # Source classifiers (before disturbance)
        src = {c: stand_row[c] for c in CLASSIFIER_NAMES}

        # Target classifiers (after disturbance)
        tgt = dict(src)  # start with copy

        if dist_type == "Clearcut":
            # Transition to post_regen growth period
            tgt["growth_period"] = GROWTH_PERIOD_POST_REGEN
            # Species may change (replanting)
            tgt["species"] = _CLEARCUT_SPECIES_MAP.get(src["species"], src["species"])
            # Trajectory for new rotation — use a default baseline or planned trajectory
            # In practice this comes from the next rotation's planned management
            tgt["mgmt_trajectory"] = "T1-0-T2-0-F1-0-F2-0"
            reset_age = 0

        elif dist_type == "1st_Thin":
            # Move to thinned trajectory; age does NOT reset
            thin1_age = int(evt["thin1"])
            fert1 = int(evt["fert1"])
            fert2 = int(evt["fert2"])
            tgt["mgmt_trajectory"] = f"T1-{thin1_age}-T2-0-F1-{fert1}-F2-{fert2}"
            reset_age = -1  # -1 = no age reset

        elif dist_type == "2nd_Thin":
            # Move to T1+T2 trajectory; age does NOT reset
            thin1_age = int(evt["thin1"])
            thin2_age = int(evt["thin2"])
            fert1 = int(evt["fert1"])
            fert2 = int(evt["fert2"])
            tgt["mgmt_trajectory"] = f"T1-{thin1_age}-T2-{thin2_age}-F1-{fert1}-F2-{fert2}"
            reset_age = -1

        elif dist_type == "Site_Prep":
            # No yield curve change
            reset_age = -1

        else:
            continue

        rule = {
            "disturbance_type": dist_type,
        }
        for c in CLASSIFIER_NAMES:
            rule[f"src_{c}"] = src[c]
            rule[f"tgt_{c}"] = tgt[c]
        rule["reset_age"] = reset_age

        rules.append(rule)

    rules_df = pd.DataFrame(rules)

    # Deduplicate: same source classifiers + disturbance type -> same target
    dedup_cols = ["disturbance_type"] + [f"src_{c}" for c in CLASSIFIER_NAMES]
    rules_df = rules_df.drop_duplicates(subset=dedup_cols)

    print(f"\n  Transition rules: {len(rules_df)}")
    print(f"    By disturbance type: {rules_df['disturbance_type'].value_counts().to_dict()}")

    return rules_df


def write_transition_rules(rules_df):
    """Write transition_rules.csv."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "transition_rules.csv"
    rules_df.to_csv(out_path, index=False)
    print(f"  Wrote {out_path}")
    return out_path


def run(events, stands):
    """Main entry point."""
    rules_df = build_transition_rules(events, stands)
    write_transition_rules(rules_df)
    return rules_df


if __name__ == "__main__":
    from _01_ingest import ingest_all
    from _02_classifiers import run as run_classifiers
    from _05_disturbances import run as run_disturbances
    data = ingest_all()
    stands, _ = run_classifiers(data["spatial"], data["condition_initial"], data["yields1"])
    events, _ = run_disturbances(data["schedule"], data["spatial"], data["yields1"], data["yields3"])
    run(events, stands)
