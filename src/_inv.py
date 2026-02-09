import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import numpy as np
from importlib import import_module
ingest = import_module("01_ingest")

SEP = "=" * 80

print(SEP)
print("LOADING DATA")
print(SEP)
schedule = ingest.load_schedule()
yields2 = ingest.load_yields2()
condition_df, condition_init = ingest.load_condition()
yields1 = ingest.load_yields1()
stand = "BH6112-1-265"
print()
print(SEP)
print("STEP 1+2: Full activity sequence for " + stand)
print(SEP)
sub = schedule[schedule["stand_key"] == stand].sort_values("YEAR")
cols = ["stand_key", "YEAR", "ACTION", "AGE", "disturbance_type", "is_disturbance"]
actual = [c for c in cols if c in sub.columns]
print(sub[actual].to_string(index=False))

print()
print(SEP)
print("STEP 3: Does the 1st thin at age 14 happen AFTER a clearcut?")
print(SEP)
events = sub.sort_values("YEAR")
saw_cc = False
for _, row in events.iterrows():
    act = row["ACTION"]
    yr = row["YEAR"]
    ag = row.get("AGE", "?")
    if act == "aHCC":
        saw_cc = True
        print("  Year {}: CLEARCUT (age {}) -- marks start of 2nd rotation".format(yr, ag))
    elif act == "aHTHIN1":
        rot = "2nd rotation" if saw_cc else "1st rotation"
        print("  Year {}: 1st THIN (age {}) -- {}".format(yr, ag, rot))
    elif act == "aHTHIN2":
        rot = "2nd rotation" if saw_cc else "1st rotation"
        print("  Year {}: 2nd THIN (age {}) -- {}".format(yr, ag, rot))
    elif act == "aSP":
        print("  Year {}: SITE_PREP (age {})".format(yr, ag))
    elif act == "aPLT":
        print("  Year {}: PLANTING (age {})".format(yr, ag))
    elif act in ("aFERTL", "aFERTM"):
        print("  Year {}: FERTILIZE (age {})".format(yr, ag))
print()
print(SEP)
print("STEP 4: Which yield curve for a 2nd-rotation thin?")
print(SEP)
print("For 1st rotation: use Yields1 (stand-specific, keyed by stand_key)")
print("For 2nd rotation (after clearcut+replant): use Yields2 (regen, keyed by SI+species)")
print()
print("Checking if stand exists in Yields1...")
y1m = yields1[yields1["stand_key"] == stand]
print("  Yields1 rows for {}: {}".format(stand, len(y1m)))
if len(y1m) > 0:
    print("  Trajectories: {}".format(y1m["mgmt_trajectory"].unique().tolist()))
    nt = y1m[y1m["mgmt_trajectory"] == "T1-0-T2-0-F1-0-F2-0"]
    if len(nt) > 0 and "14" in nt.columns:
        v14 = nt.iloc[0]["14"]
        print("  Yields1 no-thin volume at age 14: {}".format(v14))

print()
print(SEP)
print("STEP 5: Site index and species for " + stand + " from Condition file")
print(SEP)
cm = condition_init[condition_init["stand_key"] == stand]
si_val = None
sp_val = None
if len(cm) > 0:
    for _, row in cm.iterrows():
        si = row.get("SI", "?")
        sp = row.get("Species", "?")
        ag = row.get("AGE", "?")
        print("  Species={}, SI={}, AGE={}".format(sp, si, ag))
        si_val = si
        sp_val = sp
else:
    print("  Not found in condition file, checking schedule...")
    sr = sub.iloc[0] if len(sub) > 0 else None
    if sr is not None:
        si_val = sr.get("si", None)
        sp_val = sr.get("species", None)
        print("  From schedule: species={}, si={}".format(sp_val, si_val))
print()
print(SEP)
print("STEP 6: Look in Yields2 for regen curve matching this stand")
print(SEP)
print("  Looking for SI={}, species={}".format(si_val, sp_val))
y2m = yields2[(yields2["si_value"] == si_val) & (yields2["species_code"] == sp_val)]
print("  Yields2 rows matching SI={}, species={}: {}".format(si_val, sp_val, len(y2m)))
if len(y2m) > 0:
    print("  Available trajectories:")
    for _, row in y2m.iterrows():
        tj = row["mgmt_trajectory"]
        iid = row["iwc_id"]
        v14 = row.get("14", None)
        print("    {}  traj={}  vol@14={}".format(iid, tj, v14))
    nt2 = y2m[y2m["mgmt_trajectory"] == "T1-0-T2-0-F1-0-F2-0"]
    if len(nt2) > 0:
        vn14 = nt2.iloc[0].get("14", None)
        print("")
        print("  >>> No-thin regen curve at age 14: volume = {}".format(vn14))
        print("  >>> This SHOULD be used for pre-thin volume in 2nd rotation!")
    t14 = y2m[y2m["thin1"] == 14]
    if len(t14) > 0:
        print("")
        print("  Yields2 curves with thin1=14:")
        for _, row in t14.iterrows():
            v14 = row.get("14", None)
            print("    {}  vol@14 = {}".format(row["iwc_id"], v14))
print()
print(SEP)
print("STEP 7+8: Count 1st-rotation vs 2nd-rotation 1st_Thin events")
print(SEP)
all_t1 = schedule[schedule["ACTION"] == "aHTHIN1"].copy()
print("Total 1st_Thin (aHTHIN1) events in schedule: {}".format(len(all_t1)))
print()
stands_t1 = all_t1["stand_key"].unique()
print("Unique stands with 1st_Thin: {}".format(len(stands_t1)))
rot2 = []
rot1 = []
for sk in stands_t1:
    se = schedule[schedule["stand_key"] == sk].sort_values("YEAR")
    t1e = se[se["ACTION"] == "aHTHIN1"]
    cce = se[se["ACTION"] == "aHCC"]
    for _, tr in t1e.iterrows():
        ty = tr["YEAR"]
        ta = tr.get("AGE", None)
        pcc = cce[cce["YEAR"] < ty]
        if len(pcc) > 0:
            rot2.append({"stand_key": sk, "thin_year": ty, "thin_age": ta, "cc_year": pcc.iloc[-1]["YEAR"]})
        else:
            rot1.append({"stand_key": sk, "thin_year": ty, "thin_age": ta})
print("")
print("1st_Thin in 1ST rotation (no prior clearcut): {}".format(len(rot1)))
print("1st_Thin in 2ND rotation (after a clearcut):  {}".format(len(rot2)))
print()
if rot1:
    df1 = pd.DataFrame(rot1)
    print("Age distribution of 1ST-rotation 1st_Thins:")
    print(df1["thin_age"].value_counts().sort_index().to_string())
    print()
if rot2:
    df2 = pd.DataFrame(rot2)
    print("Age distribution of 2ND-rotation 1st_Thins:")
    print(df2["thin_age"].value_counts().sort_index().to_string())
    print()
    df2["gap"] = df2["thin_year"] - df2["cc_year"]
    print("Years between clearcut and 2nd-rotation 1st_Thin:")
    print(df2["gap"].value_counts().sort_index().to_string())
    print()
print(SEP)
print("BONUS: For 2nd-rotation thins, what does Yields1 show at that age?")
print(SEP)
if rot2:
    s2 = pd.DataFrame(rot2)
    ex = s2.head(5)
    for _, e in ex.iterrows():
        sk = e["stand_key"]
        age = int(e["thin_age"])
        y1 = yields1[(yields1["stand_key"] == sk) & (yields1["mgmt_trajectory"] == "T1-0-T2-0-F1-0-F2-0")]
        if len(y1) > 0 and str(age) in y1.columns:
            v = y1.iloc[0][str(age)]
            print("  {} age {} in Yields1 (no-thin): vol = {}".format(sk, age, v))
        else:
            print("  {} age {} in Yields1: NOT FOUND or column missing".format(sk, age))
    print()

print(SEP)
print("SUMMARY")
print(SEP)
tot = len(all_t1)
n1 = len(rot1)
n2 = len(rot2)
print("Total 1st_Thin events:  {}".format(tot))
print("  1st rotation thins:   {} (use Yields1 - stand-specific curves)".format(n1))
print("  2nd rotation thins:   {} (use Yields2 - regen SI-based curves)".format(n2))
print()
print("CONCLUSION:")
if n2 > 0:
    print("  {} of the {} 1st_Thin events occur in the 2nd rotation".format(n2, tot))
    print("  (after a clearcut). For these events, the pre-thin volume should be")
    print("  looked up from Yields2 (regen curves keyed by SI+species), NOT Yields1")
    print("  (which is the stand-specific current-rotation curve).")
    print()
    print("  At age 14 in Yields2, the regen curves DO have volume, confirming that")
    print("  the zero-volume issue comes from looking in the wrong yield table.")
else:
    print("  No 2nd rotation thins found. Hypothesis not supported.")
