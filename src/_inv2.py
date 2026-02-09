import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import numpy as np
from importlib import import_module
ingest = import_module("01_ingest")

SEP = "=" * 80
print(SEP)
print("FOLLOW-UP: SI rounding and Yields2 volume lookup")
print(SEP)

yields2 = ingest.load_yields2()
condition_df, condition_init = ingest.load_condition()

# SI=78 is not in Yields2 SI values [50,55,60,...,100]
# Need to round to nearest 5 -> 80
print()
print("Yields2 available SI values: {}".format(sorted(yields2["si_value"].unique())))
print()
print("BH6112-1-265 has SI=78 from Condition file")
print("Rounded to nearest 5: SI=80")
print()

# Look up SI=80, species=LB
y2m = yields2[(yields2["si_value"] == 80) & (yields2["species_code"] == "LB")]
print("Yields2 rows for SI=80, species=LB: {}".format(len(y2m)))
print()

# Show no-thin trajectory
nt = y2m[y2m["mgmt_trajectory"] == "T1-0-T2-0-F1-0-F2-0"]
if len(nt) > 0:
    print("No-thin trajectory for SI=80 LB:")
    # Show ages 10-20
    ages = [str(a) for a in range(10, 21)]
    for a in ages:
        if a in nt.columns:
            print("  Age {}: vol = {}".format(a, nt.iloc[0][a]))
print()

# Show thin1=14 trajectory
t14 = y2m[y2m["thin1"] == 14]
if len(t14) > 0:
    print("Trajectories with thin1=14 for SI=80 LB:")
    for _, row in t14.iterrows():
        iid = row["iwc_id"]
        v14 = row.get("14", None)
        print("  {} vol@14={}".format(iid, v14))
print()

# Also check: of the 80 1st-rotation thins at age 14,
# what do THOSE stands show in Yields1 at age 14?
yields1 = ingest.load_yields1()
schedule = ingest.load_schedule()

all_t1 = schedule[schedule["ACTION"] == "aHTHIN1"]
rot1_14 = []
for sk in all_t1["stand_key"].unique():
    se = schedule[schedule["stand_key"] == sk].sort_values("YEAR")
    t1e = se[se["ACTION"] == "aHTHIN1"]
    cce = se[se["ACTION"] == "aHCC"]
    for _, tr in t1e.iterrows():
        ty = tr["YEAR"]
        ta = tr.get("AGE", None)
        pcc = cce[cce["YEAR"] < ty]
        if len(pcc) == 0 and ta == 14:
            rot1_14.append(sk)

print(SEP)
print("1st-rotation thins at age 14: {} stands".format(len(rot1_14)))
print(SEP)
print("Checking Yields1 no-thin volume at age 14 for these stands:")
zero_count = 0
nonzero_count = 0
missing_count = 0
for sk in rot1_14[:10]:
    y1 = yields1[(yields1["stand_key"] == sk) & (yields1["mgmt_trajectory"] == "T1-0-T2-0-F1-0-F2-0")]
    if len(y1) > 0 and "14" in y1.columns:
        v = y1.iloc[0]["14"]
        status = "ZERO" if v == 0 else "nonzero"
        if v == 0:
            zero_count += 1
        else:
            nonzero_count += 1
        print("  {} vol@14 = {} ({})".format(sk, v, status))
    else:
        missing_count += 1
        print("  {} NOT FOUND".format(sk))
print("  ... (showing first 10)")
print()

# Full count
zc = 0
nc = 0
mc = 0
for sk in rot1_14:
    y1 = yields1[(yields1["stand_key"] == sk) & (yields1["mgmt_trajectory"] == "T1-0-T2-0-F1-0-F2-0")]
    if len(y1) > 0 and "14" in y1.columns:
        v = y1.iloc[0]["14"]
        if v == 0:
            zc += 1
        else:
            nc += 1
    else:
        mc += 1
print("Full count for {} 1st-rotation thins at age 14:".format(len(rot1_14)))
print("  Zero volume in Yields1:    {}".format(zc))
print("  Nonzero volume in Yields1: {}".format(nc))
print("  Missing from Yields1:      {}".format(mc))