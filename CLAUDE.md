# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository processes forestry data for IWC (International Woodland Company) — specifically the "Boothill" tract in Georgia (378 spatial features, ~12,400 acres) — into inputs for the GCBM (Generic Carbon Budget Model) with the mojadata tiler.

## Repository Structure

- **`spatial/`** — ESRI Shapefiles representing stand/property boundaries. Current dataset: `IWC_FFF_NA_IWCBH_GA_20260115115551.shp` (EPSG:4326, 378 stands). Must be loaded with `pyogrio` engine due to `0000/00/00` date fields.
- **`yields/IWC_Shared_Folder/`** — Three yield CSVs, a condition Excel file, and carbon modeling docs.
- **`managment_schedule/`** — 50-year harvest/silviculture schedule (`IWC_2025_BTHILL_HSM_DRAFT6.xlsx`). Note: directory name has a typo ("managment").
- **`aidb_disturbance_manager.py`** — Reusable module for managing AIDB disturbance matrices and species types. Used by the pipeline's step 07.
- **`src/`** — GCBM input processing pipeline (see Pipeline section below).
- **`output/gcbm_input/`** — Generated GCBM input files.

## Pipeline (`src/`)

Run with: `cd src && python run_pipeline.py --skip-aidb`

| Module | Purpose |
|--------|---------|
| `config.py` | Paths, unit conversions, species/disturbance mappings, simulation params, stand key corrections |
| `01_ingest.py` | Loads spatial, 3 yield CSVs, condition Excel, schedule; applies stand key renames; validates cross-source |
| `02_classifiers.py` | 6 GCBM classifiers: stand_key, species, origin, si_class, growth_period, mgmt_trajectory |
| `03_yield_curves.py` | Converts yield tables to GCBM format (m3/ha); applies post-thin qP volume adjustment |
| `04_inventory.py` | Starting inventory GeoPackage (301 forest features with classifiers + geometry) |
| `05_disturbances.py` | Extracts events, calculates thinning removal %, classifies partial clearcuts, writes disturbances.gpkg |
| `06_transitions.py` | Post-disturbance yield curve transition rules (incl. partial CC = no transition) |
| `07_aidb_thinning.py` | Adds thinning disturbance matrices to AIDB via `aidb_disturbance_manager.py` |
| `08_tiler_config.py` | Generates mojadata tiler script |
| `run_pipeline.py` | Orchestrates steps 01-08 |

### Output Files (`output/gcbm_input/`)

- `classifiers.csv` — SIT-format classifier definitions
- `yield_curves.csv` — Merchantable volume curves (m3/ha) by classifier combo, ages 1-78
- `inventory.gpkg` — Starting inventory with classifiers, age, historical disturbance, geometry
- `disturbances.gpkg` — All 1,386 disturbance events (422 CC, 394 SP, 339 1st thin, 208 2nd thin, 23 partial CC) with year, disturbance type, removal %
- `disturbance_events.csv` — SIT-format disturbance events with timestep
- `transition_rules.csv` — Post-disturbance classifier transitions
- `thinning_disturbance_mapping.csv` — Maps removal % to AIDB disturbance type names
- `tiler.py` — Mojadata tiler configuration script

## Key Data Concepts

### Yield Curve Sources
- **Yields1** — Stand-specific curves for the current (1st) rotation, keyed by `iwc_id` which encodes stand_key + management trajectory (T1/T2/F1/F2 ages).
- **Yields2** — Generic regeneration curves for post-clearcut (2nd) rotation, keyed by site index (SI50-SI100) + species (LB/LL/SL) + trajectory.
- **Yields3** — Thinning simulation overrides; same structure as Yields1 but with `qP_TOP4M3PA` (removed volume) populated. Pipe-delimited values at thin ages (before|after).

### Post-Thin Volume Adjustment
`03_yield_curves.py` adds removed volume (`qP_TOP4M3PA`) back to post-thin softwood curves as a constant offset. This prevents double-counting: GCBM's disturbance matrix is the sole mechanism for removing volume, and the yield curve must show the "as if no thin happened" trajectory.

### Thinning Volume Removal Calculation
See `src/05_disturbances.py`. The schedule's TH9/TH10 columns (thin1/thin2) represent state BEFORE the action row, NOT the action parameter. The actual thin age comes from the AGE column. 73% of thinning events are 2nd-rotation (after clearcut) and must use Yields2.

### Partial (Split-Year) Clearcuts
10 stands have clearcuts split across 2-4 years due to harvest constraints. Intermediate events → "XX.XX% clearcut" (area/condition_AREA × 100), final → standard "Clearcut". Partial CCs trigger no yield curve transition (only the final Clearcut does). 23 events reclassified, 17 unique partial CC percentages.

### Stand Key Corrections
BH5149-1-997 is renamed → BH5149-1-162 at load time (config `STAND_KEY_RENAMES`). This reunifies a split polygon (5.79 + 10.82 = 16.61 ac = condition file area). Attributes are copied from the target stand so the fragment is correctly classified as forest.

### iwc_id Format
- Yields1/3: `BH1427-1-1-TPA-XX-BA-XX-T1-19-T2-0-F1-0-F2-0` (stand-specific)
- Yields2: `SI80-1-U-LB-TPA-XX-BA-XX-T1-14-T2-19-F1-15-F2-20` (SI-based regen)

## Dependencies

```
pandas>=2.0, geopandas>=0.14, openpyxl>=3.1, pyogrio>=0.7, shapely>=2.0, numpy>=1.24
```

AIDB step additionally requires: `pyodbc`, `sqlalchemy` (with MS Access driver).

## Remaining Work

- **Zero-volume Yields1 stands**: 2 forest stands have Yields1 curves but all values are zero:
  - BH6112-1-205 (LB, Natural, Age 86, SI=47, 11.7 ac)
  - BH5132-1-119 (PH, Natural, Age 86, SI=50, 118.9 ac)
  - Fix: substitute a Yields2 regen curve with matching/similar SI, even though these are 1st-rotation stands outside the normal regen age window.
- **3 missing HH stands**: 3 Hard Hardwood forest stands have no Yields1 entry at all (~103 ac total, all in tract BH6125, Natural origin, age 11, TreatmentType=NO, no scheduled management):
  - BH6125-1-299 (SI=58, 68.9 ac)
  - BH6125-1-315 (SI=95 shapefile / 71 condition, 8.5 ac)
  - BH6125-1-316 (SI=50 shapefile / 38 condition, 25.8 ac)
  - Note: SI values differ between shapefile and condition file for 315 and 316.
  - TODO: Decide on approach — assign generic HH growth curve or leave as zero.
- **2025 actual disturbances**: Set up disturbances for activities that occurred in 2025 (prior to sim start year 2026). These are real/historical events, not projected.
- **AIDB editing**: Add all disturbance matrices to the AIDB:
  - Commercial thinning matrices (scaled from 50% thin template, ~40+ unique percentages)
  - Partial clearcut matrices (scaled from clearcut template, ~20 unique percentages)
  - Verify/add standard Clearcut and Site_Prep disturbance types
