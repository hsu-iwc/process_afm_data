# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository processes forestry data for IWC (International Woodland Company) — specifically the "Boothill" tract in Georgia (378 stands, ~12,400 acres) — into inputs for the GCBM (Generic Carbon Budget Model) with the mojadata tiler.

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
| `config.py` | Paths, unit conversions, species/disturbance mappings, simulation params |
| `01_ingest.py` | Loads spatial, 3 yield CSVs, condition Excel, schedule; validates cross-source |
| `02_classifiers.py` | 5 GCBM classifiers: species, origin, si_class, growth_period, mgmt_trajectory |
| `03_yield_curves.py` | Converts yield tables to GCBM format (m3/ha); Yields1 for current, Yields2 for post-regen |
| `04_inventory.py` | Starting inventory GeoPackage (300 forest stands with classifiers + geometry) |
| `05_disturbances.py` | Extracts events, calculates thinning removal %, writes disturbances.gpkg |
| `06_transitions.py` | Post-disturbance yield curve transition rules |
| `07_aidb_thinning.py` | Adds thinning disturbance matrices to AIDB via `aidb_disturbance_manager.py` |
| `08_tiler_config.py` | Generates mojadata tiler script |
| `run_pipeline.py` | Orchestrates steps 01-08 |

### Output Files (`output/gcbm_input/`)

- `classifiers.csv` — SIT-format classifier definitions
- `yield_curves.csv` — Merchantable volume curves (m3/ha) by classifier combo, ages 1-78
- `inventory.gpkg` — Starting inventory with classifiers, age, historical disturbance, geometry
- `disturbances.gpkg` — All 1,386 disturbance events with year column, disturbance type, removal %
- `disturbance_events.csv` — SIT-format disturbance events with timestep
- `transition_rules.csv` — Post-disturbance classifier transitions
- `thinning_disturbance_mapping.csv` — Maps removal % to AIDB disturbance type names
- `tiler.py` — Mojadata tiler configuration script

## Key Data Concepts

### Yield Curve Sources
- **Yields1** — Stand-specific curves for the current (1st) rotation, keyed by `iwc_id` which encodes stand_key + management trajectory (T1/T2/F1/F2 ages).
- **Yields2** — Generic regeneration curves for post-clearcut (2nd) rotation, keyed by site index (SI50-SI100) + species (LB/LL/SL) + trajectory.
- **Yields3** — Thinning simulation overrides; same structure as Yields1 but with `qP_TOP4M3PA` (removed volume) populated. Pipe-delimited values at thin ages (before|after).

### Thinning Volume Removal Calculation
See `src/05_disturbances.py`. The schedule's TH9/TH10 columns (thin1/thin2) represent state BEFORE the action row, NOT the action parameter. The actual thin age comes from the AGE column. 73% of thinning events are 2nd-rotation (after clearcut) and must use Yields2.

### iwc_id Format
- Yields1/3: `BH1427-1-1-TPA-XX-BA-XX-T1-19-T2-0-F1-0-F2-0` (stand-specific)
- Yields2: `SI80-1-U-LB-TPA-XX-BA-XX-T1-14-T2-19-F1-15-F2-20` (SI-based regen)

## Dependencies

```
pandas>=2.0, geopandas>=0.14, openpyxl>=3.1, pyogrio>=0.7, shapely>=2.0, numpy>=1.24
```

AIDB step additionally requires: `pyodbc`, `sqlalchemy` (with MS Access driver).
