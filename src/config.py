"""
Project configuration for IWC Boothill GCBM input processing pipeline.
"""

from pathlib import Path

# =============================================================================
# PROJECT PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Source data
SPATIAL_DIR = PROJECT_ROOT / "spatial"
SHAPEFILE = SPATIAL_DIR / "IWC_FFF_NA_IWCBH_GA_20260115115551.shp"

YIELDS_DIR = PROJECT_ROOT / "yields" / "IWC_Shared_Folder"
YIELDS1_CSV = YIELDS_DIR / "Yields1_IWC_Formatted.csv"
YIELDS2_CSV = YIELDS_DIR / "Yields2_Regen_IWC_Formatted.csv"
YIELDS3_CSV = YIELDS_DIR / "Yields3_THINSIM_IWC_Formatted.csv"

CONDITION_XLSX = YIELDS_DIR / "IWC_Boothill_Condition.xlsx"

SCHEDULE_DIR = PROJECT_ROOT / "managment_schedule"
SCHEDULE_XLSX = SCHEDULE_DIR / "IWC_2025_BTHILL_HSM_DRAFT6.xlsx"
SCHEDULE_SHEET = "Activity rawdata"

# Output
OUTPUT_DIR = PROJECT_ROOT / "output" / "gcbm_input"
DISTURBANCE_DIR = OUTPUT_DIR / "disturbances"

# AIDB path (user provides at runtime; this is the default placeholder)
AIDB_PATH = None  # Set via CLI argument or environment variable

# =============================================================================
# UNIT CONVERSIONS
# =============================================================================

ACRES_TO_HA = 0.404686
M3_ACRE_TO_M3_HA = 2.47105  # 1/ACRES_TO_HA, converts m³/acre to m³/ha

# =============================================================================
# SIMULATION PARAMETERS
# =============================================================================

SIM_START_YEAR = 2026
SIM_END_YEAR = 2075
PROJECTION_LENGTH = 50  # years

# Maximum age in yield curves
MAX_AGE_YIELDS1 = 78
MAX_AGE_YIELDS2 = 50

# =============================================================================
# SPECIES CODE MAPPINGS
# =============================================================================

# IWC DOM_SPEC (long name in shapefile) -> DOMSPECLAB (short code)
DOM_SPEC_TO_CODE = {
    "Loblolly Pine": "LB",
    "Longleaf Pine": "LL",
    "Slash Pine": "SL",
    "Pine/Hardwood": "PH",
    "Hard Hardwood": "HH",
    "Soft Hardwood": "SH",
    "Cutover": "CO",
    "Undefined": "UD",
}

# Condition file species codes (includes cutover variants)
# CO-prefixed species represent cutover stands being converted
SPECIES_CODES = [
    "LB", "LL", "SL", "HH", "PH", "SH",
    "COLB", "COLL", "COSL",
    "CSLB", "CSLL", "CSSL",
    "UD",
]

# =============================================================================
# ORIGIN MAPPINGS
# =============================================================================

# Shapefile ORIGIN values -> Condition file codes
ORIGIN_LONG_TO_CODE = {
    "Planted": "PY",
    "Natural": "NN",
    "Open": "ONO",
}

# All origin codes used in Condition/Schedule
ORIGIN_CODES = ["PY", "NN", "NY", "OY", "ONO"]

# =============================================================================
# GROWTH TYPE CODES
# =============================================================================

GROWTH_TYPES = ["STP1", "STP2", "STP3", "STP4", "NOGROW"]

# =============================================================================
# DISTURBANCE ACTION MAPPINGS
# =============================================================================

# Management schedule ACTION -> GCBM disturbance type
ACTION_TO_DISTURBANCE = {
    "aHCC": "Clearcut",
    "aHTHIN1": "1st_Thin",
    "aHTHIN2": "2nd_Thin",
    "aSP": "Site_Prep",
}

# Actions that are NOT disturbances (effects embedded in yield curves)
NON_DISTURBANCE_ACTIONS = {"aPLT", "aFERTL", "aFERTM"}

# =============================================================================
# HISTORICAL DISTURBANCE MAPPING (by Origin code)
# =============================================================================

# Origin code -> (historical_disturbance_type, last_pass_disturbance_type)
HISTORICAL_DISTURBANCE_MAP = {
    "PY": ("Clearcut", "Clearcut"),
    "NN": ("Wildfire", "Wildfire"),
    "NY": ("Clearcut", "Clearcut"),
    "OY": ("Clearcut", "Site_Prep"),
    "ONO": ("Wildfire", "Wildfire"),  # Non-forested open land
}

# =============================================================================
# CLASSIFIER DEFINITIONS
# =============================================================================

CLASSIFIER_NAMES = [
    "species",
    "origin",
    "si_class",
    "growth_period",
    "mgmt_trajectory",
]

# growth_period values
GROWTH_PERIOD_CURRENT = "current"
GROWTH_PERIOD_POST_REGEN = "post_regen"

# SI class rounding interval
SI_CLASS_INTERVAL = 5

# =============================================================================
# SCHEDULE COLUMN MAPPINGS
# =============================================================================

# Activity rawdata columns -> semantic names
SCHEDULE_COLUMNS = {
    "TH1": "stand_key",
    "TH2": "species",
    "TH3": "origin",
    "TH4": "grow_type",
    "TH5": "si",
    "TH6": "fert0",
    "TH7": "fert1",
    "TH8": "fert2",
    "TH9": "thin1",
    "TH10": "thin2",
    "TH11": "zone",
    "TH12": "treatment_type",
    "TH13": "management_type",
}
