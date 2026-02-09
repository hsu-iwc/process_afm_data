"""
AIDB Manager
============
Reusable module for managing GCBM Archive Index Database (AIDB) entities:
- Disturbance types (scaled disturbance matrices)
- Species types (for yield curve assignment)

Disturbance Management:
    Automatically creates scaled disturbance matrices based on templates for:
    - Precommercial thinning (scaled from 85% template)
    - Commercial thinning / partial harvests (scaled from 50% template)

Species Management:
    Query and manage species types for yield curve assignment:
    - Get available species from AIDB
    - Create species-to-yield-curve mappings

Usage:
    from aidb_disturbance_manager import ensure_disturbances_exist, get_aidb_species

    # Disturbance management
    dist_types = ['30% precommercial thinning', '45.23% ct', '97% clear-cut']
    mapping = ensure_disturbances_exist(aidb_path, dist_types)

    # Species management
    species_df = get_aidb_species(aidb_path)
"""

import re
import os
import warnings
import pyodbc
import pandas as pd
from sqlalchemy import create_engine


# Suppress the pandas warning about pyodbc connections
warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy.*')


# =============================================================================
# CONFIGURATION
# =============================================================================

# Template disturbance matrices for scaling
TEMPLATES = {
    'precommercial': {
        'dmid': 20136,           # 85% precommercial thinning
        'base_pct': 0.85,
        'dm_structure_id': 2
    },
    'commercial': {
        'dmid': 20112,           # 50% commercial thinning
        'base_pct': 0.50,
        'dm_structure_id': 2
    }
}

# Standard disturbance types that should exist (verify but don't create)
STANDARD_TYPES = ['97% clear-cut', 'Planting']

# Disturbance matrix structure ID for biomass transfers
DM_STRUCTURE_ID = 2


# =============================================================================
# DATABASE CONNECTION (matches original notebook pattern)
# =============================================================================

def connect_aidb(db_path: str) -> pyodbc.Connection:
    """
    Establish connection to MS Access AIDB.
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found at {db_path}")

    connection_string = (
        f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};"
    )
    return pyodbc.connect(connection_string)


def get_sqlalchemy_engine(db_path: str):
    """
    Create SQLAlchemy engine for bulk inserts (matches original notebook pattern).
    """
    connection_string = (
        f"access+pyodbc:///?odbc_connect="
        f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={db_path};"
    )
    return create_engine(connection_string)


# =============================================================================
# HELPER FUNCTION (matches original notebook)
# =============================================================================

def fetch_max(cursor, query):
    """
    Executes a query to fetch the maximum value from a specified table.
    (Matches original notebook pattern)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] if result[0] is not None else 0


# =============================================================================
# DISTURBANCE TYPE PARSING
# =============================================================================

def parse_disturbance_type(dist_type: str) -> dict | None:
    """
    Parse a disturbance type string to extract percentage and category.

    Returns:
        dict with 'percent', 'category', 'name' keys, or None for standard types
    """
    dist_type = dist_type.strip()

    # Check for standard types (no scaling needed)
    if dist_type.lower() in [s.lower() for s in STANDARD_TYPES]:
        return None

    # Pattern: "X% precommercial thinning"
    match = re.match(r'^([\d.]+)%\s*precommercial\s*thinning$', dist_type, re.IGNORECASE)
    if match:
        return {
            'percent': float(match.group(1)) / 100,
            'category': 'precommercial',
            'name': dist_type
        }

    # Pattern: "X% commercial thinning"
    match = re.match(r'^([\d.]+)%\s*commercial\s*thinning$', dist_type, re.IGNORECASE)
    if match:
        return {
            'percent': float(match.group(1)) / 100,
            'category': 'commercial',
            'name': dist_type
        }

    # Pattern: "X% ct" (partial harvest / commercial thinning shorthand)
    match = re.match(r'^([\d.]+)%\s*ct$', dist_type, re.IGNORECASE)
    if match:
        return {
            'percent': float(match.group(1)) / 100,
            'category': 'commercial',
            'name': dist_type
        }

    # Pattern: "X% clear-cut" (treat as standard)
    match = re.match(r'^([\d.]+)%\s*clear-cut$', dist_type, re.IGNORECASE)
    if match:
        return None

    # Unknown pattern
    return None


# =============================================================================
# DATABASE QUERIES (matches original notebook patterns)
# =============================================================================

def get_existing_disturbances(conn: pyodbc.Connection) -> dict:
    """
    Get all existing disturbance types from AIDB.
    Returns dict mapping lowercase name -> info dict
    """
    # Query disturbance types (simple query like original notebook)
    df_dist = pd.read_sql("SELECT * FROM tblDisturbanceTypeDefault", con=conn)

    # Query DM associations to get DMID mapping
    df_assoc = pd.read_sql("SELECT * FROM tblDMAssociationDefault", con=conn)

    # Build result dict
    result = {}
    for _, row in df_dist.iterrows():
        name = row['DistTypeName']
        dist_type_id = row['DistTypeID']

        if name:
            key = name.lower().strip()

            # Find DMID from associations
            assoc_match = df_assoc[df_assoc['DefaultDisturbanceTypeID'] == dist_type_id]
            dmid = assoc_match['DMID'].iloc[0] if len(assoc_match) > 0 else None

            if key not in result:
                result[key] = {
                    'dist_type_id': dist_type_id,
                    'dmid': dmid,
                    'name': name
                }

    return result


# =============================================================================
# DISTURBANCE CREATION (matches original notebook logic exactly)
# =============================================================================

def create_scaled_disturbance(
    conn: pyodbc.Connection,
    db_path: str,
    name: str,
    target_pct: float,
    category: str,
    new_dmid: int,
    new_dist_type_id: int
) -> dict:
    """
    Create a new disturbance type by scaling a template matrix.
    (Logic matches original DisturbanceUpdateAIDB notebook)
    """
    template = TEMPLATES[category]
    copyID = template['dmid']
    startVal = template['base_pct']
    endVal = target_pct
    DMStructureID = template['dm_structure_id']

    Description = name
    StandReplacing = False

    # Get template matrix values with Proportion <> 1 (matches original notebook)
    df = pd.read_sql(
        "SELECT * FROM tblDMValuesLookup WHERE DMID = {} AND Proportion <> 1".format(copyID),
        con=conn
    )

    # Get sink pool transfers (non-diagonal elements)
    dfSinkPool = df.loc[df.DMRow != df.DMColumn, ['DMRow', 'DMColumn', 'Proportion']].copy()

    # Scale proportions (matches original: (endVal/startVal) * Proportion)
    dfSinkPool.loc[:, 'Proportion'] = (endVal / startVal) * dfSinkPool.Proportion

    # Calculate source pool retention (diagonal elements)
    dfSourcePool = dfSinkPool.groupby(['DMRow'])['Proportion'].sum().reset_index()
    dfSourcePool.loc[:, 'DMColumn'] = dfSourcePool.DMRow
    dfSourcePool.loc[:, 'Proportion'] = 1 - dfSourcePool.Proportion
    dfSourcePool = dfSourcePool[['DMRow', 'DMColumn', 'Proportion']]

    # Get stable pools (Proportion = 1) from template
    # Note: Original uses DMID=136, but that seems like a bug - should be copyID
    # Using copyID to be consistent
    dfStablePool = pd.read_sql(
        "SELECT DMRow, DMColumn, Proportion FROM tblDMValuesLookup WHERE DMID = {} AND Proportion = 1".format(copyID),
        con=conn
    )

    # Combine all matrix values
    dfDMValuesUpdate = pd.concat([dfStablePool, dfSinkPool, dfSourcePool])

    # Validate: each row should sum to 1
    chk = dfDMValuesUpdate.groupby("DMRow")["Proportion"].sum().values
    if any(abs(value - 1) > 0.001 for value in chk):
        print(f"WARNING: Matrix row sums don't equal 1 for {name}")

    dfDMValuesUpdate.loc[:, 'DMID'] = new_dmid

    # Prepare tblDM update
    dfDMUpdate = pd.DataFrame.from_records({
        "DMID": [new_dmid],
        "Name": [name],
        "Description": [Description],
        "DMStructureID": [DMStructureID]
    })

    # Prepare tblDisturbanceTypeDefault update
    dfDistTypUpdate = pd.DataFrame.from_records({
        "DistTypeID": [new_dist_type_id],
        "DistTypeName": [name],
        "OnOffSwitch": [True],
        "Description": [Description],
        "IsStandReplacing": [StandReplacing],
        "IsMultiYear": [False],
        "MultiYearCount": [0]
    })

    # Prepare tblDMAssociationDefault update
    df_eco = pd.read_sql("SELECT EcoBoundaryID, EcoBoundaryName FROM tblEcoBoundaryDefault", con=conn)
    df_eco.loc[:, "DefaultDisturbanceTypeID"] = new_dist_type_id
    df_eco.loc[:, "AnnualOrder"] = 1
    df_eco.loc[:, "DMID"] = new_dmid
    df_eco.loc[:, "DefaultEcoBoundaryID"] = df_eco.EcoBoundaryID
    df_eco.loc[:, "Name"] = name + "-" + df_eco.EcoBoundaryName
    df_eco.loc[:, "Description"] = Description

    del df_eco['EcoBoundaryID'], df_eco['EcoBoundaryName']

    # Write to database using SQLAlchemy engine (matches original notebook)
    engine = get_sqlalchemy_engine(db_path)

    with engine.begin() as c:
        dfDMValuesUpdate.to_sql("tblDMValuesLookup", con=c, if_exists="append", index=False)
        dfDMUpdate.to_sql("tblDM", con=c, if_exists="append", index=False)
        dfDistTypUpdate.to_sql("tblDisturbanceTypeDefault", con=c, if_exists="append", index=False)
        df_eco.to_sql("tblDMAssociationDefault", con=c, if_exists="append", index=False)

    del engine

    return {
        'dmid': new_dmid,
        'dist_type_id': new_dist_type_id
    }


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def ensure_disturbances_exist(
    db_path: str,
    disturbance_specs: list,
    dry_run: bool = False
) -> dict:
    """
    Ensure all disturbance types exist in the AIDB, creating missing ones.

    Parameters:
        db_path: Path to the AIDB .mdb or .accdb file
        disturbance_specs: List of dicts, each with:
            - 'name': str - The disturbance type name (e.g., "30% precommercial thinning")
            - 'percent': float - The removal percentage as decimal (e.g., 0.30 for 30%)
            - 'category': str - Either 'precommercial' or 'commercial'

            For standard types that don't need scaling (e.g., "97% clear-cut", "Planting"),
            you can pass just {'name': '...'} with percent and category omitted.

        dry_run: If True, report what would be created without making changes

    Returns:
        dict mapping disturbance name -> {
            'dmid': int or None,
            'dist_type_id': int or None,
            'created': bool,
            'category': str or None
        }

    Example:
        specs = [
            {'name': '30% precommercial thinning', 'percent': 0.30, 'category': 'precommercial'},
            {'name': '45.23% ct', 'percent': 0.4523, 'category': 'commercial'},
            {'name': '97% clear-cut'},  # Standard type, no scaling needed
            {'name': 'Planting'},       # Standard type, no scaling needed
        ]
        result = ensure_disturbances_exist(aidb_path, specs)
    """
    conn = connect_aidb(db_path)

    try:
        # Get existing disturbances
        existing = get_existing_disturbances(conn)

        # Get current max IDs (matches original notebook pattern)
        cursor = conn.cursor()
        base_dmid = fetch_max(cursor, "SELECT MAX(DMID) FROM tblDMValuesLookup")
        base_dist_type_id = fetch_max(cursor, "SELECT MAX(DistTypeID) FROM tblDisturbanceTypeDefault")
        cursor.close()

        idx = 0  # Counter for new IDs (matches original notebook pattern)
        result = {}
        warnings_list = []

        for spec in disturbance_specs:
            name = spec.get('name', '').strip()
            percent = spec.get('percent')
            category = spec.get('category')

            if not name:
                continue

            key = name.lower()

            # Check if already exists
            if key in existing:
                result[name] = {
                    'dmid': existing[key]['dmid'],
                    'dist_type_id': existing[key]['dist_type_id'],
                    'created': False,
                    'category': category
                }
                continue

            # If no percent/category provided, it's a standard type that should already exist
            if percent is None or category is None:
                warnings_list.append(f"'{name}' not found in AIDB (no percent/category provided for creation)")
                result[name] = {
                    'dmid': None,
                    'dist_type_id': None,
                    'created': False,
                    'category': None,
                    'warning': "Not found and no scaling parameters provided"
                }
                continue

            # Validate category
            if category not in TEMPLATES:
                warnings_list.append(f"Unknown category '{category}' for '{name}'. Must be 'precommercial' or 'commercial'.")
                result[name] = {
                    'dmid': None,
                    'dist_type_id': None,
                    'created': False,
                    'category': category,
                    'warning': f"Unknown category: {category}"
                }
                continue

            # Increment counter for new IDs
            idx += 1
            new_dmid = base_dmid + idx
            new_dist_type_id = base_dist_type_id + idx

            # Create the disturbance type
            if dry_run:
                result[name] = {
                    'dmid': new_dmid,
                    'dist_type_id': new_dist_type_id,
                    'created': True,
                    'category': category,
                    'dry_run': True
                }
                print(f"[DRY RUN] Would create: {name} (DMID: {new_dmid}, DistTypeID: {new_dist_type_id})")
            else:
                created_info = create_scaled_disturbance(
                    conn=conn,
                    db_path=db_path,
                    name=name,
                    target_pct=percent,
                    category=category,
                    new_dmid=new_dmid,
                    new_dist_type_id=new_dist_type_id
                )
                result[name] = {
                    'dmid': created_info['dmid'],
                    'dist_type_id': created_info['dist_type_id'],
                    'created': True,
                    'category': category
                }
                print(f"Created: {name} (DMID: {new_dmid}, DistTypeID: {new_dist_type_id})")

        # Print warnings
        for warning in warnings_list:
            print(f"WARNING: {warning}")

        return result

    finally:
        conn.close()


# =============================================================================
# SPECIES MANAGEMENT
# =============================================================================

def get_aidb_species(db_path: str) -> pd.DataFrame:
    """
    Query species types from AIDB.

    Parameters:
        db_path: Path to the AIDB .mdb or .accdb file

    Returns:
        DataFrame with SpeciesTypeID and SpeciesTypeName columns
    """
    conn = connect_aidb(db_path)
    try:
        df = pd.read_sql(
            "SELECT SpeciesTypeID, SpeciesTypeName FROM tblSpeciesTypeDefault ORDER BY SpeciesTypeName",
            con=conn
        )
        return df
    finally:
        conn.close()


def get_species_with_hierarchy(db_path: str) -> pd.DataFrame:
    """
    Query species with genus and forest type context from AIDB.

    This provides additional context for species selection by including
    the genus name and forest type name for each species.

    Parameters:
        db_path: Path to the AIDB .mdb or .accdb file

    Returns:
        DataFrame with SpeciesTypeID, SpeciesTypeName, GenusName, ForestTypeName columns
    """
    conn = connect_aidb(db_path)
    try:
        # Query with joins to get hierarchy context
        query = """
            SELECT
                s.SpeciesTypeID,
                s.SpeciesTypeName,
                g.GenusName,
                f.ForestTypeName
            FROM tblSpeciesTypeDefault s
            LEFT JOIN tblGenusTypeDefault g ON s.GenusTypeID = g.GenusTypeID
            LEFT JOIN tblForestTypeDefault f ON g.ForestTypeID = f.ForestTypeID
            ORDER BY f.ForestTypeName, g.GenusName, s.SpeciesTypeName
        """
        df = pd.read_sql(query, con=conn)
        return df
    finally:
        conn.close()


def get_eco_boundaries(db_path: str) -> pd.DataFrame:
    """
    Query ecological boundaries from AIDB.

    Parameters:
        db_path: Path to the AIDB .mdb or .accdb file

    Returns:
        DataFrame with EcoBoundaryID and EcoBoundaryName columns
    """
    conn = connect_aidb(db_path)
    try:
        df = pd.read_sql(
            "SELECT EcoBoundaryID, EcoBoundaryName FROM tblEcoBoundaryDefault ORDER BY EcoBoundaryName",
            con=conn
        )
        return df
    finally:
        conn.close()


def get_species_by_eco_boundary(db_path: str, eco_boundary_id: int = None) -> pd.DataFrame:
    """
    Query species types available for a given ecological boundary.

    Only returns SpeciesType entries (not Genus or ForestType) that have
    biomass parameters defined for the specified ecological region.

    Parameters:
        db_path: Path to the AIDB .mdb or .accdb file
        eco_boundary_id: EcoBoundaryID to filter by (if None, returns all species)

    Returns:
        DataFrame with EcoBoundaryID, EcoBoundaryName, SpeciesTypeID, SpeciesTypeName columns
    """
    conn = connect_aidb(db_path)
    try:
        # MS Access requires parentheses around multiple JOINs
        # Use subquery approach that works with Access SQL
        query = """
            SELECT DISTINCT
                tblSPUDefault.EcoBoundaryID,
                tblEcoBoundaryDefault.EcoBoundaryName,
                merged.DefaultID AS SpeciesTypeID,
                merged.Name AS SpeciesTypeName
            FROM
                ((tblSPUDefault
                LEFT JOIN tblEcoBoundaryDefault
                    ON tblSPUDefault.EcoBoundaryID = tblEcoBoundaryDefault.EcoBoundaryID)
                LEFT JOIN
                (
                    SELECT
                        tblBioTotalStemwoodSpeciesTypeDefault.DefaultSPUID,
                        tblBioTotalStemwoodSpeciesTypeDefault.DefaultSpeciesTypeID AS DefaultID,
                        tblSpeciesTypeDefault.SpeciesTypeName AS Name
                    FROM tblBioTotalStemwoodSpeciesTypeDefault
                    LEFT JOIN tblSpeciesTypeDefault
                        ON tblBioTotalStemwoodSpeciesTypeDefault.DefaultSpeciesTypeID = tblSpeciesTypeDefault.SpeciesTypeID
                ) AS merged
                ON tblSPUDefault.SPUID = merged.DefaultSPUID)
            WHERE merged.DefaultID IS NOT NULL
        """

        if eco_boundary_id is not None:
            query += f" AND tblSPUDefault.EcoBoundaryID = {eco_boundary_id}"

        query += " ORDER BY merged.Name"

        df = pd.read_sql(query, con=conn)
        return df
    finally:
        conn.close()


# =============================================================================
# TESTING / CLI
# =============================================================================

if __name__ == "__main__":
    # Example of how to use the module
    print("Example disturbance specifications:")
    print("-" * 50)

    example_specs = [
        {'name': '30% precommercial thinning', 'percent': 0.30, 'category': 'precommercial'},
        {'name': '45.23% ct', 'percent': 0.4523, 'category': 'commercial'},
        {'name': '50% commercial thinning', 'percent': 0.50, 'category': 'commercial'},
        {'name': '97% clear-cut'},  # Standard type, no scaling
        {'name': 'Planting'},       # Standard type, no scaling
    ]

    for spec in example_specs:
        if 'percent' in spec:
            print(f"  {spec['name']} -> {spec['percent']*100:.2f}% {spec['category']}")
        else:
            print(f"  {spec['name']} -> (standard type, no scaling)")

    print()
    print("To use: ensure_disturbances_exist(aidb_path, example_specs)")
    print()
    print("Legacy parse_disturbance_type() helper still available:")
    print("-" * 50)
    test_cases = ["30% precommercial thinning", "45.23% ct", "Planting"]
    for test in test_cases:
        result = parse_disturbance_type(test)
        print(f"  '{test}' -> {result}")
