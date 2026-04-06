import os
import re
import pandas as pd


# --- Required columns per location type ---

REQUIRED_STORE_COLS = ["name", "address", "lat", "lng", "ECC", "parentBranch", "PAR_LEVEL_Roll_Cart"]
REQUIRED_TRAILER_COLS = ["name", "parentBranch", "trailerLength", "trailerMake"]
REQUIRED_BRANCH_COLS = ["name", "parentBranch"]
VALID_TRAILER_LENGTHS = {28, 48, 53}

# Path to the static DC locations file shipped with the app
DC_LOCATIONS_FILE = os.path.join(os.path.dirname(__file__), "..", "app", "data", "dc_locations.csv")


def load_dc_locations(filepath=None):
    """
    Loads DC locations from a static CSV file (columns: id, name).
    Returns a DataFrame with location_name and location_id for use as reference data.

    :param filepath: Optional override path to the CSV file
    :return: DataFrame with columns: location_name, location_id
    """
    path = filepath or DC_LOCATIONS_FILE
    df = pd.read_csv(path)
    df = df.rename(columns={"id": "location_id", "name": "location_name"})
    return df[["location_name", "location_id"]]


def parse_upload(file, filename, sheet_name=None):
    """
    Parses an uploaded file (CSV or Excel) into a DataFrame.
    Normalizes column names by stripping whitespace and drops unnamed columns.

    :param file: File-like object (e.g., Streamlit UploadedFile)
    :param filename: Original filename (used to detect format)
    :param sheet_name: Optional sheet name to read from Excel files
    :return: DataFrame
    """
    if filename.endswith(".csv"):
        df = pd.read_csv(file)
    elif filename.endswith((".xlsx", ".xls")):
        # Use requested sheet if it exists, otherwise fall back to first sheet
        if sheet_name:
            xls = pd.ExcelFile(file)
            if sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
            else:
                df = pd.read_excel(xls, sheet_name=0)
        else:
            df = pd.read_excel(file)
    else:
        raise ValueError(f"Unsupported file format: {filename}. Use CSV or Excel (.xlsx).")

    df.columns = [col.strip() for col in df.columns]
    # Drop unnamed/extra columns that come from template formatting
    df = df.loc[:, ~df.columns.str.startswith("Unnamed:")]
    # Drop duplicate suffixed columns (e.g. parentBranch.1 from validation dropdowns)
    df = df.loc[:, ~df.columns.str.match(r".*\.\d+$")]
    # Drop rows where all values are empty
    df = df.dropna(how="all").reset_index(drop=True)
    return df


def validate_store_data(df, dc_locations_df):
    """
    Validates a store DataFrame before creation.

    Checks:
    - All required columns exist
    - name: alphanumeric only (letters, numbers, spaces), no blanks/NaN
    - name: no duplicates in input
    - address: no blanks/NaN, no duplicates in input
    - lat/lng are numeric, no blanks/NaN
    - ECC is numeric
    - PAR_LEVEL_Roll_Cart is numeric
    - All parentBranch values match a known DC

    :param df: Store DataFrame to validate
    :param dc_locations_df: DC reference DataFrame (from get_dc_locations)
    :return: list of error strings (empty = valid)
    """
    errors = []

    # Check required columns
    missing_cols = [col for col in REQUIRED_STORE_COLS if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return errors  # Can't validate further without the right columns

    # Check for empty DataFrame
    if len(df) == 0:
        errors.append("File contains no data rows.")
        return errors

    # --- Name checks ---
    # Blank / NaN names
    blank_names = df["name"].isna() | (df["name"].astype(str).str.strip() == "")
    blank_name_count = blank_names.sum()
    if blank_name_count:
        errors.append(f"{blank_name_count} rows have blank or empty names (will be skipped during creation).")

    # Names with non-alphanumeric characters (only letters, numbers, and spaces allowed)
    valid_names = df["name"].astype(str).str.strip()
    invalid_name_mask = ~blank_names & ~valid_names.str.match(r"^[A-Za-z0-9 ]+$")
    invalid_name_count = invalid_name_mask.sum()
    if invalid_name_count:
        bad_names = valid_names[invalid_name_mask].tolist()
        errors.append(f"{invalid_name_count} names contain special characters (only letters, numbers, and spaces allowed) — will be skipped: {bad_names}")

    # Duplicate names in input
    non_blank_names = valid_names[~blank_names]
    name_counts = non_blank_names.value_counts()
    duplicated_names = name_counts[name_counts > 1]
    if len(duplicated_names) > 0:
        dup_list = [f"{name} ({count}x)" for name, count in duplicated_names.items()]
        errors.append(f"Duplicate names found in input — only the first occurrence will be created, rest will be skipped: {', '.join(dup_list)}")

    # --- Address checks ---
    # Blank / NaN addresses
    blank_addresses = df["address"].isna() | (df["address"].astype(str).str.strip() == "")
    blank_addr_count = blank_addresses.sum()
    if blank_addr_count:
        errors.append(f"{blank_addr_count} rows have blank or empty addresses (will be skipped during creation).")

    # Duplicate addresses in input
    non_blank_addrs = df["address"].astype(str).str.strip().str.upper()
    non_blank_addrs_filtered = non_blank_addrs[~blank_addresses]
    addr_counts = non_blank_addrs_filtered.value_counts()
    duplicated_addrs = addr_counts[addr_counts > 1]
    if len(duplicated_addrs) > 0:
        dup_list = [f"{addr} ({count}x)" for addr, count in duplicated_addrs.items()]
        errors.append(f"Duplicate addresses found in input — only the first occurrence will be created, rest will be skipped: {', '.join(dup_list)}")

    # --- Numeric checks ---
    for col in ["lat", "lng"]:
        blank = df[col].isna() | (df[col].astype(str).str.strip() == "")
        blank_count = blank.sum()
        if blank_count:
            errors.append(f"{blank_count} rows have blank '{col}' values (will be skipped during creation).")
        non_numeric = pd.to_numeric(df[col], errors="coerce").isna() & ~blank
        count = non_numeric.sum()
        if count:
            errors.append(f"{count} rows have non-numeric '{col}' values.")

    for col in ["ECC", "PAR_LEVEL_Roll_Cart"]:
        non_numeric = pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
        count = non_numeric.sum()
        if count:
            errors.append(f"{count} rows have non-numeric '{col}' values.")

    # Check parentBranch against DC list (case-sensitive exact match)
    if len(dc_locations_df) > 0:
        known_dcs = set(dc_locations_df["location_name"])
        upload_branches = set(df["parentBranch"].dropna().str.strip().unique())
        unmatched = upload_branches - known_dcs
        if unmatched:
            errors.append(f"parentBranch values not found in DC list (case-sensitive): {sorted(unmatched)}")

    return errors


def validate_trailer_data(df, dc_locations_df):
    """
    Validates a trailer DataFrame before creation.

    Checks:
    - All required columns exist
    - name: alphanumeric + dashes only, must start with 'Truck', no blanks/NaN, no duplicates
    - trailerLength is numeric and in valid set (28, 48, 53)
    - trailerMake is alphabetic only (letters and spaces)
    - All parentBranch values match a known DC

    :param df: Trailer DataFrame to validate
    :param dc_locations_df: DC reference DataFrame (from get_dc_locations)
    :return: list of error strings (empty = valid)
    """
    errors = []

    # Check required columns
    missing_cols = [col for col in REQUIRED_TRAILER_COLS if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return errors

    # Check for empty DataFrame
    if len(df) == 0:
        errors.append("File contains no data rows.")
        return errors

    # --- Name checks ---
    # Blank / NaN names
    blank_names = df["name"].isna() | (df["name"].astype(str).str.strip() == "")
    blank_name_count = blank_names.sum()
    if blank_name_count:
        errors.append(f"{blank_name_count} rows have blank or empty names (will be skipped during creation).")

    # Names with invalid characters (only letters, numbers, and dashes allowed — no spaces)
    valid_names = df["name"].astype(str).str.strip()
    invalid_char_mask = ~blank_names & ~valid_names.str.match(r"^[A-Za-z0-9\-]+$")
    invalid_char_count = invalid_char_mask.sum()
    if invalid_char_count:
        bad_names = valid_names[invalid_char_mask].tolist()
        errors.append(f"{invalid_char_count} names contain invalid characters (only letters, numbers, and dashes allowed — no spaces) — will be skipped: {bad_names}")

    # Names missing the 'Truck' prefix
    missing_prefix_mask = ~blank_names & ~invalid_char_mask & ~valid_names.str.startswith("Truck")
    missing_prefix_count = missing_prefix_mask.sum()
    if missing_prefix_count:
        bad_names = valid_names[missing_prefix_mask].tolist()
        errors.append(f"{missing_prefix_count} names are missing the required 'Truck' prefix — will be skipped: {bad_names}")

    # Duplicate names in input
    non_blank_names = valid_names[~blank_names]
    name_counts = non_blank_names.value_counts()
    duplicated_names = name_counts[name_counts > 1]
    if len(duplicated_names) > 0:
        dup_list = [f"{name} ({count}x)" for name, count in duplicated_names.items()]
        errors.append(f"Duplicate names found in input — only the first occurrence will be created, rest will be skipped: {', '.join(dup_list)}")

    # --- trailerLength checks ---
    non_numeric = pd.to_numeric(df["trailerLength"], errors="coerce").isna() & df["trailerLength"].notna()
    count = non_numeric.sum()
    if count:
        errors.append(f"{count} rows have non-numeric 'trailerLength' values.")

    numeric_lengths = pd.to_numeric(df["trailerLength"], errors="coerce").dropna().astype(int)
    invalid_lengths = set(numeric_lengths.unique()) - VALID_TRAILER_LENGTHS
    if invalid_lengths:
        errors.append(f"Unexpected trailerLength values: {sorted(invalid_lengths)}. Expected: {sorted(VALID_TRAILER_LENGTHS)}")

    # --- trailerMake checks ---
    non_blank_makes = df["trailerMake"].astype(str).str.strip()
    blank_makes = df["trailerMake"].isna() | (non_blank_makes == "")
    invalid_make_mask = ~blank_makes & ~non_blank_makes.str.match(r"^[A-Za-z ]+$")
    invalid_make_count = invalid_make_mask.sum()
    if invalid_make_count:
        bad_makes = non_blank_makes[invalid_make_mask].tolist()
        errors.append(f"{invalid_make_count} trailerMake values contain non-alphabetic characters (only letters and spaces allowed): {bad_makes}")

    # Check parentBranch against DC list (case-sensitive exact match)
    if len(dc_locations_df) > 0:
        known_dcs = set(dc_locations_df["location_name"])
        upload_branches = set(df["parentBranch"].dropna().str.strip().unique())
        unmatched = upload_branches - known_dcs
        if unmatched:
            errors.append(f"parentBranch values not found in DC list (case-sensitive): {sorted(unmatched)}")

    return errors


def validate_branch_data(df, dc_locations_df):
    """
    Validates a branch-update DataFrame before updating labels.

    Checks:
    - All required columns exist
    - name: no blanks/NaN, no duplicates
    - parentBranch: no blanks/NaN
    - All parentBranch values match a known DC

    :param df: Branch DataFrame to validate
    :param dc_locations_df: DC reference DataFrame (from load_dc_locations)
    :return: list of error strings (empty = valid)
    """
    errors = []

    # Check required columns
    missing_cols = [col for col in REQUIRED_BRANCH_COLS if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")
        return errors

    # Check for empty DataFrame
    if len(df) == 0:
        errors.append("File contains no data rows.")
        return errors

    # --- Name checks ---
    blank_names = df["name"].isna() | (df["name"].astype(str).str.strip() == "")
    blank_name_count = blank_names.sum()
    if blank_name_count:
        errors.append(f"{blank_name_count} rows have blank or empty names (will be skipped during update).")

    # Duplicate names in input
    valid_names = df["name"].astype(str).str.strip()
    non_blank_names = valid_names[~blank_names]
    name_counts = non_blank_names.value_counts()
    duplicated_names = name_counts[name_counts > 1]
    if len(duplicated_names) > 0:
        dup_list = [f"{name} ({count}x)" for name, count in duplicated_names.items()]
        errors.append(f"Duplicate names found in input — only the first occurrence will be updated, rest will be skipped: {', '.join(dup_list)}")

    # --- parentBranch checks ---
    blank_branches = df["parentBranch"].isna() | (df["parentBranch"].astype(str).str.strip() == "")
    blank_branch_count = blank_branches.sum()
    if blank_branch_count:
        errors.append(f"{blank_branch_count} rows have blank or empty parentBranch values (will be skipped during update).")

    # Check parentBranch against DC list (case-sensitive exact match)
    if len(dc_locations_df) > 0:
        known_dcs = set(dc_locations_df["location_name"])
        upload_branches = set(df["parentBranch"].dropna().str.strip().unique())
        unmatched = upload_branches - known_dcs
        if unmatched:
            errors.append(f"parentBranch values not found in DC list (case-sensitive): {sorted(unmatched)}")

    return errors
