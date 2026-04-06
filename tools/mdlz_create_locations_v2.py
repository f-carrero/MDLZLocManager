import json
import os
import re
import time
from collections import defaultdict

import pandas as pd
from wiliot_api.platform.platform import LocationType, EntityType

# Label IDs for branch update operations
PARENT_BRANCH_LABEL_ID = "757ed2f1-0a3f-4fce-afa6-b023403f42d6"
ASSOCIATE_DC_LABEL_ID = "3f3d92ab-df29-469a-88bf-c8a7dc292794"

# --- Adaptive delay configuration ---
MIN_DELAY = 0.2          # Minimum delay between API calls (seconds)
DEFAULT_DELAY = 0.5      # Starting delay
MAX_DELAY = 10.0          # Maximum delay after repeated errors
DELAY_INCREASE_FACTOR = 1.5  # Multiply delay by this on error
DELAY_DECREASE_FACTOR = 0.9  # Multiply delay by this on consecutive successes
SUCCESSES_TO_DECREASE = 10   # Decrease delay after this many consecutive successes

# Checkpoint directory
CHECKPOINT_DIR = os.path.join(os.path.dirname(__file__), "..", ".tmp")


class AdaptiveRateLimiter:
    """Tracks success/error rates and adjusts inter-request delay dynamically."""

    def __init__(self, initial_delay=DEFAULT_DELAY):
        self.delay = initial_delay
        self.consecutive_successes = 0

    def record_success(self):
        self.consecutive_successes += 1
        if self.consecutive_successes >= SUCCESSES_TO_DECREASE:
            self.delay = max(MIN_DELAY, self.delay * DELAY_DECREASE_FACTOR)
            self.consecutive_successes = 0

    def record_error(self):
        self.consecutive_successes = 0
        self.delay = min(MAX_DELAY, self.delay * DELAY_INCREASE_FACTOR)

    def wait(self):
        time.sleep(self.delay)


def _retry_api_call(func, max_retries=3, base_delay=2):
    """Retry an API call with exponential backoff and rate-limit awareness.

    The Wiliot SDK handles HTTP 429 internally (sleeps on Retry-After header).
    This wrapper handles transient errors that propagate as exceptions.
    """
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            err_str = str(e).lower()
            is_rate_limit = "429" in err_str or "rate" in err_str or "throttl" in err_str
            is_last_attempt = attempt == max_retries - 1

            if is_last_attempt:
                raise

            if is_rate_limit:
                delay = base_delay * (3 ** attempt)  # More aggressive backoff for rate limits
                print(f"  Rate limit hit, retry {attempt + 1}/{max_retries} after {delay}s")
            else:
                delay = base_delay * (2 ** attempt)
                print(f"  Retry {attempt + 1}/{max_retries} after {delay}s: {e}")

            time.sleep(delay)


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def _checkpoint_path(job_type, job_id):
    """Return the path for a checkpoint file."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    return os.path.join(CHECKPOINT_DIR, f"checkpoint_{job_type}_{job_id}.json")


def _save_checkpoint(path, processed_indices, results):
    """Persist progress so a crashed job can resume."""
    data = {
        "processed_indices": list(processed_indices),
        "results": results,
        "timestamp": time.time(),
    }
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _load_checkpoint(path):
    """Load a checkpoint if it exists and is recent (< 24 hours)."""
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        age_hours = (time.time() - data.get("timestamp", 0)) / 3600
        if age_hours > 24:
            os.remove(path)
            return None
        return data
    except (json.JSONDecodeError, KeyError):
        os.remove(path)
        return None


def _clear_checkpoint(path):
    """Remove a checkpoint file after successful completion."""
    if os.path.exists(path):
        os.remove(path)


def _make_job_id(df, job_type):
    """Generate a deterministic job ID from DataFrame content for checkpoint matching."""
    import hashlib
    content = f"{job_type}_{len(df)}_{list(df.columns)}"
    if len(df) > 0:
        first = df.iloc[0].to_dict()
        last = df.iloc[-1].to_dict()
        content += f"_{first}_{last}"
    return hashlib.md5(content.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Store creation
# ---------------------------------------------------------------------------

def create_store_locations(pc, mdlz_stores, mdlz_dc_locations, on_progress=None):
    """
    Creates platform locations for stores from the mdlz_stores DataFrame.
    Each location gets labels: ECC, locationType, parentBranch, ASSOCIATE_DC, PAR_LEVEL_Roll_Cart.
    ASSOCIATE_DC is resolved by matching parentBranch to mdlz_dc_locations.location_name.

    Features:
    - Adaptive rate limiting (speeds up on success, slows down on errors)
    - Checkpoint/resume for interrupted jobs
    - Smart retry with rate-limit detection

    :param pc: An already-instantiated PlatformClient
    :param mdlz_stores: DataFrame with columns: name, address, lat, lng, ECC,
                         parentBranch, PAR_LEVEL_Roll_Cart
    :param mdlz_dc_locations: DataFrame with columns: location_name, location_id
    :param on_progress: Optional callback(current, total, result_dict) for UI updates
    :return: DataFrame with creation results
    """
    mdlz_stores = mdlz_stores.copy()
    mdlz_stores = mdlz_stores.merge(
        mdlz_dc_locations[["location_name", "location_id"]],
        how="left",
        left_on="parentBranch",
        right_on="location_name",
    )
    mdlz_stores.rename(columns={"location_id": "ASSOCIATE_DC"}, inplace=True)

    unmatched = mdlz_stores["ASSOCIATE_DC"].isna().sum()
    if unmatched:
        print(f"WARNING: {unmatched} stores have no matching DC in mdlz_dc_locations\n")

    # Fetch existing locations to prevent duplicates (names and addresses)
    print("Fetching existing locations from platform...")
    existing_locations = pc.get_locations()
    existing_names = {loc["name"] for loc in existing_locations}
    existing_addresses = {loc.get("address", "").strip().upper() for loc in existing_locations if loc.get("address")}
    print(f"Found {len(existing_names)} existing locations on platform")
    print(f"Found {len(mdlz_stores)} stores to process\n")

    REQUIRED_STORE_COLS = ["name", "address", "lat", "lng", "ECC", "parentBranch", "ASSOCIATE_DC", "PAR_LEVEL_Roll_Cart"]

    # --- Checkpoint: resume if previous run was interrupted ---
    job_id = _make_job_id(mdlz_stores, "store")
    cp_path = _checkpoint_path("store", job_id)
    checkpoint = _load_checkpoint(cp_path)

    results = []
    already_processed = set()
    if checkpoint:
        results = checkpoint["results"]
        already_processed = set(checkpoint["processed_indices"])
        print(f"Resuming from checkpoint: {len(already_processed)} rows already processed")

    skipped = 0
    processed = len(already_processed)
    total = len(mdlz_stores)
    seen_names_in_file = set()
    seen_addresses_in_file = set()
    rate_limiter = AdaptiveRateLimiter()

    # Rebuild seen-sets from checkpoint results
    for r in results:
        name = r.get("store_name", "")
        if name:
            seen_names_in_file.add(name)

    def _skip(idx, row, name, reason):
        nonlocal skipped, processed
        skipped += 1
        processed += 1
        print(f"  SKIP: {name} — {reason}")
        result = {"ecc": str(row.get("ECC", "")), "store_name": name, "location_id": None, "status": "skipped", "reason": reason}
        results.append(result)
        already_processed.add(idx)
        if on_progress:
            on_progress(processed, total, result)

    for idx, row in mdlz_stores.iterrows():
        if idx in already_processed:
            continue

        raw_name = str(row.get("name", "")).strip()

        # Skip rows with missing required fields
        missing = [c for c in REQUIRED_STORE_COLS if pd.isna(row.get(c)) or str(row.get(c)).strip() == ""]
        if missing:
            _skip(idx, row, raw_name or "unknown", f"missing {missing}")
            continue

        name = raw_name

        # Skip names with non-alphanumeric characters
        if not re.match(r"^[A-Za-z0-9 ]+$", name):
            _skip(idx, row, name, "name contains special characters (only letters, numbers, spaces allowed)")
            continue

        # Skip duplicate names within the input file
        if name in seen_names_in_file:
            _skip(idx, row, name, "duplicate name in input file")
            continue
        seen_names_in_file.add(name)

        address = str(row["address"]).strip()
        address_upper = address.upper()

        # Skip duplicate addresses within the input file
        if address_upper in seen_addresses_in_file:
            _skip(idx, row, name, "duplicate address in input file")
            continue
        seen_addresses_in_file.add(address_upper)

        # Skip if address already exists on platform
        if address_upper in existing_addresses:
            _skip(idx, row, name, f"address already exists on platform: {address}")
            continue

        lat = float(row["lat"])
        lng = float(row["lng"])
        ecc = str(int(row["ECC"]))
        parent_branch = str(row["parentBranch"]).strip()
        associate_dc = str(row["ASSOCIATE_DC"]).strip()
        par_level = str(int(row["PAR_LEVEL_Roll_Cart"]))

        # Skip if location name already exists on platform
        if name in existing_names:
            print(f"  SKIP (duplicate): {name} already exists on platform")
            processed += 1
            result = {"ecc": ecc, "store_name": name, "location_id": None, "status": "skipped", "reason": "name already exists on platform"}
            results.append(result)
            already_processed.add(idx)
            if on_progress:
                on_progress(processed, total, result)
            continue

        print(f"[{ecc}] Creating store location: {name}")

        result = None
        try:
            location = _retry_api_call(lambda: pc.create_location(
                location_type=LocationType.SITE,
                name=name,
                lat=lat,
                lng=lng,
                address=address,
                country="US",
            ))
            location_id = location["id"]
            print(f"  Location created: {location_id}")

            _retry_api_call(lambda: pc.set_keys_values_for_entities(
                entity_type=EntityType.LOCATION,
                entity_ids=[location_id],
                keys_values={
                    "ECC": ecc,
                    "siteType": "store",
                    "parentBranch": parent_branch,
                    "ASSOCIATE_DC": associate_dc,
                    "PAR_LEVEL_Roll_Cart": par_level,
                },
                overwrite_existing=True,
            ))
            print(f"  Labels set: ECC={ecc}, siteType=store, parentBranch={parent_branch}, ASSOCIATE_DC={associate_dc}, PAR_LEVEL_Roll_Cart={par_level}")

            # Track the new name so duplicates within this run are caught
            existing_names.add(name)
            existing_addresses.add(address_upper)

            result = {
                "ecc": ecc,
                "store_name": name,
                "location_id": location_id,
                "status": "success",
            }
            rate_limiter.record_success()

        except Exception as e:
            print(f"  ERROR: {e}")
            result = {
                "ecc": ecc,
                "store_name": name,
                "location_id": None,
                "status": f"error: {e}",
            }
            rate_limiter.record_error()

        results.append(result)
        processed += 1
        already_processed.add(idx)
        if on_progress:
            on_progress(processed, total, result)

        # Save checkpoint every 10 rows
        if processed % 10 == 0:
            _save_checkpoint(cp_path, already_processed, results)

        rate_limiter.wait()

    # Final checkpoint save + cleanup
    _clear_checkpoint(cp_path)

    success = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - success
    if skipped:
        print(f"\nSkipped {skipped} rows with missing required values.")
    print(f"Done. {success} succeeded, {failed} failed.")
    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Trailer creation
# ---------------------------------------------------------------------------

def create_trailer_locations(pc, mdlz_trailers, mdlz_dc_locations, on_progress=None):
    """
    Creates TRANSPORTER locations for trailers from the mdlz_trailers DataFrame.
    Each location gets labels for parentBranch, ASSOCIATE_DC, trailerLength, and trailerMake.
    ASSOCIATE_DC is resolved by matching parentBranch to mdlz_dc_locations.location_name.
    Each location gets a zone with template_name based on trailerLength:
      - 28 -> Standard_truck_-_2_bridges
      - 48 or 53 -> Standard_truck

    Features:
    - Adaptive rate limiting (speeds up on success, slows down on errors)
    - Checkpoint/resume for interrupted jobs
    - Smart retry with rate-limit detection

    :param pc: An already-instantiated PlatformClient
    :param mdlz_trailers: DataFrame with columns: name, parentBranch, trailerLength, trailerMake
    :param mdlz_dc_locations: DataFrame with columns: location_name, location_id
    :param on_progress: Optional callback(current, total, result_dict) for UI updates
    :return: DataFrame with creation results
    """
    mdlz_trailers = mdlz_trailers.copy()
    mdlz_trailers = mdlz_trailers.merge(
        mdlz_dc_locations[["location_name", "location_id"]],
        how="left",
        left_on="parentBranch",
        right_on="location_name",
    )
    mdlz_trailers.rename(columns={"location_id": "ASSOCIATE_DC"}, inplace=True)

    unmatched = mdlz_trailers["ASSOCIATE_DC"].isna().sum()
    if unmatched:
        print(f"WARNING: {unmatched} trailers have no matching DC in mdlz_dc_locations\n")

    # Fetch existing location names to prevent duplicates
    print("Fetching existing locations from platform...")
    existing_locations = pc.get_locations()
    existing_names = {loc["name"] for loc in existing_locations}
    print(f"Found {len(existing_names)} existing locations on platform")

    TEMPLATE_MAP = {
        28: "Standard_truck_-_2_bridges",
        48: "Standard_truck",
        53: "Standard_truck",
    }

    print(f"Found {len(mdlz_trailers)} trailers to process\n")

    REQUIRED_TRAILER_COLS = ["name", "parentBranch", "trailerLength", "trailerMake", "ASSOCIATE_DC"]

    # --- Checkpoint: resume if previous run was interrupted ---
    job_id = _make_job_id(mdlz_trailers, "trailer")
    cp_path = _checkpoint_path("trailer", job_id)
    checkpoint = _load_checkpoint(cp_path)

    results = []
    already_processed = set()
    if checkpoint:
        results = checkpoint["results"]
        already_processed = set(checkpoint["processed_indices"])
        print(f"Resuming from checkpoint: {len(already_processed)} rows already processed")

    skipped = 0
    processed = len(already_processed)
    total = len(mdlz_trailers)
    seen_names_in_file = set()
    rate_limiter = AdaptiveRateLimiter()

    # Rebuild seen-sets from checkpoint results
    for r in results:
        name = r.get("location_name", "")
        if name:
            seen_names_in_file.add(name)

    def _skip(idx, row, name, reason):
        nonlocal skipped, processed
        skipped += 1
        processed += 1
        zone_name = name.replace("Truck", "", 1) if name.startswith("Truck") else name
        print(f"  SKIP: {name} — {reason}")
        result = {"unit_num": zone_name, "location_name": name, "location_id": None, "zone_id": None, "status": "skipped", "reason": reason}
        results.append(result)
        already_processed.add(idx)
        if on_progress:
            on_progress(processed, total, result)

    for idx, row in mdlz_trailers.iterrows():
        if idx in already_processed:
            continue

        raw_name = str(row.get("name", "")).strip()

        # Skip rows with missing required fields
        missing = [c for c in REQUIRED_TRAILER_COLS if pd.isna(row.get(c)) or str(row.get(c)).strip() == ""]
        if missing:
            _skip(idx, row, raw_name or "unknown", f"missing {missing}")
            continue

        name = raw_name

        # Skip names with invalid characters (only letters, numbers, dashes — no spaces)
        if not re.match(r"^[A-Za-z0-9\-]+$", name):
            _skip(idx, row, name, "name contains invalid characters (only letters, numbers, and dashes allowed — no spaces)")
            continue

        # Skip names missing the 'Truck' prefix
        if not name.startswith("Truck"):
            _skip(idx, row, name, "name missing required 'Truck' prefix")
            continue

        # Skip non-alphabetic trailerMake
        trailer_make_raw = str(row["trailerMake"]).strip()
        if not re.match(r"^[A-Za-z ]+$", trailer_make_raw):
            _skip(idx, row, name, f"trailerMake contains non-alphabetic characters: {trailer_make_raw}")
            continue

        # Skip duplicate names within the input file
        if name in seen_names_in_file:
            _skip(idx, row, name, "duplicate name in input file")
            continue
        seen_names_in_file.add(name)

        parent_branch = str(row["parentBranch"]).strip()
        trailer_length = int(row["trailerLength"])
        trailer_make = trailer_make_raw
        associate_dc = str(row["ASSOCIATE_DC"]).strip()

        zone_name = name.replace("Truck", "", 1)
        template_name = TEMPLATE_MAP.get(trailer_length, "Standard_truck")

        # Skip if location name already exists on platform
        if name in existing_names:
            _skip(idx, row, name, "name already exists on platform")
            continue

        print(f"[{zone_name}] Creating transporter: {name}")

        result = None
        try:
            location = _retry_api_call(lambda: pc.create_location(
                location_type=LocationType.TRANSPORTER,
                name=name,
            ))
            location_id = location["id"]
            print(f"  Location created: {location_id}")

            _retry_api_call(lambda: pc.set_keys_values_for_entities(
                entity_type=EntityType.LOCATION,
                entity_ids=[location_id],
                keys_values={
                    "parentBranch": parent_branch,
                    "ASSOCIATE_DC": associate_dc,
                    "trailerLength": str(trailer_length),
                    "trailerMake": trailer_make,
                },
                overwrite_existing=True,
            ))
            print(f"  Labels set: parentBranch={parent_branch}, ASSOCIATE_DC={associate_dc}, trailerLength={trailer_length}, trailerMake={trailer_make}")

            zone = _retry_api_call(lambda: pc.create_zone(name=zone_name, location_id=location_id))
            zone_id = zone["id"]
            print(f"  Zone created: {zone_id}")

            _retry_api_call(lambda: pc.set_keys_values_for_entities(
                entity_type=EntityType.ZONE,
                entity_ids=[zone_id],
                keys_values={"template_name": template_name},
                overwrite_existing=True,
            ))
            print(f"  Zone label set: template_name={template_name}")

            # Track the new name so duplicates within this run are caught
            existing_names.add(name)

            result = {
                "unit_num": zone_name,
                "location_name": name,
                "location_id": location_id,
                "zone_id": zone_id,
                "status": "success",
            }
            rate_limiter.record_success()

        except Exception as e:
            print(f"  ERROR: {e}")
            result = {
                "unit_num": zone_name,
                "location_name": name,
                "location_id": None,
                "zone_id": None,
                "status": f"error: {e}",
            }
            rate_limiter.record_error()

        results.append(result)
        processed += 1
        already_processed.add(idx)
        if on_progress:
            on_progress(processed, total, result)

        # Save checkpoint every 10 rows
        if processed % 10 == 0:
            _save_checkpoint(cp_path, already_processed, results)

        rate_limiter.wait()

    # Final checkpoint save + cleanup
    _clear_checkpoint(cp_path)

    success = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - success
    if skipped:
        print(f"\nSkipped {skipped} rows with missing required values.")
    print(f"Done. {success} succeeded, {failed} failed.")
    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Branch label updates (with batching)
# ---------------------------------------------------------------------------

def update_branch_labels(pc, mdlz_branches, mdlz_dc_locations, on_progress=None):
    """
    Updates parentBranch and ASSOCIATE_DC labels for existing locations.
    ASSOCIATE_DC is resolved by matching parentBranch to mdlz_dc_locations.location_name.

    Groups locations that share the same (parentBranch, ASSOCIATE_DC) values and
    updates them in a single batched API call for efficiency.

    Features:
    - Batched label updates (groups locations with same label values)
    - Adaptive rate limiting
    - Checkpoint/resume for interrupted jobs
    - Smart retry with rate-limit detection

    :param pc: An already-instantiated PlatformClient
    :param mdlz_branches: DataFrame with columns: name, parentBranch
    :param mdlz_dc_locations: DataFrame with columns: location_name, location_id
    :param on_progress: Optional callback(current, total, result_dict) for UI updates
    :return: DataFrame with update results
    """
    mdlz_branches = mdlz_branches.copy()
    mdlz_branches = mdlz_branches.merge(
        mdlz_dc_locations[["location_name", "location_id"]],
        how="left",
        left_on="parentBranch",
        right_on="location_name",
    )
    mdlz_branches.rename(columns={"location_id": "ASSOCIATE_DC"}, inplace=True)

    unmatched = mdlz_branches["ASSOCIATE_DC"].isna().sum()
    if unmatched:
        print(f"WARNING: {unmatched} rows have no matching DC in mdlz_dc_locations\n")

    # Fetch existing locations to map name -> locationId
    print("Fetching existing locations from platform...")
    existing_locations = pc.get_locations()
    name_to_location_id = {loc["name"]: loc["id"] for loc in existing_locations}
    print(f"Found {len(name_to_location_id)} existing locations on platform")
    print(f"Found {len(mdlz_branches)} branches to update\n")

    # --- Checkpoint: resume if previous run was interrupted ---
    job_id = _make_job_id(mdlz_branches, "branch")
    cp_path = _checkpoint_path("branch", job_id)
    checkpoint = _load_checkpoint(cp_path)

    results = []
    already_processed = set()
    if checkpoint:
        results = checkpoint["results"]
        already_processed = set(checkpoint["processed_indices"])
        print(f"Resuming from checkpoint: {len(already_processed)} rows already processed")

    skipped = 0
    total = len(mdlz_branches)
    seen_names_in_file = set()

    # --- Phase 1: Validate all rows and group by (parentBranch, ASSOCIATE_DC) ---
    # Groups: {(parentBranch, ASSOCIATE_DC): [(idx, name, location_id), ...]}
    batch_groups = defaultdict(list)

    for idx, row in mdlz_branches.iterrows():
        if idx in already_processed:
            continue

        raw_name = str(row.get("name", "")).strip()

        # Skip rows with missing required fields
        if pd.isna(row.get("name")) or raw_name == "":
            skipped += 1
            print(f"  SKIP: {raw_name or 'unknown'} — missing name")
            result = {"location_name": raw_name or "unknown", "status": "skipped", "reason": "missing name"}
            results.append(result)
            already_processed.add(idx)
            if on_progress:
                on_progress(len(already_processed), total, result)
            continue
        if pd.isna(row.get("parentBranch")) or str(row["parentBranch"]).strip() == "":
            skipped += 1
            print(f"  SKIP: {raw_name} — missing parentBranch")
            result = {"location_name": raw_name, "status": "skipped", "reason": "missing parentBranch"}
            results.append(result)
            already_processed.add(idx)
            if on_progress:
                on_progress(len(already_processed), total, result)
            continue
        if pd.isna(row.get("ASSOCIATE_DC")) or str(row["ASSOCIATE_DC"]).strip() == "":
            skipped += 1
            print(f"  SKIP: {raw_name} — parentBranch not found in DC list")
            result = {"location_name": raw_name, "status": "skipped", "reason": "parentBranch not found in DC list"}
            results.append(result)
            already_processed.add(idx)
            if on_progress:
                on_progress(len(already_processed), total, result)
            continue

        name = raw_name

        # Skip duplicate names within the input file
        if name in seen_names_in_file:
            skipped += 1
            print(f"  SKIP: {name} — duplicate name in input file")
            result = {"location_name": name, "status": "skipped", "reason": "duplicate name in input file"}
            results.append(result)
            already_processed.add(idx)
            if on_progress:
                on_progress(len(already_processed), total, result)
            continue
        seen_names_in_file.add(name)

        # Look up locationId from platform
        location_id = name_to_location_id.get(name)
        if not location_id:
            skipped += 1
            print(f"  SKIP: {name} — location not found on platform")
            result = {"location_name": name, "status": "skipped", "reason": "location not found on platform"}
            results.append(result)
            already_processed.add(idx)
            if on_progress:
                on_progress(len(already_processed), total, result)
            continue

        parent_branch = str(row["parentBranch"]).strip()
        associate_dc = str(row["ASSOCIATE_DC"]).strip()

        batch_groups[(parent_branch, associate_dc)].append((idx, name, location_id))

    # --- Phase 2: Execute batched updates ---
    BATCH_SIZE = 50  # Max entity IDs per API call
    rate_limiter = AdaptiveRateLimiter()

    batch_count = len(batch_groups)
    print(f"\n{len(already_processed)} rows skipped/resumed, {sum(len(v) for v in batch_groups.values())} rows to update in {batch_count} batch groups")

    for (parent_branch, associate_dc), entries in batch_groups.items():
        # Process in sub-batches of BATCH_SIZE
        for batch_start in range(0, len(entries), BATCH_SIZE):
            batch = entries[batch_start:batch_start + BATCH_SIZE]
            entity_ids = [loc_id for _, _, loc_id in batch]
            names = [name for _, name, _ in batch]
            indices = [idx for idx, _, _ in batch]

            print(f"Batch updating {len(batch)} locations: parentBranch={parent_branch}, ASSOCIATE_DC={associate_dc}")

            try:
                _retry_api_call(lambda: pc._update_label_value_for_entities(
                    entity_type=EntityType.LOCATION,
                    entity_ids=entity_ids,
                    label_values=[
                        {"labelId": PARENT_BRANCH_LABEL_ID, "value": parent_branch},
                        {"labelId": ASSOCIATE_DC_LABEL_ID, "value": associate_dc},
                    ],
                ))

                for idx, name, _ in batch:
                    result = {
                        "location_name": name,
                        "parentBranch": parent_branch,
                        "ASSOCIATE_DC": associate_dc,
                        "status": "success",
                    }
                    results.append(result)
                    already_processed.add(idx)
                    if on_progress:
                        on_progress(len(already_processed), total, result)

                print(f"  Batch success: {len(batch)} locations updated")
                rate_limiter.record_success()

            except Exception as e:
                print(f"  Batch ERROR: {e}")
                print(f"  Falling back to individual updates for this batch...")

                # Fall back to one-by-one for this batch
                for idx, name, location_id in batch:
                    try:
                        _retry_api_call(lambda: pc._update_label_value_for_entities(
                            entity_type=EntityType.LOCATION,
                            entity_ids=[location_id],
                            label_values=[
                                {"labelId": PARENT_BRANCH_LABEL_ID, "value": parent_branch},
                                {"labelId": ASSOCIATE_DC_LABEL_ID, "value": associate_dc},
                            ],
                        ))
                        result = {
                            "location_name": name,
                            "parentBranch": parent_branch,
                            "ASSOCIATE_DC": associate_dc,
                            "status": "success",
                        }
                        rate_limiter.record_success()
                    except Exception as e2:
                        print(f"    ERROR for {name}: {e2}")
                        result = {
                            "location_name": name,
                            "parentBranch": parent_branch,
                            "ASSOCIATE_DC": associate_dc,
                            "status": f"error: {e2}",
                        }
                        rate_limiter.record_error()

                    results.append(result)
                    already_processed.add(idx)
                    if on_progress:
                        on_progress(len(already_processed), total, result)
                    rate_limiter.wait()

            # Save checkpoint after each batch
            _save_checkpoint(cp_path, already_processed, results)
            rate_limiter.wait()

    # Final cleanup
    _clear_checkpoint(cp_path)

    success = sum(1 for r in results if r["status"] == "success")
    failed = len(results) - success
    if skipped:
        print(f"\nSkipped {skipped} rows with missing required values.")
    print(f"Done. {success} succeeded, {failed} failed.")
    return pd.DataFrame(results)
