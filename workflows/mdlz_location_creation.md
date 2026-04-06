# MDLZ Location Creation

## Objective
Create Mondelez (MDLZ) customer locations on the Wiliot platform in the correct hierarchy: Parent Branches (DCs) → Crossdocks (XDs) → Stores → Trailers. Also supports adding ECC labels to existing stores.

## Two Modes of Operation

### 1. Self-Service Web App (Stores & Trailers)

External MDLZ contacts and internal Wiliot team members can create store and trailer locations through a Streamlit web interface — no developer involvement needed.

**App location:** `MDLZ/app/streamlit_app.py`

**How it works:**
1. User logs in with a shared password
2. Selects "Stores" or "Trailers" tab
3. Uploads a CSV or Excel file with the required columns
4. App validates the data (column checks, type checks, DC matching)
5. User clicks "Create Locations" and watches the progress bar
6. Downloads results CSV when done

**Running locally:**
```bash
cd MDLZ/app
pip install -r requirements.txt
streamlit run streamlit_app.py
```

**Environment variables / Streamlit secrets required:**
- `WILIOT_API_KEY` — Wiliot platform API key
- `WILIOT_OWNER_ID` — Wiliot account owner ID
- `APP_PASSWORD` — Password for the login gate

**Deployment:** Streamlit Community Cloud (free). Push to GitHub, connect repo, set secrets in dashboard.

**Tools used by the app:**
- `MDLZ/tools/mdlz_create_locations_v2.py` — Store and trailer creation with progress callbacks and retry logic
- `MDLZ/tools/mdlz_location_service.py` — Validation, DC fetching, file parsing

**Store upload columns:** `name, address, lat, lng, ECC, parentBranch, PAR_LEVEL_Roll_Cart`

**Trailer upload columns:** `name, parentBranch, trailerLength, trailerMake`

DC reference data (`ASSOCIATE_DC`) is resolved automatically from the platform — customers don't need to provide it.

---

### 2. Manual Script (All Location Types)

For DCs, crossdocks, ECC labels, or complex batch operations, use the original script directly.

**Tool:** `MDLZ/tools/mdlz_create_locations.py`

## Prerequisites
- Python packages: `wiliot-api`, `pandas`, `openpyxl`, `streamlit` (for web app)
- Wiliot API key and owner ID (stored in root `.env` or as environment variables)
- Excel data files placed in `MDLZ/.tmp/` (for manual script mode)

## Data Files (Manual Script Mode)

Place Excel files in `MDLZ/.tmp/`. Templates are in `MDLZ/.tmp/templates/`.

### branch_crossdock_match_results.xlsx
Used by: `create_parent_branch_locations()`, `create_crossdock_locations()`

| Column | Description |
|---|---|
| `Depot_#` | Depot number identifier |
| `Depot_Name` | Name of the depot |
| `Street` | Street address (can be empty) |
| `City` | City name |
| `State` | State abbreviation |
| `Zip` | Zip code (numeric) |
| `Latitude` | Decimal latitude |
| `Longitude` | Decimal longitude |
| `Type` | `PARENT BRANCH` or `CROSSDOCK` |
| `Match_Type` | `no_match` = needs creation |
| `Parent_Branch` | Name of parent branch (for crossdocks) |
| `Platform_ID` | Existing platform ID if already matched |
| `Parent_Branch_Platform_ID` | Platform ID of parent DC (required for crossdocks) |

### store_match_results.xlsx
Used by: `create_store_locations()`, `add_ecc_labels()`

Sheet: **"Store Match Results"** (column order matters — script uses 1-based indices)

| Col # | Column | Description |
|---|---|---|
| 1 | `ECC` | Store ECC identifier (numeric) |
| 2 | `Store_Name` | Store name |
| 3 | `Store_#` | Store number |
| 4 | `Street` | Street address |
| 5 | `City` | City |
| 6 | `State` | State abbreviation |
| 7 | `ZIP` | Zip code |
| 8 | `Branch` | Branch code |
| 9 | `Branch_Name` | Branch name (used as filter) |
| 10 | `Store_Lat` | Decimal latitude |
| 11 | `Store_Lng` | Decimal longitude |
| 12 | `Match_Type` | `no_match` = create, `name+coords` = already matched |
| 13 | `Platform_ID` | Filled after creation |
| 14 | `Platform_Name` | Filled after creation |
| 15 | `Platform_Lat` | Filled after creation |
| 16 | `Platform_Lng` | Filled after creation |
| 17 | `Distance_KM` | Filled after creation |
| 18 | `Platform_Loc_Type` | Filled after creation |

Optional sheet: **"No ECC Analysis"** — columns: `Platform_ID`, `Platform_Name`, `Match_Status`, `Matched_ECC`

### Trailer Excel files (any name)
Used by: `create_trailer_locations()`

Sheet: **"LIST"** (configurable). Column A = `Unit #`, data starts row 2.

## Execution Order

**Must be run in this sequence** (each step depends on IDs from the previous):

1. **Parent Branches first** — creates DCs, returns location IDs
2. **Crossdocks second** — requires `Parent_Branch_Platform_ID` from step 1
3. **Stores third** — requires `associate_dc_id` (DC platform ID) and `branch_name`
4. **Trailers** — requires `dc_location_id` (DC platform ID)
5. **ECC Labels** — can be run independently on already-matched stores

## Usage

```python
from wiliot_api.platform.platform import PlatformClient
from MDLZ.tools.mdlz_create_locations import *

pc = PlatformClient(api_key="<key>", owner_id="<owner>")

# Step 1: Parent Branches
create_parent_branch_locations(pc)

# Step 2: Crossdocks
create_crossdock_locations(pc)

# Step 3: Stores (per branch)
create_store_locations(pc, branch_name="PORTLAND OR DU", associate_dc_id="<id>")

# Step 4: Trailers
create_trailer_locations(pc, excel_path="trailers.xlsx", dc_location_id="<id>", template_name="Standard_truck")

# Step 5: ECC Labels
add_ecc_labels(pc)
```

All functions accept `limit=N` for testing or resuming.

## Output
JSON results saved to `MDLZ/.tmp/results/` with success/error status per row.

## Edge Cases & Lessons Learned
- Store creation updates Excel in-place after each row — safe to stop and resume
- 0.3–0.5s sleep between API calls for rate limiting
- `create_store_locations` also renames matched stores that have ECC incorrectly appended to their name
- Crossdocks require a valid `Parent_Branch_Platform_ID` — rows without one are skipped
