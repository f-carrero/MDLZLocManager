import os
import sys
import time
from datetime import datetime
import streamlit as st
import pandas as pd

# Add project root to path so we can import tools
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tools.mdlz_location_service import (
    load_dc_locations,
    parse_upload,
    validate_store_data,
    validate_trailer_data,
    validate_branch_data,
    REQUIRED_STORE_COLS,
    REQUIRED_TRAILER_COLS,
    REQUIRED_BRANCH_COLS,
)
from tools.mdlz_create_locations_v2 import (
    create_store_locations,
    create_trailer_locations,
    update_branch_labels,
    _checkpoint_path,
    _load_checkpoint,
    _make_job_id,
    _clear_checkpoint,
)
from tools.send_email import send_email_with_attachment

# --- Page Config ---
st.set_page_config(page_title="MDLZ Location Manager", page_icon="📍", layout="wide")


def _get_secret(key, default=""):
    """Read a config value from environment variables or Streamlit secrets."""
    val = os.environ.get(key, "")
    if val:
        return val
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return default



# --- Initialize PlatformClient ---
@st.cache_resource
def get_platform_client():
    """Initialize PlatformClient once per app session."""
    from wiliot_api.platform.platform import PlatformClient

    api_key = _get_secret("WILIOT_API_KEY")
    owner_id = _get_secret("WILIOT_OWNER_ID")

    if not api_key or not owner_id:
        st.error("WILIOT_API_KEY and WILIOT_OWNER_ID must be set in environment variables or Streamlit secrets.")
        st.stop()

    return PlatformClient(api_key=api_key, owner_id=owner_id)


@st.cache_data
def fetch_dc_locations():
    """Load DC locations from static CSV file."""
    return load_dc_locations()


# --- Session State Init ---
if "job_running" not in st.session_state:
    st.session_state.job_running = False
if "results_df" not in st.session_state:
    st.session_state.results_df = None
if "job_log" not in st.session_state:
    st.session_state.job_log = []


# --- Header ---
st.title("📍 MDLZ Location Manager")
st.caption("Upload store or trailer data to create locations, or update branch labels on the Wiliot platform.")

# --- Load DC reference data ---
pc = get_platform_client()
dc_locations_df = fetch_dc_locations()

if len(dc_locations_df) == 0:
    st.warning("No DC locations found. Check that app/data/dc_locations.csv exists and has data.")

st.markdown(f"**{len(dc_locations_df)} DCs loaded** as reference data.")

# --- Template file paths ---
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
STORES_TEMPLATE = os.path.join(DATA_DIR, "MDLZ-Stores-Creation-Inputs.xlsx")
TRAILERS_TEMPLATE = os.path.join(DATA_DIR, "MDLZ-Trailers-Creation-Inputs.xlsx")
BRANCHES_TEMPLATE = os.path.join(DATA_DIR, "MDLZ-Branch-Update-Inputs.xlsx")

# --- Tabs ---
tab_stores, tab_trailers, tab_branches = st.tabs(["Stores", "Trailers", "Branches"])


def email_results(results_df, location_type):
    """Email the results CSV after a creation job completes."""
    smtp_host = _get_secret("SMTP_HOST")
    smtp_port = _get_secret("SMTP_PORT", "587")
    smtp_username = _get_secret("SMTP_USERNAME")
    smtp_password = _get_secret("SMTP_PASSWORD")
    sender = _get_secret("SMTP_SENDER")
    recipient = _get_secret("RESULTS_RECIPIENT")

    if not all([smtp_host, smtp_username, smtp_password, sender, recipient]):
        return False, "Email not configured — missing SMTP credentials or recipient."

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    csv_data = results_df.to_csv(index=False)
    filename = f"{location_type}_creation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    success_count = (results_df["status"] == "success").sum() if "status" in results_df.columns else 0
    skipped_count = (results_df["status"] == "skipped").sum() if "status" in results_df.columns else 0
    failed_count = len(results_df) - success_count - skipped_count

    subject = f"MDLZ {location_type.title()} Creation Results — {timestamp}"
    body = (
        f"MDLZ {location_type.title()} Location Creation Results\n"
        f"Date: {timestamp}\n\n"
        f"Total processed: {len(results_df)}\n"
        f"Succeeded: {success_count}\n"
        f"Skipped: {skipped_count}\n"
        f"Failed: {failed_count}\n\n"
        f"See attached CSV for full details."
    )

    try:
        send_email_with_attachment(
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_username=smtp_username,
            smtp_password=smtp_password,
            sender=sender,
            recipient=recipient,
            subject=subject,
            body=body,
            attachment_data=csv_data,
            attachment_filename=filename,
        )
        return True, f"Results emailed to {recipient}"
    except Exception as e:
        return False, f"Failed to send email: {e}"


def _check_for_checkpoint(df, job_type):
    """Check if a resumable checkpoint exists for this data."""
    job_id = _make_job_id(df, job_type)
    cp_path = _checkpoint_path(job_type, job_id)
    checkpoint = _load_checkpoint(cp_path)
    if checkpoint:
        return cp_path, len(checkpoint["processed_indices"]), len(checkpoint["results"])
    return None, 0, 0


def _discard_checkpoint(df, job_type):
    """Remove a checkpoint so the job starts fresh."""
    job_id = _make_job_id(df, job_type)
    cp_path = _checkpoint_path(job_type, job_id)
    _clear_checkpoint(cp_path)


def run_creation_job(creation_func, data_df, dc_df, location_type):
    """Run a creation job with progress tracking and elapsed time."""
    st.session_state.job_running = True
    st.session_state.results_df = None
    st.session_state.job_log = []

    total = len(data_df)
    start_time = time.time()
    progress_bar = st.progress(0, text=f"Creating {location_type}s... 0/{total}")
    status_container = st.empty()
    stats_container = st.empty()

    def on_progress(current, total_count, result):
        elapsed = time.time() - start_time
        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        remaining = ""
        if current > 0:
            rate = elapsed / current
            eta = rate * (total_count - current)
            remaining = f" — ~{int(eta // 60)}m {int(eta % 60)}s remaining"

        progress_bar.progress(
            current / total_count,
            text=f"Processing {location_type}s... {current}/{total_count} ({elapsed_str}{remaining})",
        )
        status = result.get("status", "unknown")
        name = result.get("store_name") or result.get("location_name") or "unknown"
        if status == "success":
            status_container.success(f"Created: {name}")
        elif status == "skipped":
            status_container.warning(f"Skipped: {name}")
        else:
            status_container.error(f"Failed: {name} - {status}")
        st.session_state.job_log.append(result)

        # Show running stats
        successes = sum(1 for r in st.session_state.job_log if r.get("status") == "success")
        skips = sum(1 for r in st.session_state.job_log if r.get("status") == "skipped")
        errors = len(st.session_state.job_log) - successes - skips
        stats_container.caption(f"✓ {successes} succeeded · ⊘ {skips} skipped · ✗ {errors} failed")

    results_df = creation_func(
        pc=pc,
        **{f"mdlz_{'stores' if location_type == 'store' else 'trailers'}": data_df},
        mdlz_dc_locations=dc_df,
        on_progress=on_progress,
    )

    elapsed = time.time() - start_time
    elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
    progress_bar.progress(1.0, text=f"Done! {total} {location_type}s processed in {elapsed_str}.")
    stats_container.empty()
    st.session_state.results_df = results_df
    st.session_state.job_running = False

    # Automatically email results
    sent, msg = email_results(results_df, location_type)
    if sent:
        st.success(f"📧 {msg}")
    else:
        st.warning(f"📧 {msg}")


# --- Stores Tab ---
with tab_stores:
    st.subheader("Create Store Locations")

    st.markdown(f"**Required columns:** `{', '.join(REQUIRED_STORE_COLS)}`")

    if os.path.exists(STORES_TEMPLATE):
        with open(STORES_TEMPLATE, "rb") as f:
            st.download_button(
                "Download Stores Template (.xlsx)",
                f.read(),
                "MDLZ-Stores-Creation-Inputs.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_stores_template",
            )

    store_file = st.file_uploader(
        "Upload store data (CSV or Excel)",
        type=["csv", "xlsx", "xls"],
        key="store_upload",
    )

    if store_file:
        try:
            store_df = parse_upload(store_file, store_file.name, sheet_name="Stores")
        except Exception as e:
            st.error(f"Failed to parse file: {e}")
            store_df = None

        if store_df is not None:
            # Force name and address to uppercase (preserve NaN so validators detect blanks)
            store_df["name"] = store_df["name"].where(store_df["name"].isna(), store_df["name"].astype(str).str.strip().str.upper())
            store_df["address"] = store_df["address"].where(store_df["address"].isna(), store_df["address"].astype(str).str.strip().str.upper())
            st.markdown(f"**{len(store_df)} rows** loaded from `{store_file.name}`")

            # Data preview
            with st.expander("Data Preview", expanded=True):
                col_config = {}
                if "ECC" in store_df.columns:
                    col_config["ECC"] = st.column_config.NumberColumn("ECC", format="%d")
                preview = store_df.head(10).copy()
                preview.index = range(1, len(preview) + 1)
                preview.index.name = "No."
                st.dataframe(preview, use_container_width=True, column_config=col_config)

            # Validation
            errors = validate_store_data(store_df, dc_locations_df)
            warnings = [e for e in errors if "will be skipped" in e]
            blockers = [e for e in errors if "will be skipped" not in e]

            if blockers:
                st.error("Validation errors (must fix before creating):")
                for err in blockers:
                    st.markdown(f"- {err}")
            elif warnings:
                for warn in warnings:
                    st.warning(warn)
                st.success("Data is valid (with warnings above). Ready to create locations.")
            else:
                st.success("All validation checks passed. Ready to create locations.")

            # Create button (with checkpoint resume support)
            if not blockers:
                cp_path, cp_done, cp_results = _check_for_checkpoint(store_df, "store")
                if cp_path:
                    st.info(f"A previous run was interrupted with {cp_done}/{len(store_df)} rows processed. You can resume or start fresh.")
                    col_resume, col_discard = st.columns(2)
                    with col_resume:
                        if st.button(
                            f"Resume ({len(store_df) - cp_done} remaining)",
                            key="resume_stores",
                            disabled=st.session_state.job_running,
                            type="primary",
                        ):
                            run_creation_job(create_store_locations, store_df, dc_locations_df, "store")
                    with col_discard:
                        if st.button(
                            "Discard & Start Fresh",
                            key="discard_stores",
                            disabled=st.session_state.job_running,
                        ):
                            _discard_checkpoint(store_df, "store")
                            st.rerun()
                else:
                    if st.button(
                        f"Create {len(store_df)} Store Locations",
                        key="create_stores",
                        disabled=st.session_state.job_running,
                        type="primary",
                    ):
                        run_creation_job(create_store_locations, store_df, dc_locations_df, "store")

            # Results
            if st.session_state.results_df is not None and not st.session_state.job_running:
                results = st.session_state.results_df
                success_count = (results["status"] == "success").sum() if "status" in results.columns else 0
                fail_count = len(results) - success_count

                st.markdown("---")
                col1, col2 = st.columns(2)
                col1.metric("Succeeded", success_count)
                col2.metric("Failed", fail_count)

                if fail_count > 0:
                    st.markdown("**Failed rows:**")
                    failed = results[results["status"] != "success"]
                    st.dataframe(failed, use_container_width=True)

                csv = results.to_csv(index=False)
                st.download_button(
                    "Download Results CSV",
                    csv,
                    "store_creation_results.csv",
                    "text/csv",
                    key="download_stores",
                )

# --- Trailers Tab ---
with tab_trailers:
    st.subheader("Create Trailer Locations")

    st.markdown(f"**Required columns:** `{', '.join(REQUIRED_TRAILER_COLS)}`")

    if os.path.exists(TRAILERS_TEMPLATE):
        with open(TRAILERS_TEMPLATE, "rb") as f:
            st.download_button(
                "Download Trailers Template (.xlsx)",
                f.read(),
                "MDLZ-Trailers-Creation-Inputs.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_trailers_template",
            )

    trailer_file = st.file_uploader(
        "Upload trailer data (CSV or Excel)",
        type=["csv", "xlsx", "xls"],
        key="trailer_upload",
    )

    if trailer_file:
        try:
            trailer_df = parse_upload(trailer_file, trailer_file.name, sheet_name="Trailers")
        except Exception as e:
            st.error(f"Failed to parse file: {e}")
            trailer_df = None

        if trailer_df is not None:
            # Force trailerMake to uppercase (preserve NaN so validators detect blanks)
            trailer_df["trailerMake"] = trailer_df["trailerMake"].where(trailer_df["trailerMake"].isna(), trailer_df["trailerMake"].astype(str).str.strip().str.upper())
            st.markdown(f"**{len(trailer_df)} rows** loaded from `{trailer_file.name}`")

            # Data preview
            with st.expander("Data Preview", expanded=True):
                preview = trailer_df.head(10).copy()
                preview.index = range(1, len(preview) + 1)
                preview.index.name = "No."
                st.dataframe(preview, use_container_width=True)

            # Validation
            errors = validate_trailer_data(trailer_df, dc_locations_df)
            warnings = [e for e in errors if "will be skipped" in e]
            blockers = [e for e in errors if "will be skipped" not in e]

            if blockers:
                st.error("Validation errors (must fix before creating):")
                for err in blockers:
                    st.markdown(f"- {err}")
            elif warnings:
                for warn in warnings:
                    st.warning(warn)
                st.success("Data is valid (with warnings above). Ready to create locations.")
            else:
                st.success("All validation checks passed. Ready to create locations.")

            # Create button (with checkpoint resume support)
            if not blockers:
                cp_path, cp_done, cp_results = _check_for_checkpoint(trailer_df, "trailer")
                if cp_path:
                    st.info(f"A previous run was interrupted with {cp_done}/{len(trailer_df)} rows processed. You can resume or start fresh.")
                    col_resume, col_discard = st.columns(2)
                    with col_resume:
                        if st.button(
                            f"Resume ({len(trailer_df) - cp_done} remaining)",
                            key="resume_trailers",
                            disabled=st.session_state.job_running,
                            type="primary",
                        ):
                            run_creation_job(create_trailer_locations, trailer_df, dc_locations_df, "trailer")
                    with col_discard:
                        if st.button(
                            "Discard & Start Fresh",
                            key="discard_trailers",
                            disabled=st.session_state.job_running,
                        ):
                            _discard_checkpoint(trailer_df, "trailer")
                            st.rerun()
                else:
                    if st.button(
                        f"Create {len(trailer_df)} Trailer Locations",
                        key="create_trailers",
                        disabled=st.session_state.job_running,
                        type="primary",
                    ):
                        run_creation_job(create_trailer_locations, trailer_df, dc_locations_df, "trailer")

            # Results
            if st.session_state.results_df is not None and not st.session_state.job_running:
                results = st.session_state.results_df
                success_count = (results["status"] == "success").sum() if "status" in results.columns else 0
                fail_count = len(results) - success_count

                st.markdown("---")
                col1, col2 = st.columns(2)
                col1.metric("Succeeded", success_count)
                col2.metric("Failed", fail_count)

                if fail_count > 0:
                    st.markdown("**Failed rows:**")
                    failed = results[results["status"] != "success"]
                    st.dataframe(failed, use_container_width=True)

                csv = results.to_csv(index=False)
                st.download_button(
                    "Download Results CSV",
                    csv,
                    "trailer_creation_results.csv",
                    "text/csv",
                    key="download_trailers",
                )

# --- Branches Tab ---
with tab_branches:
    st.subheader("Update Branch Labels")

    st.markdown(f"**Required columns:** `{', '.join(REQUIRED_BRANCH_COLS)}`")

    if os.path.exists(BRANCHES_TEMPLATE):
        with open(BRANCHES_TEMPLATE, "rb") as f:
            st.download_button(
                "Download Branches Template (.xlsx)",
                f.read(),
                "MDLZ-Branch-Update-Inputs.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_branches_template",
            )

    branch_file = st.file_uploader(
        "Upload branch data (CSV or Excel)",
        type=["csv", "xlsx", "xls"],
        key="branch_upload",
    )

    if branch_file:
        try:
            branch_df = parse_upload(branch_file, branch_file.name, sheet_name="Branches")
        except Exception as e:
            st.error(f"Failed to parse file: {e}")
            branch_df = None

        if branch_df is not None:
            st.markdown(f"**{len(branch_df)} rows** loaded from `{branch_file.name}`")

            # Data preview
            with st.expander("Data Preview", expanded=True):
                preview = branch_df.head(10).copy()
                preview.index = range(1, len(preview) + 1)
                preview.index.name = "No."
                st.dataframe(preview, use_container_width=True)

            # Validation
            errors = validate_branch_data(branch_df, dc_locations_df)
            warnings = [e for e in errors if "will be skipped" in e]
            blockers = [e for e in errors if "will be skipped" not in e]

            if blockers:
                st.error("Validation errors (must fix before updating):")
                for err in blockers:
                    st.markdown(f"- {err}")
            elif warnings:
                for warn in warnings:
                    st.warning(warn)
                st.success("Data is valid (with warnings above). Ready to update labels.")
            else:
                st.success("All validation checks passed. Ready to update labels.")

            # Update button (with checkpoint resume support)
            if not blockers:
                cp_path, cp_done, cp_results = _check_for_checkpoint(branch_df, "branch")

                def _run_branch_job():
                    st.session_state.job_running = True
                    st.session_state.results_df = None
                    st.session_state.job_log = []

                    total = len(branch_df)
                    start_time = time.time()
                    progress_bar = st.progress(0, text=f"Updating branches... 0/{total}")
                    status_container = st.empty()
                    stats_container = st.empty()

                    def on_branch_progress(current, total_count, result):
                        elapsed = time.time() - start_time
                        elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                        remaining = ""
                        if current > 0:
                            rate = elapsed / current
                            eta = rate * (total_count - current)
                            remaining = f" — ~{int(eta // 60)}m {int(eta % 60)}s remaining"

                        progress_bar.progress(
                            current / total_count,
                            text=f"Updating branches... {current}/{total_count} ({elapsed_str}{remaining})",
                        )
                        status = result.get("status", "unknown")
                        name = result.get("location_name", "unknown")
                        if status == "success":
                            status_container.success(f"Updated: {name}")
                        elif status == "skipped":
                            status_container.warning(f"Skipped: {name}")
                        else:
                            status_container.error(f"Failed: {name} - {status}")
                        st.session_state.job_log.append(result)

                        successes = sum(1 for r in st.session_state.job_log if r.get("status") == "success")
                        skips = sum(1 for r in st.session_state.job_log if r.get("status") == "skipped")
                        errors = len(st.session_state.job_log) - successes - skips
                        stats_container.caption(f"✓ {successes} succeeded · ⊘ {skips} skipped · ✗ {errors} failed")

                    results_df = update_branch_labels(
                        pc=pc,
                        mdlz_branches=branch_df,
                        mdlz_dc_locations=dc_locations_df,
                        on_progress=on_branch_progress,
                    )

                    elapsed = time.time() - start_time
                    elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
                    progress_bar.progress(1.0, text=f"Done! {total} branches processed in {elapsed_str}.")
                    stats_container.empty()
                    st.session_state.results_df = results_df
                    st.session_state.job_running = False

                    # Automatically email results
                    sent, msg = email_results(results_df, "branch")
                    if sent:
                        st.success(f"📧 {msg}")
                    else:
                        st.warning(f"📧 {msg}")

                if cp_path:
                    st.info(f"A previous run was interrupted with {cp_done}/{len(branch_df)} rows processed. You can resume or start fresh.")
                    col_resume, col_discard = st.columns(2)
                    with col_resume:
                        if st.button(
                            f"Resume ({len(branch_df) - cp_done} remaining)",
                            key="resume_branches",
                            disabled=st.session_state.job_running,
                            type="primary",
                        ):
                            _run_branch_job()
                    with col_discard:
                        if st.button(
                            "Discard & Start Fresh",
                            key="discard_branches",
                            disabled=st.session_state.job_running,
                        ):
                            _discard_checkpoint(branch_df, "branch")
                            st.rerun()
                else:
                    if st.button(
                        f"Update {len(branch_df)} Branch Labels",
                        key="update_branches",
                        disabled=st.session_state.job_running,
                        type="primary",
                    ):
                        _run_branch_job()

            # Results
            if st.session_state.results_df is not None and not st.session_state.job_running:
                results = st.session_state.results_df
                success_count = (results["status"] == "success").sum() if "status" in results.columns else 0
                fail_count = len(results) - success_count

                st.markdown("---")
                col1, col2 = st.columns(2)
                col1.metric("Succeeded", success_count)
                col2.metric("Failed", fail_count)

                if fail_count > 0:
                    st.markdown("**Failed rows:**")
                    failed = results[results["status"] != "success"]
                    st.dataframe(failed, use_container_width=True)

                csv = results.to_csv(index=False)
                st.download_button(
                    "Download Results CSV",
                    csv,
                    "branch_update_results.csv",
                    "text/csv",
                    key="download_branches",
                )
