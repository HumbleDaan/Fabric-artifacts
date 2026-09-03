# MARKDOWN ********************
# # Workspace Access Campaign (Admin Utility)
#
# Grants, verifies, and then **revokes** the workspace access that
# [`custom_visuals_audit`](custom_visuals_audit.ipynb) needs in order to read report
# definitions tenant-wide.
#
# > **This notebook performs writes against every workspace in scope.** It is deliberately
# > separate from the audit notebook so that the thing which *reads* cannot escalate its own
# > access, and so the permission change can be reviewed and approved on its own merits.
#
# ## Why this is unavoidable
#
# There is no admin-scope path to report content. Verified against the API reference:
#
# | Surface | Returns report definition/content? |
# |---|---|
# | `GET /v1/admin/items` | No - metadata only |
# | `GET /v1/admin/items/{id}/users` | No - access list only |
# | Scanner API (`admin/workspaces/getInfo`) | No - the report object has **no visual fields at all** |
# | `ExportReportAsAdmin` | **Does not exist** (dataflows have an admin export; reports do not) |
#
# `getDefinition` states verbatim: *"The caller must have read and write permissions for the
# report"*, scope `Report.ReadWrite.All`. **Viewer is not sufficient** - do not spend a cycle
# testing it. Contributor is the floor.
#
# ## Run this as a Fabric Administrator, not as the audit service principal
#
# `AddUserAsAdmin` requires `Tenant.ReadWrite.All` and states *"The user must be a Fabric
# administrator."* It appears on **neither** service-principal enablement list - not the
# read-only Power BI admin list, and not the Fabric update list. Assume the audit SP
# **cannot** grant its own access.
#
# Sign in interactively as a Fabric Administrator and run this notebook under that identity.
#
# ## Grant, scan, revoke
#
# Run this notebook -> run the audit -> return here and revoke. Treating access as a
# **time-boxed campaign** rather than a standing grant turns the ask from *"permanent write
# access to the entire estate"* into a bounded maintenance window, and stops the audit
# tooling becoming its own long-lived risk.
#
# ## Throttle
#
# `AddUserAsAdmin` is capped at **200 requests/hour tenant-wide**. The loop paces itself;
# estimate the campaign duration from the selected workspace count. Do not raise
# `GRANT_CALLS_PER_HOUR` above 200.

# CELL ********************

import datetime as dt
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import sempy_labs as labs
from sempy_labs import admin
from sempy_labs._helper_functions import _base_api

# ---------------------------------------------------------------------------
# CONFIGURATION - review every value before running
# ---------------------------------------------------------------------------

# Microsoft's own documentation contradicts itself on what `identifier` means for a
# service principal. The Add-PowerBIWorkspaceUser PowerShell doc and the Fabric core
# roleAssignments API both say OBJECT ID; community reports say the APPLICATION
# (CLIENT) ID works. The probe below settles it empirically - fill in both.
SERVICE_PRINCIPAL_OBJECT_ID = "REPLACE-ME"   # Enterprise Application object ID
SERVICE_PRINCIPAL_CLIENT_ID = "REPLACE-ME"   # Application (client) ID - fallback

# Contributor is the minimum that satisfies getDefinition's read+write requirement.
GRANT_ROLE = "Contributor"

# Tenant-wide cap on AddUserAsAdmin. Do not raise this.
GRANT_CALLS_PER_HOUR = 200

# Ledger of what THIS tool granted, so revoke touches nothing it did not create.
LEDGER_TABLE = "cv_grant_ledger"

# Restrict the campaign to specific workspace IDs. None = every active,
# non-personal workspace. Start with a short list.
WORKSPACE_ALLOWLIST: Optional[List[str]] = None

# Safety interlocks. Each stage requires an explicit opt-in.
DRY_RUN = True       # True = show what would change, touch nothing
PROBE_ONLY = True    # True = resolve the identifier on ONE workspace, then stop

RUN_ID = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
print(f"Run ID: {RUN_ID}")

# CELL ********************

def log(msg: str) -> None:
    print(f"[{dt.datetime.now(dt.timezone.utc):%H:%M:%S}] {msg}", flush=True)


def call_with_retry(fn, *args, max_retries: int = 5, **kwargs):
    """Retry on 429/5xx, honouring Retry-After where present."""
    last: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last = exc
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", None)
            if status is not None and status not in {429, 500, 502, 503, 504}:
                raise
            retry_after = None
            if response is not None:
                try:
                    retry_after = float(response.headers.get("Retry-After", ""))
                except (TypeError, ValueError):
                    retry_after = None
            sleep_for = min(retry_after or 5 * (2 ** attempt), 300)
            log(f"  retryable error (status={status}); sleeping {sleep_for:.0f}s")
            time.sleep(sleep_for)
    raise last  # type: ignore[misc]


def grant(identifier: str, workspace_id: str) -> Tuple[bool, str]:
    try:
        call_with_retry(
            admin.add_user_to_workspace,
            user=identifier,
            role=GRANT_ROLE,
            principal_type="App",  # case-sensitive - must be exactly "App"
            workspace=workspace_id,
        )
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)[:400]


def revoke(identifier: str, workspace_id: str) -> Tuple[bool, str]:
    """Admin - Groups DeleteUserAsAdmin."""
    try:
        call_with_retry(
            _base_api,
            request=f"/v1.0/myorg/admin/groups/{workspace_id}/users/{identifier}",
            method="delete",
            client="fabric",
        )
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)[:400]


def paced(index: int, total: int, started: float) -> None:
    """Sleep so the loop never exceeds GRANT_CALLS_PER_HOUR."""
    if index + 1 >= total:
        return
    min_interval = 3600.0 / GRANT_CALLS_PER_HOUR
    elapsed = time.time() - started
    target = (index + 1) * min_interval
    if target > elapsed:
        time.sleep(target - elapsed)

# CELL ********************

# --- Scope ---------------------------------------------------------------

workspaces = call_with_retry(admin.list_workspaces)
workspaces = workspaces[workspaces["State"].str.lower() == "active"]
workspaces = workspaces[
    ~workspaces["Type"].str.lower().isin({"personalgroup", "personal"})
]
workspaces = workspaces.rename(columns={"Id": "workspace_id", "Name": "workspace_name"})
workspaces = workspaces[["workspace_id", "workspace_name"]].copy()

if WORKSPACE_ALLOWLIST is not None:
    workspaces = workspaces[workspaces["workspace_id"].isin(WORKSPACE_ALLOWLIST)].copy()

hours = len(workspaces) / GRANT_CALLS_PER_HOUR
log(f"Workspaces in scope: {len(workspaces):,} (~{hours:.1f}h at the 200/hr cap)")
display(workspaces.head(20))

# CELL ********************

# --- Probe: resolve the identifier ambiguity on ONE workspace -------------

GRANT_IDENTIFIER: Optional[str] = None

if DRY_RUN:
    log("DRY RUN - probe skipped. Set DRY_RUN = False to resolve the identifier.")
else:
    probe_workspace = workspaces.iloc[0]
    log(f"Probing on '{probe_workspace['workspace_name']}' only.")
    for label, identifier in (
        ("object ID", SERVICE_PRINCIPAL_OBJECT_ID),
        ("client ID", SERVICE_PRINCIPAL_CLIENT_ID),
    ):
        ok, err = grant(identifier, probe_workspace["workspace_id"])
        log(f"  {label} ({identifier}): {'SUCCESS' if ok else 'failed - ' + err}")
        if ok:
            GRANT_IDENTIFIER = identifier
            log(f"RESOLVED: use the {label}. Set PROBE_ONLY = False to run the campaign.")
            break
    if GRANT_IDENTIFIER is None:
        log(
            "Both identifiers failed. Check that you are signed in as a Fabric "
            "Administrator and NOT running under the audit service principal."
        )

# CELL ********************

# --- Grant campaign -------------------------------------------------------

if DRY_RUN or PROBE_ONLY:
    log(
        f"NO CHANGES MADE. Would grant '{GRANT_ROLE}' on {len(workspaces):,} workspaces. "
        "Set DRY_RUN = False and PROBE_ONLY = False to execute."
    )
else:
    assert GRANT_IDENTIFIER, "Run the probe cell first to resolve the identifier."
    granted: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    started = time.time()

    for i, (_, row) in enumerate(workspaces.reset_index(drop=True).iterrows()):
        ok, err = grant(GRANT_IDENTIFIER, row["workspace_id"])
        record = {
            "run_id": RUN_ID,
            "workspace_id": row["workspace_id"],
            "workspace_name": row["workspace_name"],
            "identifier": GRANT_IDENTIFIER,
            "role": GRANT_ROLE,
            "granted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        if ok:
            granted.append(record)
        else:
            failed.append({**record, "error": err})
        if i % 25 == 0:
            log(f"  {i + 1}/{len(workspaces)} - {len(granted)} granted, {len(failed)} failed")
        paced(i, len(workspaces), started)

    log(f"Granted on {len(granted):,} workspaces; {len(failed):,} failures.")

    if granted:
        # Append-only ledger - revoke reads this so it can never touch pre-existing access.
        (
            spark.createDataFrame(pd.DataFrame(granted))
            .write.mode("append")
            .format("delta")
            .saveAsTable(LEDGER_TABLE)
        )
        log(f"Ledger written: {LEDGER_TABLE} (run_id = {RUN_ID})")
    if failed:
        log("These workspaces were NOT granted - the audit will under-cover them:")
        display(pd.DataFrame(failed)[["workspace_name", "error"]])

# MARKDOWN ********************
# ## Revoke
#
# Run this **after** the audit notebook has completed and its output has been saved.
#
# Revocation reads the ledger written above, so it removes only access that this tool
# created. Pre-existing permissions are never touched.
#
# Set `REVOKE_RUN_ID` to the campaign you want to unwind.

# CELL ********************

REVOKE = False
REVOKE_RUN_ID: Optional[str] = None  # None = the most recent campaign

if not REVOKE:
    log("Revoke skipped (REVOKE = False).")
else:
    ledger = spark.read.table(LEDGER_TABLE).toPandas()
    target_run = REVOKE_RUN_ID or ledger["run_id"].max()
    ledger = ledger[ledger["run_id"] == target_run].copy()
    log(f"Revoking {len(ledger):,} grants from campaign {target_run}.")

    revoked, revoke_failed = 0, []
    started = time.time()
    for i, (_, row) in enumerate(ledger.reset_index(drop=True).iterrows()):
        ok, err = revoke(row["identifier"], row["workspace_id"])
        if ok:
            revoked += 1
        else:
            revoke_failed.append(
                {"workspace_name": row["workspace_name"],
                 "workspace_id": row["workspace_id"], "error": err}
            )
        paced(i, len(ledger), started)

    log(f"Revoked {revoked:,}; {len(revoke_failed):,} failures.")
    if revoke_failed:
        log("MANUAL FOLLOW-UP REQUIRED - these workspaces still carry the grant:")
        display(pd.DataFrame(revoke_failed))
    else:
        log("All grants from this campaign have been removed.")
