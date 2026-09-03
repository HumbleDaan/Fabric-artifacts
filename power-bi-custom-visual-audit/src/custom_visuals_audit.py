# MARKDOWN ********************
# # Tenant-Wide Power BI Custom Visual Audit
#
# Builds an evidence-based inventory of custom visual registration and placement across accessible Power BI workspaces. It enriches that inventory with AppSource certification metadata, relevant tenant settings, and recent custom-visual authentication activity.
#
# **Scope:** active, non-personal workspaces visible to the audit identity and Power BI reports whose public definitions can be read. Both `PBIR` and `PBIR-Legacy` definitions are supported.
#
# ## What this notebook produces
#
# | Delta table | Grain | Purpose |
# |---|---|---|
# | `cv_audit_workspaces` | run x workspace | Workspace inventory |
# | `cv_audit_reports` | run x report | Report inventory |
# | `cv_audit_catalog` | run x visual GUID | AppSource catalogue snapshot and certification flag |
# | `cv_audit_report_visuals` | run x report x visual | Registered and actively placed custom visuals |
# | `cv_audit_placements` | run x report x page x visual instance | Placement and page link for remediation |
# | `cv_audit_scan_status` | run x report | Reconciled scan outcome, including zero-visual reports |
# | `cv_audit_scan_errors` | run x resource | Workspace inventory and report-definition failures |
# | `cv_audit_tenant_settings` | run x setting | Visual-related tenant settings |
# | `cv_audit_activity_events` | run x activity event | Relevant token and organizational-gallery events |
# | `cv_audit_activity_errors` | run x UTC date | Activity-log extraction failures |
# | `cv_audit_findings` | run x report x visual | Joined governance register |
#
# ## Interpretation boundaries
#
# - This notebook identifies **configuration, capability, and exposure signals**. It does not prove data exfiltration.
# - A token event shows that a custom visual requested and received a Microsoft Entra token. It does not show where the token or report data was subsequently sent.
# - An embedded visual package does not establish provenance by itself. It can originate from a file, an organizational process, or another approved distribution path.
# - Reports blocked by permissions, encrypted sensitivity labels, malformed definitions, or API failures remain explicit coverage gaps.
#
# ## Design decisions
#
# 1. **Definitions are read without conversion.** PBIR-Legacy `report.json` and PBIR `definition/` parts are parsed directly. The notebook does not rewrite reports.
# 2. **Storage format comes from definition parts.** `report.json` identifies PBIR-Legacy; `definition/report.json` identifies PBIR.
# 3. **Every run is an isolated snapshot.** All persisted rows carry `run_id`, and downstream analysis reads only the current run.
# 4. **The AppSource catalogue is enrichment, not an authority for provenance.** Its endpoint is undocumented and may change; catalogue failure stops classification rather than silently reusing uncertain metadata.

# MARKDOWN ********************
# ## Prerequisites and data handling
#
# ### Runtime
#
# Use a **Microsoft Fabric Runtime** PySpark notebook with an attached Lakehouse. Create a Fabric Custom Environment and pin versions that have passed your acceptance test. This notebook intentionally does not install or upgrade packages at runtime.
#
# Required packages:
#
# - `semantic-link >= 0.12.0`
# - A tested, pinned release of `semantic-link-labs`
# - Runtime-provided `pandas`, `requests`, PySpark, and Delta Lake
#
# ### Audit identity
#
# 1. Register a dedicated Microsoft Entra application and store its tenant ID, client ID, and client secret in Azure Key Vault.
# 2. Put the service principal in a security group allowed by the Fabric tenant settings for read-only admin APIs and Fabric APIs.
# 3. Do not assign admin-consent-required Power BI application permissions to the app used for read-only admin APIs.
# 4. Grant the service principal only the workspace access approved for this audit. `getDefinition` requires read and write permission on each report, which normally means Contributor or a stronger workspace role.
#
# This notebook does **not** grant workspace permissions. Permission changes belong in a separately reviewed administrative process.
#
# ### Expected coverage gaps
#
# - The Fabric `getDefinition` API blocks reports with encrypted sensitivity labels.
# - Workspaces or reports outside the audit identity's access are not scanned.
# - AppSource certification describes the reviewed package version; it is not a guarantee about publisher operations or future versions.
# - The report definition does not reliably identify whether an embedded visual came from a local file or an organizational store.
#
# ### Output sensitivity
#
# The output can contain workspace names, report names, report URLs, user identifiers from activity events, publisher information, and declared WebAccess endpoints. Store the Delta tables in a restricted Lakehouse, apply an appropriate retention policy, and do not publish generated output with this notebook.

# CELL ********************

import base64
import concurrent.futures as cf
import contextvars
import datetime as dt
import importlib.metadata
import json
import random
import re
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
from delta.tables import DeltaTable
from pyspark.sql import functions as F

import sempy_labs as labs
from sempy_labs import admin
from sempy_labs._helper_functions import _base_api

# ---------------------------------------------------------------------------
# CONFIGURATION - review every value before running
# ---------------------------------------------------------------------------

# sempy_labs' service_principal_authentication takes Key Vault secret names,
# not the secret values themselves.
USE_SERVICE_PRINCIPAL = True
KEY_VAULT_URI = "https://REPLACE-ME.vault.azure.net/"
KV_SECRET_TENANT_ID = "fabric-audit-tenant-id"
KV_SECRET_CLIENT_ID = "fabric-audit-client-id"
KV_SECRET_CLIENT_SECRET = "fabric-audit-client-secret"

TABLE_PREFIX = "cv_audit"
MAX_WORKERS = 4
MAX_RETRIES = 4
RETRY_BASE_SECONDS = 5
MIN_CATALOG_ROWS = 500

# Start with a deterministic pilot. Set to None after acceptance testing.
WORKSPACE_LIMIT: Optional[int] = 5

# Power BI activity data is available for at most 28 days.
ACTIVITY_LOOKBACK_DAYS = 28

# Visuals shipped by Microsoft that can appear in custom-visual registrations.
BUILTIN_VISUAL_ALLOWLIST = {
    "esriVisual",
    "FlowVisual_C29F1DCC_81F5_4973_94AD_0517D44CC06A",
}

UTC = dt.timezone.utc
RUN_ID = dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def validate_configuration() -> None:
    if not 1 <= MAX_WORKERS <= 16:
        raise ValueError("MAX_WORKERS must be between 1 and 16.")
    if not 1 <= ACTIVITY_LOOKBACK_DAYS <= 28:
        raise ValueError("ACTIVITY_LOOKBACK_DAYS must be between 1 and 28.")
    if MIN_CATALOG_ROWS < 1:
        raise ValueError("MIN_CATALOG_ROWS must be positive.")
    if USE_SERVICE_PRINCIPAL and "REPLACE-ME" in KEY_VAULT_URI:
        raise ValueError("Set KEY_VAULT_URI before running with service-principal authentication.")
    if not spark.catalog.currentDatabase():
        raise RuntimeError("Attach a default Lakehouse before running the audit.")


validate_configuration()
print(f"Run ID: {RUN_ID}")
print(f"semantic-link-labs: {importlib.metadata.version('semantic-link-labs')}")

# CELL ********************

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_print_lock = threading.Lock()
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}


def log(message: str) -> None:
    with _print_lock:
        timestamp = dt.datetime.now(UTC).strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}", flush=True)


def _status_of(exc: Exception) -> Optional[int]:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status is not None:
        return int(status)
    status = getattr(exc, "status_code", None)
    if status is not None:
        return int(status)
    match = re.search(r"\b([45]\d{2})\b", str(exc))
    return int(match.group(1)) if match else None


def _retry_after_of(exc: Exception) -> Optional[float]:
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", None) or {}
    value = headers.get("Retry-After") or headers.get("retry-after")
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def call_with_retry(function, *args, max_retries: int = MAX_RETRIES, **kwargs):
    """Retry documented transient HTTP failures with bounded jittered backoff."""
    last_error: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            return function(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - status/type is inspected below
            last_error = exc
            status = _status_of(exc)
            retryable = status in RETRYABLE_STATUSES or isinstance(exc, requests.RequestException)
            if not retryable or attempt == max_retries - 1:
                raise
            base_delay = _retry_after_of(exc) or RETRY_BASE_SECONDS * (2 ** attempt)
            delay = min(base_delay + random.uniform(0, 1), 300)
            log(
                f"Retryable error (status={status}); sleeping {delay:.1f}s "
                f"[attempt {attempt + 1}/{max_retries}]"
            )
            time.sleep(delay)
    raise RuntimeError("Retry loop exited unexpectedly") from last_error


def submit_with_context(pool: cf.ThreadPoolExecutor, function, *args):
    """Submit work with a copy of the caller's context variables, including auth."""
    context = contextvars.copy_context()
    return pool.submit(context.run, function, *args)


def _prepare_for_spark(dataframe: pd.DataFrame) -> Tuple[pd.DataFrame, Any]:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    output = dataframe.copy()
    output["run_id"] = RUN_ID
    fields = []

    for column in output.columns:
        series = output[column]
        if pd.api.types.is_bool_dtype(series):
            spark_type = BooleanType()
        elif pd.api.types.is_integer_dtype(series):
            spark_type = LongType()
        elif pd.api.types.is_float_dtype(series):
            spark_type = DoubleType()
        else:
            spark_type = StringType()
            output[column] = series.map(
                lambda value: None
                if value is None
                or (not isinstance(value, (list, dict)) and pd.isna(value))
                else json.dumps(value, sort_keys=True)
                if isinstance(value, (list, dict))
                else str(value)
            )
        fields.append(StructField(column, spark_type, True))

    return output, StructType(fields)


def write_run_table(dataframe: pd.DataFrame, name: str) -> None:
    """Replace this run's rows while preserving snapshots from other run IDs."""
    full_name = f"{TABLE_PREFIX}_{name}"

    if spark.catalog.tableExists(full_name):
        DeltaTable.forName(spark, full_name).delete(F.col("run_id") == RUN_ID)

    if dataframe is None or dataframe.empty:
        log(f"Wrote 0 rows for run {RUN_ID} -> {full_name}")
        return

    output, schema = _prepare_for_spark(dataframe)
    (
        spark.createDataFrame(output, schema=schema)
        .write.mode("append")
        .option("mergeSchema", "true")
        .format("delta")
        .saveAsTable(full_name)
    )
    log(f"Wrote {len(output):,} rows for run {RUN_ID} -> {full_name}")


def read_run_table(name: str) -> Optional[pd.DataFrame]:
    full_name = f"{TABLE_PREFIX}_{name}"
    if not spark.catalog.tableExists(full_name):
        return None
    return (
        spark.read.table(full_name)
        .where(F.col("run_id") == RUN_ID)
        .drop("run_id")
        .toPandas()
    )


def as_bool(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})

# MARKDOWN ********************
# ## Phase 0 - Authenticate and validate access
#
# All `semantic-link-labs` calls run inside one service-principal authentication context. When work is submitted to a thread pool, the notebook explicitly copies that context into each worker.
#
# The first check confirms read-only admin API access. Phase 2 then tests workspace-level report access, and Phase 5 records every report-definition authorization failure. The notebook never changes tenant or workspace permissions.

# CELL ********************

from contextlib import nullcontext


def auth_context():
    if not USE_SERVICE_PRINCIPAL:
        log("Running under the notebook owner's identity (pilot mode).")
        return nullcontext()
    log("Running under service principal via Key Vault.")
    return labs.service_principal_authentication(
        key_vault_uri=KEY_VAULT_URI,
        key_vault_tenant_id=KV_SECRET_TENANT_ID,
        key_vault_client_id=KV_SECRET_CLIENT_ID,
        key_vault_client_secret=KV_SECRET_CLIENT_SECRET,
    )


# Smoke test - confirms the identity can reach the admin surface.
with auth_context():
    _probe = call_with_retry(admin.list_workspaces)
    log(f"Auth OK. Admin scope sees {len(_probe):,} workspaces.")

# MARKDOWN ********************
# ## Phase 1 - Workspace inventory
#
# `admin.list_workspaces()` uses `GET /v1/admin/workspaces`, which is service-principal
# supported and paginated. We keep only active, non-personal workspaces.

# CELL ********************

with auth_context():
    workspaces = call_with_retry(admin.list_workspaces)

required_workspace_columns = {"Id", "Name", "Type", "State", "Capacity Id"}
missing_workspace_columns = required_workspace_columns - set(workspaces.columns)
if missing_workspace_columns:
    raise RuntimeError(f"Workspace API response is missing columns: {sorted(missing_workspace_columns)}")

workspaces = workspaces[workspaces["State"].str.lower() == "active"].copy()
workspaces = workspaces[
    ~workspaces["Type"].str.lower().isin({"personalgroup", "personal"})
].copy()
workspaces = workspaces.rename(columns={"Id": "workspace_id", "Name": "workspace_name"})
workspaces = workspaces[["workspace_id", "workspace_name", "Type", "State", "Capacity Id"]]
workspaces.columns = [
    "workspace_id",
    "workspace_name",
    "workspace_type",
    "state",
    "capacity_id",
]

if WORKSPACE_LIMIT is not None:
    log(f"PILOT MODE - limiting this run to {WORKSPACE_LIMIT} workspaces.")
    workspaces = workspaces.sort_values("workspace_name").head(WORKSPACE_LIMIT).copy()

log(f"Workspaces in scope: {len(workspaces):,}")
write_run_table(workspaces, "workspaces")
display(workspaces.head(20))

# MARKDOWN ********************
# ## Phase 2 - Report inventory
#
# The notebook lists reports once per workspace and excludes paginated reports, whose definitions use RDL rather than PBIR. Workspace-level failures are retained and included in final reconciliation.
#
# The reported format is advisory. Phase 5 derives the authoritative format from the returned definition parts.

# CELL ********************

REPORT_COLUMNS = [
    "workspace_id",
    "workspace_name",
    "report_id",
    "report_name",
    "report_type",
    "dataset_id",
    "web_url",
    "format_reported",
]


def list_reports_in_workspace(
    workspace_id: str, workspace_name: str
) -> List[Dict[str, Any]]:
    response = call_with_retry(
        _base_api,
        request=f"/v1.0/myorg/groups/{workspace_id}/reports",
        client="fabric_sp",
    )
    rows: List[Dict[str, Any]] = []
    for report in response.json().get("value", []):
        if (report.get("reportType") or "").lower() == "paginatedreport":
            continue
        raw_format = report.get("format")
        rows.append(
            {
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "report_id": report.get("id"),
                "report_name": report.get("name"),
                "report_type": report.get("reportType"),
                "dataset_id": report.get("datasetId"),
                "web_url": report.get("webUrl"),
                "format_reported": (raw_format or "").replace("-", "") or None,
            }
        )
    return rows


report_rows: List[Dict[str, Any]] = []
inventory_error_rows: List[Dict[str, Any]] = []

with auth_context():
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            submit_with_context(
                pool,
                list_reports_in_workspace,
                str(row["workspace_id"]),
                str(row["workspace_name"]),
            ): row
            for _, row in workspaces.iterrows()
        }
        for index, future in enumerate(cf.as_completed(futures), start=1):
            workspace = futures[future]
            try:
                report_rows.extend(future.result())
            except Exception as exc:  # noqa: BLE001 - failure is persisted
                inventory_error_rows.append(
                    {
                        "resource_type": "workspace",
                        "workspace_id": workspace["workspace_id"],
                        "workspace_name": workspace["workspace_name"],
                        "report_id": None,
                        "report_name": None,
                        "stage": "list_reports",
                        "status_code": _status_of(exc),
                        "error": str(exc)[:1000],
                    }
                )
            if index % 25 == 0:
                log(f"Inventoried {index:,}/{len(futures):,} workspaces")

reports = pd.DataFrame(report_rows, columns=REPORT_COLUMNS)
log(
    f"Reports in scope: {len(reports):,}; "
    f"workspace inventory failures: {len(inventory_error_rows):,}"
)

if not reports.empty:
    display(reports["format_reported"].value_counts(dropna=False).rename("reports"))

write_run_table(reports, "reports")

# MARKDOWN ********************
# ## Phase 3 - AppSource catalogue snapshot
#
# The AppSource catalogue enriches registered visual identifiers with display name, publisher, version, and certification status. Power BI certification prohibits external HTTP/S and WebSocket access, so certification is a useful capability signal.
#
# The catalogue endpoint used here is undocumented and versioned `2018-08-01-beta`. The notebook therefore validates its shape and a configurable minimum row count, retries transient failures, and stops rather than producing classifications from a suspiciously incomplete response. This snapshot does not identify organizational-store provenance.

# CELL ********************

CATALOG_URL = (
    "https://catalogapi.azure.com/offers"
    "?api-version=2018-08-01-beta"
    "&storefront=appsource"
    "&$filter=offerType+eq+%27PowerBIVisuals%27"
)

CATALOG_COLUMNS = [
    "visual_guid",
    "visual_key",
    "visual_display_name",
    "publisher",
    "publisher_id",
    "version",
    "is_certified",
    "is_preview",
    "is_stop_sell",
    "privacy_policy_uri",
    "legal_terms_uri",
    "support_uri",
    "categories",
]


def fetch_appsource_catalog() -> pd.DataFrame:
    items: List[Dict[str, Any]] = []
    url: Optional[str] = CATALOG_URL
    seen_urls: Set[str] = set()

    while url:
        if url in seen_urls:
            raise RuntimeError("AppSource catalogue returned a pagination loop.")
        seen_urls.add(url)

        response = call_with_retry(requests.get, url, timeout=60)
        response.raise_for_status()
        payload = response.json()
        page_items = payload.get("items")
        if not isinstance(page_items, list):
            raise RuntimeError("AppSource catalogue response has no items array.")
        items.extend(page_items)
        url = payload.get("nextPageLink")

    rows: List[Dict[str, Any]] = []
    for item in items:
        visual_guid = item.get("powerBIVisualId")
        if not visual_guid:
            continue
        categories = item.get("categoryIds") or []
        rows.append(
            {
                "visual_guid": visual_guid,
                "visual_key": str(visual_guid).casefold(),
                "visual_display_name": item.get("displayName"),
                "publisher": item.get("publisherDisplayName"),
                "publisher_id": item.get("publisherId"),
                "version": item.get("version"),
                "is_certified": "PowerBICertified" in categories,
                "is_preview": bool(item.get("isPreview")),
                "is_stop_sell": bool(item.get("isStopSell")),
                "privacy_policy_uri": item.get("privacyPolicyUri"),
                "legal_terms_uri": item.get("legalTermsUri"),
                "support_uri": item.get("supportUri"),
                "categories": ",".join(sorted(str(value) for value in categories)),
            }
        )

    dataframe = pd.DataFrame(rows, columns=CATALOG_COLUMNS)
    dataframe = dataframe.drop_duplicates(subset=["visual_key"], keep="last")
    if len(dataframe) < MIN_CATALOG_ROWS:
        raise RuntimeError(
            f"AppSource catalogue returned {len(dataframe)} rows; expected at least "
            f"{MIN_CATALOG_ROWS}. Review the endpoint before continuing."
        )
    return dataframe


catalog = fetch_appsource_catalog()
log(
    f"Catalogue: {len(catalog):,} visuals; "
    f"{int(catalog['is_certified'].sum()):,} certified"
)
write_run_table(catalog, "catalog")
display(catalog.head(10))

# MARKDOWN ********************
# ## Phase 4 - Definition parsers
#
# The parsers inspect documented report-definition parts and known custom-visual registration structures. They distinguish registration from active placement and retain parser warnings for malformed or unexpected content.
#
# Observed registration and placement locations include:
#
# ```text
# publicCustomVisuals[]
# resourcePackages[].resourcePackage
# sections[].visualContainers[].config.singleVisual.visualType
# PBIR definition/pages/{page}/visuals/{visual}/visual.json
# ```
#
# These structures can evolve. Unexpected payload types, malformed JSON, missing required report parts, and placed visual types absent from the registration list are reported as warnings rather than silently discarded.

# CELL ********************

def decode_parts(
    definition_json: Dict[str, Any]
) -> Tuple[Dict[str, bytes], List[str]]:
    parts: Dict[str, bytes] = {}
    warnings: List[str] = []
    raw_parts = (definition_json.get("definition") or {}).get("parts", []) or []

    for part in raw_parts:
        path = part.get("path")
        payload = part.get("payload")
        payload_type = part.get("payloadType", "InlineBase64")
        if not path or payload is None:
            warnings.append("Definition contained a part without path or payload.")
            continue
        if payload_type != "InlineBase64":
            warnings.append(f"Unsupported payload type {payload_type!r} for {path}.")
            continue
        try:
            parts[path] = base64.b64decode(payload)
        except Exception as exc:  # noqa: BLE001 - warning is persisted
            warnings.append(f"Could not decode {path}: {str(exc)[:160]}")

    return parts, warnings


def detect_format(parts: Dict[str, bytes]) -> str:
    if "definition/report.json" in parts:
        return "PBIR"
    if "report.json" in parts:
        return "PBIRLegacy"
    return "Unknown"


def _load_json(raw: Optional[bytes]) -> Optional[dict]:
    if raw is None:
        return None
    try:
        value = json.loads(raw.decode("utf-8-sig"))
        return value if isinstance(value, dict) else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _collect_registered(report_json: dict, custom_type_marker: Any) -> Dict[str, bool]:
    """Return visual GUID -> public registration flag."""
    registered: Dict[str, bool] = {}
    for entry in report_json.get("resourcePackages") or []:
        resource_package = (
            entry.get("resourcePackage", entry) if isinstance(entry, dict) else {}
        )
        if resource_package.get("type") == custom_type_marker:
            name = resource_package.get("name")
            if name:
                registered[str(name)] = False
    for item in report_json.get("publicCustomVisuals") or []:
        name = item.get("name") if isinstance(item, dict) else item
        if name:
            registered[str(name)] = True
    return registered


def parse_legacy(
    parts: Dict[str, bytes]
) -> Tuple[Dict[str, bool], List[Dict[str, Any]], List[str]]:
    report_json = _load_json(parts.get("report.json"))
    if report_json is None:
        return {}, [], ["PBIR-Legacy report.json is missing or invalid JSON."]

    registered = _collect_registered(report_json, custom_type_marker=0)
    placements: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for section in report_json.get("sections") or []:
        page_name = section.get("name")
        page_display_name = section.get("displayName")
        page_config = section.get("config") or {}
        if isinstance(page_config, str):
            try:
                page_config = json.loads(page_config)
            except json.JSONDecodeError:
                warnings.append(f"Invalid page config JSON on page {page_name!r}.")
                page_config = {}
        page_hidden = isinstance(page_config, dict) and page_config.get("visibility") == 1

        for visual_container in section.get("visualContainers") or []:
            config = visual_container.get("config")
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except json.JSONDecodeError:
                    warnings.append(
                        f"Invalid visual config JSON on page {page_name!r}; visual skipped."
                    )
                    continue
            if not isinstance(config, dict):
                warnings.append(f"Non-object visual config on page {page_name!r}; visual skipped.")
                continue
            single_visual = config.get("singleVisual")
            if not isinstance(single_visual, dict):
                continue
            visual_type = single_visual.get("visualType")
            if not visual_type:
                continue
            placements.append(
                {
                    "page_name": page_name,
                    "page_display_name": page_display_name,
                    "visual_name": config.get("name"),
                    "visual_type": str(visual_type),
                    "hidden": bool(page_hidden),
                }
            )

    return registered, placements, warnings


def parse_pbir(
    parts: Dict[str, bytes]
) -> Tuple[Dict[str, bool], List[Dict[str, Any]], List[str]]:
    report_json = _load_json(parts.get("definition/report.json"))
    if report_json is None:
        return {}, [], ["PBIR definition/report.json is missing or invalid JSON."]

    registered = _collect_registered(report_json, custom_type_marker="CustomVisual")
    page_display_names: Dict[str, str] = {}
    warnings: List[str] = []

    for path, raw in parts.items():
        match = re.match(r"^definition/pages/([^/]+)/page\.json$", path)
        if not match:
            continue
        page_json = _load_json(raw)
        if page_json is None:
            warnings.append(f"Invalid page JSON in {path}.")
            continue
        page_display_names[match.group(1)] = page_json.get("displayName") or match.group(1)

    placements: List[Dict[str, Any]] = []
    for path, raw in parts.items():
        match = re.match(
            r"^definition/pages/([^/]+)/visuals/([^/]+)/visual\.json$", path
        )
        if not match:
            continue
        visual_json = _load_json(raw)
        if visual_json is None:
            warnings.append(f"Invalid visual JSON in {path}; visual skipped.")
            continue
        visual = (
            visual_json.get("visual")
            if isinstance(visual_json.get("visual"), dict)
            else visual_json
        )
        visual_type = visual.get("visualType")
        if not visual_type:
            continue
        placements.append(
            {
                "page_name": match.group(1),
                "page_display_name": page_display_names.get(match.group(1), match.group(1)),
                "visual_name": visual_json.get("name") or match.group(2),
                "visual_type": str(visual_type),
                "hidden": bool(visual_json.get("isHidden", False)),
            }
        )

    return registered, placements, warnings


def extract_pbiviz_metadata(
    parts: Dict[str, bytes]
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    found: Dict[str, Dict[str, Any]] = {}
    warnings: List[str] = []

    for path, raw in parts.items():
        if not path.lower().endswith(".pbiviz.json"):
            continue
        metadata = _load_json(raw)
        if metadata is None:
            warnings.append(f"Invalid pbiviz metadata JSON in {path}.")
            continue
        visual = metadata.get("visual") or {}
        guid = visual.get("guid") or path.split("/")[-1].removesuffix(".pbiviz.json")
        privileges = (metadata.get("capabilities") or {}).get("privileges") or []
        if not isinstance(privileges, list):
            warnings.append(f"Unexpected privileges structure in {path}.")
            privileges = []

        privilege_names = sorted(
            str(privilege["name"])
            for privilege in privileges
            if isinstance(privilege, dict) and privilege.get("name")
        )
        web_urls: List[str] = []
        for privilege in privileges:
            if not isinstance(privilege, dict) or privilege.get("name") != "WebAccess":
                continue
            parameters = privilege.get("parameters") or []
            if isinstance(parameters, list):
                web_urls.extend(str(value) for value in parameters)
            else:
                warnings.append(f"Unexpected WebAccess parameters structure in {path}.")

        found[str(guid).casefold()] = {
            "pbiviz_version": visual.get("version"),
            "pbiviz_author": (metadata.get("author") or {}).get("name"),
            "pbiviz_privileges": ",".join(privilege_names) or None,
            "pbiviz_web_access_urls": ",".join(sorted(set(web_urls))) or None,
        }

    return found, warnings

# MARKDOWN ********************
# ## Phase 5 - Scan report definitions
#
# The notebook makes one long-running `getDefinition` request per report and parses the returned format. Every discovered report receives one row in `cv_audit_scan_status`, including reports with zero custom visuals and reports that fail.
#
# Each execution creates a new `run_id` snapshot. Re-running this cell in the same session replaces that run's rows, while separate notebook executions preserve separate snapshots. Failures and parser warnings are persisted without aborting unrelated reports.

# CELL ********************

VISUAL_COLUMNS = [
    "workspace_id",
    "workspace_name",
    "report_id",
    "report_name",
    "format_actual",
    "visual_guid",
    "visual_key",
    "registration_kind",
    "used_in_report",
    "placement_count",
    "pbiviz_version",
    "pbiviz_author",
    "pbiviz_privileges",
    "pbiviz_web_access_urls",
]
PLACEMENT_COLUMNS = [
    "workspace_id",
    "workspace_name",
    "report_id",
    "report_name",
    "page_name",
    "page_display_name",
    "visual_name",
    "visual_type",
    "visual_key",
    "hidden",
    "page_url",
]
STATUS_COLUMNS = [
    "workspace_id",
    "workspace_name",
    "report_id",
    "report_name",
    "format_reported",
    "format_actual",
    "status",
    "registered_visual_count",
    "placement_count",
    "warning_count",
]
ERROR_COLUMNS = [
    "severity",
    "resource_type",
    "workspace_id",
    "workspace_name",
    "report_id",
    "report_name",
    "stage",
    "status_code",
    "failure_category",
    "error",
]

catalog_keys = set(catalog["visual_key"])


def scan_report(metadata: Dict[str, Any]) -> Dict[str, Any]:
    workspace_id = str(metadata["workspace_id"])
    report_id = str(metadata["report_id"])
    result: Dict[str, Any] = {
        "meta": metadata,
        "error": None,
        "status_code": None,
        "registered": {},
        "placements": [],
        "pbiviz": {},
        "format_actual": None,
        "part_paths": [],
        "warnings": [],
    }

    try:
        definition = call_with_retry(
            _base_api,
            request=f"/v1/workspaces/{workspace_id}/reports/{report_id}/getDefinition",
            client="fabric_sp",
            method="post",
            status_codes=None,
            lro_return_json=True,
        )
        parts, decode_warnings = decode_parts(definition)
        result["warnings"].extend(decode_warnings)
        result["part_paths"] = sorted(parts)
        result["format_actual"] = detect_format(parts)

        if result["format_actual"] == "PBIR":
            registered, placements, parser_warnings = parse_pbir(parts)
        elif result["format_actual"] == "PBIRLegacy":
            registered, placements, parser_warnings = parse_legacy(parts)
        else:
            raise ValueError("Unrecognized definition layout: no supported report JSON part.")

        result["warnings"].extend(parser_warnings)

        registered_keys = {guid.casefold() for guid in registered}
        for placement in placements:
            visual_type = str(placement["visual_type"])
            visual_key = visual_type.casefold()
            if visual_key in catalog_keys and visual_key not in registered_keys:
                registered[visual_type] = True
                registered_keys.add(visual_key)
                result["warnings"].append(
                    f"Placed AppSource visual {visual_type!r} was absent from registrations."
                )

        pbiviz, metadata_warnings = extract_pbiviz_metadata(parts)
        result["warnings"].extend(metadata_warnings)
        result["registered"] = registered
        result["placements"] = placements
        result["pbiviz"] = pbiviz
    except Exception as exc:  # noqa: BLE001 - failure is persisted per report
        result["status_code"] = _status_of(exc)
        result["error"] = str(exc)[:1000]

    return result


visual_rows: List[Dict[str, Any]] = []
placement_rows: List[Dict[str, Any]] = []
status_rows: List[Dict[str, Any]] = []
scan_error_rows: List[Dict[str, Any]] = [
    {"severity": "error", "failure_category": "workspace_inventory", **row}
    for row in inventory_error_rows
]
sample_part_paths: List[Dict[str, Any]] = []

log(f"Scanning {len(reports):,} report definitions with {MAX_WORKERS} workers...")

with auth_context():
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            submit_with_context(pool, scan_report, dict(record))
            for record in reports.to_dict("records")
        ]
        for index, future in enumerate(cf.as_completed(futures), start=1):
            result = future.result()
            metadata = result["meta"]
            base = {
                "workspace_id": metadata["workspace_id"],
                "workspace_name": metadata["workspace_name"],
                "report_id": metadata["report_id"],
                "report_name": metadata["report_name"],
            }

            if result["error"]:
                status_code = result["status_code"]
                failure_category = (
                    "permission_or_encrypted_label"
                    if status_code == 403
                    else "throttled"
                    if status_code == 429
                    else "definition_or_api_error"
                )
                scan_error_rows.append(
                    {
                        "severity": "error",
                        "resource_type": "report",
                        **base,
                        "stage": "get_or_parse_definition",
                        "status_code": status_code,
                        "failure_category": failure_category,
                        "error": result["error"],
                    }
                )
                status_rows.append(
                    {
                        **base,
                        "format_reported": metadata.get("format_reported"),
                        "format_actual": result["format_actual"],
                        "status": "failed",
                        "registered_visual_count": 0,
                        "placement_count": 0,
                        "warning_count": len(result["warnings"]),
                    }
                )
                continue

            registered = result["registered"]
            placements_for_report = result["placements"]
            placed_keys = {
                str(placement["visual_type"]).casefold()
                for placement in placements_for_report
            }
            pbiviz = result["pbiviz"]

            for guid, is_public in registered.items():
                visual_key = guid.casefold()
                extra = pbiviz.get(visual_key, {})
                visual_rows.append(
                    {
                        **base,
                        "format_actual": result["format_actual"],
                        "visual_guid": guid,
                        "visual_key": visual_key,
                        "registration_kind": (
                            "public_registration" if is_public else "embedded_package"
                        ),
                        "used_in_report": visual_key in placed_keys,
                        "placement_count": sum(
                            1
                            for placement in placements_for_report
                            if str(placement["visual_type"]).casefold() == visual_key
                        ),
                        "pbiviz_version": extra.get("pbiviz_version"),
                        "pbiviz_author": extra.get("pbiviz_author"),
                        "pbiviz_privileges": extra.get("pbiviz_privileges"),
                        "pbiviz_web_access_urls": extra.get("pbiviz_web_access_urls"),
                    }
                )

            registered_keys = {guid.casefold() for guid in registered}
            custom_placements = [
                placement
                for placement in placements_for_report
                if str(placement["visual_type"]).casefold() in registered_keys
            ]
            for placement in custom_placements:
                visual_key = str(placement["visual_type"]).casefold()
                web_url = metadata.get("web_url") or ""
                page_name = placement.get("page_name") or ""
                page_url = f"{web_url}/{page_name}" if web_url and page_name else web_url or None
                placement_rows.append(
                    {**base, **placement, "visual_key": visual_key, "page_url": page_url}
                )

            for warning in result["warnings"]:
                scan_error_rows.append(
                    {
                        "severity": "warning",
                        "resource_type": "report",
                        **base,
                        "stage": "parse_definition",
                        "status_code": None,
                        "failure_category": "parser_warning",
                        "error": warning,
                    }
                )

            status_rows.append(
                {
                    **base,
                    "format_reported": metadata.get("format_reported"),
                    "format_actual": result["format_actual"],
                    "status": "succeeded",
                    "registered_visual_count": len(registered),
                    "placement_count": len(custom_placements),
                    "warning_count": len(result["warnings"]),
                }
            )

            if len(sample_part_paths) < 5 and result["part_paths"]:
                sample_part_paths.append(
                    {**base, "part_paths": " | ".join(result["part_paths"][:40])}
                )

            if index % 100 == 0:
                failed_count = sum(row["status"] == "failed" for row in status_rows)
                log(f"Scanned {index:,}/{len(futures):,}; failures: {failed_count:,}")

report_visuals = pd.DataFrame(visual_rows, columns=VISUAL_COLUMNS)
placements = pd.DataFrame(placement_rows, columns=PLACEMENT_COLUMNS)
scan_status = pd.DataFrame(status_rows, columns=STATUS_COLUMNS)
scan_errors = pd.DataFrame(scan_error_rows, columns=ERROR_COLUMNS)

write_run_table(report_visuals, "report_visuals")
write_run_table(placements, "placements")
write_run_table(scan_status, "scan_status")
write_run_table(scan_errors, "scan_errors")

log(
    f"Scan complete: {len(scan_status):,} reports accounted for, "
    f"{len(report_visuals):,} report-visual rows, "
    f"{len(placements):,} placements, "
    f"{int((scan_status['status'] == 'failed').sum()) if not scan_status.empty else 0:,} failures."
)

# CELL ********************

# Pilot diagnostics: inspect returned definition parts and parser coverage before
# increasing WORKSPACE_LIMIT.
if sample_part_paths:
    display(pd.DataFrame(sample_part_paths))

if not report_visuals.empty:
    metadata_rows = report_visuals["pbiviz_privileges"].notna().sum()
    log(
        f"Embedded pbiviz privilege metadata recovered for {metadata_rows:,} of "
        f"{len(report_visuals):,} report-visual rows."
    )

if not scan_status.empty:
    log("Reported vs actual format agreement:")
    display(
        pd.crosstab(
            scan_status["format_reported"].fillna("(absent)"),
            scan_status["format_actual"].fillna("(unavailable)"),
        )
    )
    log("Scan outcome reconciliation:")
    display(scan_status["status"].value_counts(dropna=False).rename("reports"))

# MARKDOWN ********************
# ## Phase 6 - Classify governance signals
#
# Each report-visual row receives an evidence-based class:
#
# | Class | Evidence | Interpretation |
# |---|---|---|
# | `builtin` | Identifier is in the explicit Microsoft allowlist | Microsoft-provided visual |
# | `certified_appsource` | Public registration and current catalogue certification | Certification prohibits external network access for the reviewed package |
# | `uncertified_appsource` | Public registration found in AppSource without certification | External access may be legitimate but requires governance review |
# | `public_not_in_catalog` | Public registration absent from the current catalogue | Delisted, renamed, or catalogue mismatch; provenance review required |
# | `embedded_provenance_unknown` | Embedded package registration | Source cannot be established from the report definition alone |
#
# The risk tier is a configurable triage opinion, not a security verdict. Active placement, declared privileges, tenant policy, publisher review, and organizational-store approval should be considered together.

# CELL ********************

FINDING_COLUMNS = VISUAL_COLUMNS + [
    "visual_display_name",
    "publisher",
    "catalog_version",
    "is_certified",
    "is_stop_sell",
    "privacy_policy_uri",
    "trust_class",
    "risk_tier",
    "risk_score",
    "risk_basis",
]

catalog_enrichment = catalog[
    [
        "visual_key",
        "visual_display_name",
        "publisher",
        "version",
        "is_certified",
        "is_stop_sell",
        "privacy_policy_uri",
    ]
].rename(columns={"version": "catalog_version"})

findings = report_visuals.merge(catalog_enrichment, on="visual_key", how="left")


def classify_visual(row: pd.Series) -> str:
    visual_guid = str(row["visual_guid"])
    if visual_guid in BUILTIN_VISUAL_ALLOWLIST:
        return "builtin"
    if row["registration_kind"] == "embedded_package":
        return "embedded_provenance_unknown"
    if pd.isna(row["is_certified"]):
        return "public_not_in_catalog"
    return "certified_appsource" if bool(row["is_certified"]) else "uncertified_appsource"


RISK_TIER = {
    "builtin": ("Low", 0),
    "certified_appsource": ("Low", 1),
    "uncertified_appsource": ("Review", 3),
    "public_not_in_catalog": ("Review", 3),
    "embedded_provenance_unknown": ("Review", 3),
}

if findings.empty:
    findings = pd.DataFrame(columns=FINDING_COLUMNS)
else:
    findings["trust_class"] = findings.apply(classify_visual, axis=1)
    findings["risk_tier"] = findings["trust_class"].map(lambda value: RISK_TIER[value][0])
    findings["risk_score"] = findings["trust_class"].map(lambda value: RISK_TIER[value][1])
    findings["used_in_report"] = as_bool(findings["used_in_report"])
    findings["risk_basis"] = "registration and catalogue classification"

    latent = ~findings["used_in_report"]
    findings.loc[latent, "risk_tier"] = "Latent"
    findings.loc[latent, "risk_score"] = 0
    findings.loc[latent, "risk_basis"] = "registered but not actively placed"

    declared_web_access = findings["pbiviz_web_access_urls"].notna()
    active_web_access = declared_web_access & findings["used_in_report"]
    findings.loc[active_web_access, "risk_tier"] = "Review"
    findings.loc[active_web_access, "risk_score"] = 4
    findings.loc[active_web_access, "risk_basis"] = "active visual declares WebAccess capability"

    findings["visual_display_name"] = findings["visual_display_name"].fillna(
        findings["visual_guid"]
    )

write_run_table(findings, "findings")

log("Governance classification for actively placed visuals:")
if findings.empty or not findings["used_in_report"].any():
    print("No actively placed custom visuals found in this run.")
else:
    display(
        findings[findings["used_in_report"]]
        .groupby(["trust_class", "risk_tier"])
        .agg(
            report_visual_rows=("report_id", "size"),
            distinct_visuals=("visual_guid", "nunique"),
            distinct_reports=("report_id", "nunique"),
            distinct_workspaces=("workspace_id", "nunique"),
        )
        .reset_index()
        .sort_values("report_visual_rows", ascending=False)
    )

# MARKDOWN ********************
# ## Phase 7 - Tenant controls and activity signals
#
# These signals answer different questions:
#
# 1. **Tenant settings:** which custom-visual capabilities are permitted and for whom.
# 2. **Definition classification:** which visuals are registered, placed, certified, embedded, or declare privileges.
# 3. **Authentication activity:** whether a custom visual received a Microsoft Entra token during the available audit window.
# 4. **Organizational-gallery activity:** whether administrators changed organizational visual entries during that window.
#
# Token issuance is evidence of authentication activity and identity exposure to the visual. It is **not evidence that a token or report data was transmitted externally**. Confirmed exfiltration requires additional network, endpoint, or incident-response evidence.

# CELL ********************

with auth_context():
    tenant_settings = call_with_retry(admin.list_tenant_settings)

VISUAL_SETTING_KEYS = {
    "EnableCustomVisuals",
    "EnableUncertifiedVisuals",
    "AllowCVToExportDataToFile",
    "CustomVisualsLocalStorage",
    "CustomVisualAADAccessToken",
}

required_setting_columns = {"Setting Name", "Title", "Enabled"}
missing_setting_columns = required_setting_columns - set(tenant_settings.columns)
if missing_setting_columns:
    raise RuntimeError(f"Tenant settings response is missing columns: {sorted(missing_setting_columns)}")

mask = tenant_settings["Setting Name"].isin(VISUAL_SETTING_KEYS) | tenant_settings[
    "Title"
].str.contains("visual", case=False, na=False)
visual_settings = tenant_settings[mask].copy()

missing_setting_keys = VISUAL_SETTING_KEYS - set(visual_settings["Setting Name"])
if missing_setting_keys:
    log(f"WARNING: expected tenant setting keys not returned: {sorted(missing_setting_keys)}")

write_run_table(visual_settings, "tenant_settings")
log("Visual-related tenant settings:")
display(visual_settings)

# CELL ********************

CUSTOM_VISUAL_OPERATIONS = {
    "GenerateCustomVisualAADAccessToken",
    "GenerateCustomVisualWACAccessToken",
    "InsertOrganizationalGalleryItem",
    "UpdateOrganizationalGalleryItem",
    "DeleteOrganizationalGalleryItem",
}
ACTIVITY_ERROR_COLUMNS = ["activity_date_utc", "status_code", "error"]

event_frames: List[pd.DataFrame] = []
activity_error_rows: List[Dict[str, Any]] = []
today_utc = dt.datetime.now(UTC).date()

with auth_context():
    for offset in range(1, ACTIVITY_LOOKBACK_DAYS + 1):
        activity_date = today_utc - dt.timedelta(days=offset)
        try:
            events = call_with_retry(
                admin.list_activity_events,
                start_time=f"{activity_date}T00:00:00Z",
                end_time=f"{activity_date}T23:59:59Z",
            )
            if events is None:
                activity_error_rows.append(
                    {
                        "activity_date_utc": str(activity_date),
                        "status_code": None,
                        "error": "Activity API returned no DataFrame.",
                    }
                )
                continue
            if not events.empty:
                relevant_events = events[
                    events["Operation"].isin(CUSTOM_VISUAL_OPERATIONS)
                ].copy()
                if not relevant_events.empty:
                    event_frames.append(relevant_events)
        except Exception as exc:  # noqa: BLE001 - date failure is persisted
            activity_error_rows.append(
                {
                    "activity_date_utc": str(activity_date),
                    "status_code": _status_of(exc),
                    "error": str(exc)[:1000],
                }
            )

activity_events = (
    pd.concat(event_frames, ignore_index=True) if event_frames else pd.DataFrame()
)
activity_errors = pd.DataFrame(activity_error_rows, columns=ACTIVITY_ERROR_COLUMNS)

write_run_table(activity_events, "activity_events")
write_run_table(activity_errors, "activity_errors")

if activity_errors.empty:
    log(f"Activity extraction completed for {ACTIVITY_LOOKBACK_DAYS} complete UTC days.")
else:
    log(
        f"WARNING: activity extraction failed for {len(activity_errors):,} of "
        f"{ACTIVITY_LOOKBACK_DAYS} UTC days."
    )
    display(activity_errors)

if activity_events.empty:
    log("No relevant custom-visual activity events found in the available window.")
    log(
        "IMPORTANT: absence of events is expected and is NOT evidence of safe use. "
        "GenerateCustomVisualAADAccessToken only fires for AppSource visuals when the "
        "'AppSource Custom Visuals SSO' tenant setting is enabled, which is off by default. "
        "No audit operation records a visual rendering, receiving, or transmitting data, so "
        "an empty result cannot show that data was not extracted."
    )
else:
    display(activity_events["Operation"].value_counts().rename("events"))
    token_events = activity_events[
        activity_events["Operation"] == "GenerateCustomVisualAADAccessToken"
    ]
    if not token_events.empty:
        log(
            f"Observed {len(token_events):,} custom-visual Entra token issuance events. "
            "These events do not establish external transmission. Note that the documented "
            "Power BI audit schema carries no field identifying which visual requested the "
            "token or the token's audience, so these events cannot be attributed to a "
            "specific visual from the log alone."
        )
        grouping_columns = [
            column
            for column in ["Workspace Id", "Report Name"]
            if column in token_events.columns
        ]
        if grouping_columns:
            display(
                token_events.groupby(grouping_columns, dropna=False)
                .size()
                .rename("events")
                .reset_index()
                .sort_values("events", ascending=False)
                .head(25)
            )

# MARKDOWN ********************
# ## Phase 8 - Reconciliation and governance summary
#
# Review coverage before reviewing risk. A run with inaccessible workspaces, failed report definitions, or activity-log gaps is an incomplete audit and must be presented as such.

# CELL ********************

report_keys = ["workspace_id", "report_id"]
if reports.duplicated(report_keys).any():
    raise RuntimeError("Report inventory contains duplicate workspace/report keys.")
if scan_status.duplicated(report_keys).any():
    raise RuntimeError("Scan status contains duplicate workspace/report keys.")
if len(scan_status) != len(reports):
    raise RuntimeError(
        f"Reconciliation failed: {len(reports)} reports discovered but "
        f"{len(scan_status)} report scan outcomes recorded."
    )

used = (
    findings[as_bool(findings["used_in_report"])].copy()
    if not findings.empty
    else findings.copy()
)
report_failures = (
    int((scan_status["status"] == "failed").sum()) if not scan_status.empty else 0
)
workspace_failures = len(inventory_error_rows)
parser_warnings = (
    int((scan_errors["severity"] == "warning").sum()) if not scan_errors.empty else 0
)

print("=" * 78)
print(f"CUSTOM VISUAL AUDIT - RUN {RUN_ID}")
print("=" * 78)
print(f"Workspaces selected .............. {len(workspaces):,}")
print(f"Workspace inventory failures .... {workspace_failures:,}")
print(f"Reports discovered ............... {len(reports):,}")
print(f"Reports successfully scanned ..... {len(reports) - report_failures:,}")
print(f"Reports that failed .............. {report_failures:,}")
print(f"Reports with custom registrations  {findings['report_id'].nunique() if not findings.empty else 0:,}")
print(f"Distinct registered visuals ...... {findings['visual_guid'].nunique() if not findings.empty else 0:,}")
print(f"Distinct actively placed visuals . {used['visual_guid'].nunique() if not used.empty else 0:,}")
print(f"Parser warnings .................. {parser_warnings:,}")
print(f"Activity dates not retrieved ..... {len(activity_errors):,}")
if not scan_status.empty:
    for format_name, count in scan_status["format_actual"].dropna().value_counts().items():
        print(f"  format {format_name:<14} ......... {count:,} reports")
print("=" * 78)

AUDIT_COMPLETE = workspace_failures == 0 and report_failures == 0 and activity_errors.empty
print(f"Audit completeness status: {'COMPLETE' if AUDIT_COMPLETE else 'INCOMPLETE'}")

# CELL ********************

log("GOVERNANCE REGISTER - actively placed visuals by classification")
if used.empty:
    print("No actively placed custom visuals found in this run.")
else:
    display(
        used.groupby(
            ["risk_score", "risk_tier", "trust_class", "risk_basis"], dropna=False
        )
        .agg(
            reports=("report_id", "nunique"),
            workspaces=("workspace_id", "nunique"),
            visuals=("visual_guid", "nunique"),
        )
        .reset_index()
        .sort_values(["risk_score", "reports"], ascending=[False, False])
    )

# CELL ********************

log("VISUALS REQUIRING REVIEW - ranked by evidence score and footprint")
review_candidates = used[used["risk_score"] >= 3]
if review_candidates.empty:
    print("No actively placed visuals met the review threshold.")
else:
    display(
        review_candidates.groupby(
            [
                "visual_guid",
                "visual_display_name",
                "trust_class",
                "risk_tier",
                "risk_score",
                "risk_basis",
                "publisher",
            ],
            dropna=False,
        )
        .agg(
            reports=("report_id", "nunique"),
            workspaces=("workspace_id", "nunique"),
            placements=("placement_count", "sum"),
        )
        .reset_index()
        .sort_values(["risk_score", "reports"], ascending=[False, False])
        .head(40)
    )

# CELL ********************

log("WORKSPACES BY REVIEW-CANDIDATE EXPOSURE")
workspace_review = used[used["risk_score"] >= 3]
if workspace_review.empty:
    print("No workspace contains an actively placed visual above the review threshold.")
else:
    display(
        workspace_review.groupby(["workspace_id", "workspace_name"])
        .agg(
            review_visuals=("visual_guid", "nunique"),
            affected_reports=("report_id", "nunique"),
            maximum_score=("risk_score", "max"),
        )
        .reset_index()
        .sort_values(["maximum_score", "affected_reports"], ascending=[False, False])
        .head(30)
    )

# CELL ********************

log("EMBEDDED VISUAL PACKAGES - provenance review required")
embedded = used[used["trust_class"] == "embedded_provenance_unknown"]
if embedded.empty:
    print("No actively placed embedded visual packages found.")
else:
    display(
        embedded.groupby(
            [
                "visual_guid",
                "pbiviz_author",
                "pbiviz_version",
                "pbiviz_privileges",
                "pbiviz_web_access_urls",
            ],
            dropna=False,
        )
        .agg(
            reports=("report_id", "nunique"),
            workspaces=("workspace_id", "nunique"),
        )
        .reset_index()
        .sort_values("reports", ascending=False)
    )

# CELL ********************

if not scan_errors.empty:
    log("COVERAGE FAILURES AND PARSER WARNINGS")
    display(
        scan_errors.groupby(
            ["severity", "stage", "failure_category"], dropna=False
        )
        .size()
        .rename("occurrences")
        .reset_index()
        .sort_values(["severity", "occurrences"], ascending=[True, False])
    )
    display(
        scan_errors[
            [
                "severity",
                "workspace_name",
                "report_name",
                "stage",
                "failure_category",
                "error",
            ]
        ].head(50)
    )

# MARKDOWN ********************
# ## Operating and interpretation guidance
#
# ### Before increasing scope
#
# 1. Run the pilot against a small, representative set of workspaces.
# 2. Compare known PBIR and PBIR-Legacy reports with manual inspection in Power BI Desktop.
# 3. Confirm that `cv_audit_scan_status` contains one outcome for every discovered report.
# 4. Review every workspace/report failure and parser warning before presenting findings.
# 5. Pin the accepted package versions in a Fabric Custom Environment.
#
# ### Triage order
#
# 1. **Coverage gaps:** resolve inaccessible workspaces, failed definitions, and missing activity dates first.
# 2. **Active declared WebAccess:** verify the declared destinations, publisher documentation, business purpose, and approved data handling.
# 3. **Embedded packages:** establish whether each visual came from an approved organizational process or an unmanaged file.
# 4. **Uncertified AppSource visuals:** review the reason certification is unavailable; external access can be legitimate.
# 5. **Public registrations absent from the catalogue:** investigate delisting, identifier changes, and catalogue completeness.
# 6. **Token issuance events:** the documented audit schema does not name the requesting visual or the token audience, so scope these by workspace and report and correlate with approved SSO use. Issuance alone is not exfiltration, and absence of issuance is not assurance.
#
# ### What the activity log cannot tell you
#
# Custom visual code runs in a sandboxed iframe in the user's browser. A call from a visual to an external endpoint goes directly from the browser to that endpoint and never traverses Microsoft infrastructure, so no Power BI, Fabric, or Microsoft 365 audit log records it. No operation fires when a visual renders, receives data, or transmits data.
#
# Consequently **absence of audit events is not exculpatory**: a clean activity log cannot be used to demonstrate that data was not extracted. The logged custom-visual operations record token and governance actions only. Native egress paths (`ExportReport`, `ExportArtifact`, `ExportTile`, `AnalyzeInExcel`, `AnalyzedByExternalApplication`) are logged but are not triggered by a visual's own outbound calls.
#
# ### Evidence required to confirm an incident
#
# This audit establishes exposure and governance signals. Confirming external transmission requires independent evidence such as proxy or firewall telemetry, endpoint logs, browser/network captures, publisher-side logs, or incident-response findings. Preserve timestamps and identifiers according to your organization's evidence-handling policy.
#
# ### Scheduling
#
# Schedule a new notebook execution for each snapshot. Every output row carries a unique `run_id`; downstream reports should filter to an explicitly selected run or a curated latest-complete-run view. Apply Lakehouse access controls and a documented retention period because outputs can contain report URLs and user identifiers.
#
# ### Public-repository checklist
#
# - Keep secrets and tenant identifiers out of source control.
# - Do not commit executed outputs or exported Delta data.
# - Document the tested Fabric Runtime and exact Custom Environment package versions.
# - Include an open-source license, contribution guidance, and a security-reporting policy at repository level.
# - Treat the undocumented AppSource catalogue endpoint and the private `semantic-link-labs` helper as compatibility risks that require regression testing after dependency updates.
