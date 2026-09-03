# Tenant-Wide Power BI Custom Visual Audit

A Microsoft Fabric notebook package for inventorying custom visual registration and placement across accessible Power BI workspaces. It enriches report-definition evidence with AppSource certification metadata, relevant tenant settings, and recent custom-visual authentication activity.

This project identifies governance and exposure signals. It does not prove data exfiltration.

## What to use

Most customers need only the README and the notebooks in [`notebooks/`](notebooks/).

| Path | Audience | Purpose |
|---|---|---|
| `notebooks/custom_visuals_audit.ipynb` | Audit operator | Main Fabric notebook; reads APIs and writes audit results to the attached Lakehouse |
| `notebooks/grant_workspace_access.ipynb` | Fabric administrator | Optional utility for a separately approved, time-boxed grant/scan/revoke campaign; performs tenant-wide writes |
| `docs/CUSTOMER_ACCEPTANCE.md` | Delivery and customer owners | Template for recording pilot evidence, limitations, access cleanup, and sign-off |
| `src/` | Maintainers | Cell-delimited notebook source of truth |
| `tools/` | Maintainers | Notebook build and synchronization utilities |
| `tests/` | Maintainers | Offline syntax, metadata, parser, and helper validation |

The root-level [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md) describe maintenance and vulnerability reporting. Customers do not need to run anything in `src/`, `tools/`, or `tests/` to perform an audit.

For maintainers, the `.py` files in `src/` are the source of truth. After editing a notebook directly, run `tools/sync_from_ipynb.py` before making further source changes.

```text
power-bi-custom-visual-audit/
|-- notebooks/   Customer-facing Fabric notebooks
|-- docs/        Acceptance and handoff material
|-- src/         Maintainer-owned notebook source
|-- tools/       Build and synchronization utilities
|-- tests/       Offline validation
|-- README.md    Setup, operation, and interpretation
|-- SECURITY.md  Vulnerability and operational security policy
`-- CONTRIBUTING.md
```

## Quick start

1. Download this folder or clone the repository.
2. In a Fabric workspace, create or select a restricted Lakehouse for audit output.
3. Create a Fabric Custom Environment, add pinned `semantic-link` and `semantic-link-labs` packages, publish it, and attach it to the notebook.
4. Import `notebooks/custom_visuals_audit.ipynb`, attach the Lakehouse, and review the configuration cell before running anything.
5. For the first run, set `USE_SERVICE_PRINCIPAL = False` and keep `WORKSPACE_LIMIT = 5`, or configure the approved service principal and Key Vault secrets described below.
6. Run all cells and review the reconciliation summary. Do not increase the workspace scope until every pilot failure and warning is understood.

The optional `notebooks/grant_workspace_access.ipynb` notebook is not part of the normal quick start. Use it only through the separately approved access campaign described under [Safety model](#safety-model).

## Configuration

| Setting | Initial value | Guidance |
|---|---:|---|
| `USE_SERVICE_PRINCIPAL` | `True` | Set to `False` for an interactive pilot; use `True` only after Key Vault and tenant access are configured. |
| `KEY_VAULT_URI` | Placeholder | Replace with the URI of the approved Key Vault. Secret values never belong in the notebook. |
| `KV_SECRET_*` | Example names | Change these only if the Key Vault uses different secret names. |
| `WORKSPACE_LIMIT` | `5` | Keep the pilot bounded; set to `None` only after acceptance. |
| `MAX_WORKERS` | `4` | Reduce if tenant throttling is observed. Do not exceed the validation bound of 16. |
| `ACTIVITY_LOOKBACK_DAYS` | `28` | Valid range is 1-28 days. |
| `TABLE_PREFIX` | `cv_audit` | Change if the Lakehouse naming convention requires a different isolated prefix. |

## Safety model

The audit notebook does not modify Power BI reports, tenant settings, or workspace permissions. It does write run-scoped Delta tables to its attached Lakehouse.

The separate workspace access utility can grant and revoke Contributor access across many workspaces. It defaults to `DRY_RUN = True` and `PROBE_ONLY = True`. Treat it as privileged administrative code: review it independently, restrict the workspace allowlist, obtain approval, run it as an authorized Fabric administrator, verify its ledger, and revoke temporary access after the scan.

Never run the grant utility merely because it is included in this package.

## Requirements

- A Microsoft Fabric Runtime PySpark notebook with an attached Lakehouse.
- A Fabric Custom Environment with tested, pinned versions of `semantic-link` and `semantic-link-labs`.
- A dedicated audit identity with access to the required read-only admin APIs.
- Read and write permission on each report passed to `getDefinition`. This generally requires Contributor or stronger workspace access.
- Key Vault access when service-principal authentication is enabled.

Do not commit credentials, tenant identifiers, notebook outputs, or generated audit data.

## Run sequence

1. Configure and pin a Fabric Custom Environment.
2. Set the Key Vault URI and secret names in the audit notebook.
3. Keep `WORKSPACE_LIMIT = 5` for an acceptance pilot.
4. Run the offline tests.
5. Run the pilot and compare representative PBIR and PBIR-Legacy reports with manual inspection.
6. Resolve all workspace failures, report failures, and parser warnings.
7. Increase scope only after the reconciliation output is understood.

If temporary Contributor access is approved, use the access utility as a separate grant/scan/revoke campaign. The audit identity must not grant its own access.

## Operating models

Choose one access model before running the solution.

### Scheduled audit with standing access

Use this model when the customer has granted the audit identity ongoing Contributor access through its normal identity-governance process.

- Schedule only `custom_visuals_audit.ipynb` in a Fabric Data Pipeline.
- Do not include `grant_workspace_access.ipynb` in the pipeline.
- Each notebook execution creates a new run-scoped snapshot with a unique `run_id`.
- Monitor the pipeline outcome and the notebook's reconciliation status; a successful pipeline activity does not by itself prove complete audit coverage.

### Time-boxed audit campaign

Use this model when the customer does not permit standing Contributor access.

1. An authorized Fabric administrator reviews and manually runs `grant_workspace_access.ipynb`, starting in dry-run and probe-only modes and using an approved workspace allowlist.
2. Run `custom_visuals_audit.ipynb` manually or from a Fabric Data Pipeline.
3. Review the reconciliation status and preserve the audit `run_id`.
4. Return to `grant_workspace_access.ipynb`, revoke the campaign, and resolve every failed revocation.

Do not schedule the grant and revoke utility as part of the default unattended pipeline. It changes permissions tenant-wide and is intentionally protected by administrator approval, dry-run and probe interlocks, throttling, a grant ledger, and explicit revocation verification.

## Customer acceptance

Before relying on the results, the customer and delivery owner should record:

- The Fabric Runtime and exact package versions attached to the notebook.
- The audit identity, approved workspace scope, and authorization owner.
- The pilot `run_id` and a manual comparison for representative PBIR and PBIR-Legacy reports.
- The disposition of every row in `cv_audit_scan_errors` and `cv_audit_activity_errors`.
- Whether the final reconciliation status is `COMPLETE`; otherwise, the documented coverage limitations.
- The Lakehouse access controls, retention period, and named owner for audit data.
- If temporary access was used, the grant campaign `run_id` and evidence that revocation completed.

Use [docs/CUSTOMER_ACCEPTANCE.md](docs/CUSTOMER_ACCEPTANCE.md) as the handoff record. It intentionally contains no customer-specific defaults and should be completed outside the public source tree.

## Outputs

Every persisted row carries a `run_id`. Downstream reports must filter to an explicitly selected run or to a curated latest-complete-run view.

| Table | Purpose |
|---|---|
| `cv_audit_workspaces` | Selected workspace inventory |
| `cv_audit_reports` | Discovered report inventory |
| `cv_audit_catalog` | AppSource catalogue snapshot |
| `cv_audit_report_visuals` | Registered and actively placed custom visuals |
| `cv_audit_placements` | Report/page/visual placement details |
| `cv_audit_scan_status` | One reconciled outcome per discovered report |
| `cv_audit_scan_errors` | Workspace/report failures and parser warnings |
| `cv_audit_tenant_settings` | Relevant tenant controls |
| `cv_audit_activity_events` | Token and organizational-gallery activity |
| `cv_audit_activity_errors` | UTC dates that could not be retrieved |
| `cv_audit_findings` | Joined governance register |

These tables can contain workspace names, report names and URLs, user identifiers, publisher metadata, and declared WebAccess endpoints. Apply Lakehouse access controls and a documented retention policy.

## Interpretation

- `certified_appsource` means the catalogue reports certification for the reviewed package. Certification prohibits external HTTP/S and WebSocket access by that visual version, but it is not a general publisher or data-handling guarantee.
- `uncertified_appsource` means certification is absent. External access can be legitimate and requires contextual review.
- `embedded_provenance_unknown` means the report contains an embedded package. The definition alone cannot determine whether it came from a file, an organizational store, or another approved process.
- A Microsoft Entra token issuance event records authentication activity. It does not establish that the token or report data was sent externally.
- A run marked `INCOMPLETE` must not be represented as tenant-wide coverage.

The Fabric API blocks `getDefinition` for reports with encrypted sensitivity labels. Those reports remain explicit manual-review gaps.

## Validation

Run from the repository root:

```powershell
python power-bi-custom-visual-audit/tests/test_notebook.py power-bi-custom-visual-audit/notebooks/custom_visuals_audit.ipynb
python power-bi-custom-visual-audit/tests/test_notebook.py power-bi-custom-visual-audit/notebooks/grant_workspace_access.ipynb
```

Regenerate the audit notebook after source edits:

```powershell
python power-bi-custom-visual-audit/tools/build_notebook.py power-bi-custom-visual-audit/src/custom_visuals_audit.py power-bi-custom-visual-audit/notebooks/custom_visuals_audit.ipynb
```

The offline suite validates notebook syntax and metadata, PBIR and PBIR-Legacy parser fixtures, malformed input handling, privilege extraction, Boolean normalization, and explicit Spark schema preparation. It does not validate tenant permissions, API behavior, throttling, or live report-definition variants.

## Compatibility risks

The AppSource catalogue endpoint is undocumented and beta. The notebook also isolates, but currently depends on, a private `semantic-link-labs` helper. Re-run the offline suite and a controlled tenant pilot after dependency or Fabric Runtime updates.

## Public release checklist

- Confirm the repository-level [MIT license](../LICENSE) is appropriate for the release.
- Review [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md), and enable GitHub private vulnerability reporting when the repository is published.
- Document the exact Fabric Runtime and package versions used for the latest acceptance run.
- Confirm the repository contains no executed outputs, customer names, tenant IDs, report URLs, or audit exports.
- Review the grant utility separately from the read-only audit.

## Current validation status

Offline tests are included. A live-tenant acceptance run is still required before representing the notebook as production validated.

## Support and changes

This is a community solution, not a Microsoft product or support entitlement. Validate it in a controlled tenant after every Fabric Runtime, API, or dependency change. Report suspected vulnerabilities privately as described in [SECURITY.md](SECURITY.md); use [CONTRIBUTING.md](CONTRIBUTING.md) for all other changes.
