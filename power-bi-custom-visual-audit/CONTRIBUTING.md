# Contributing

Keep changes customer-agnostic and make them first in the cell-delimited `.py` source. Regenerate the corresponding `.ipynb` file; do not hand-maintain both representations.

## Development workflow

From the repository root:

```powershell
python power-bi-custom-visual-audit/tools/build_notebook.py power-bi-custom-visual-audit/src/custom_visuals_audit.py power-bi-custom-visual-audit/notebooks/custom_visuals_audit.ipynb
python power-bi-custom-visual-audit/tools/build_notebook.py power-bi-custom-visual-audit/src/grant_workspace_access.py power-bi-custom-visual-audit/notebooks/grant_workspace_access.ipynb
python power-bi-custom-visual-audit/tests/test_notebook.py power-bi-custom-visual-audit/notebooks/custom_visuals_audit.ipynb
python power-bi-custom-visual-audit/tests/test_notebook.py power-bi-custom-visual-audit/notebooks/grant_workspace_access.ipynb
```

When editing a notebook in Fabric, export it and run `tools/sync_from_ipynb.py` before editing its `.py` source again. The source files under `src/` remain authoritative.

## Pull request expectations

- Explain the behavior and evidence affected by the change.
- Add or update an offline fixture for parser and helper changes.
- Keep notebook outputs and execution counts empty.
- Do not include customer names, tenant or object IDs, report URLs, email addresses, credentials, audit exports, or screenshots containing customer data.
- Record the Fabric Runtime and exact dependency versions used for any live acceptance test, without including tenant-specific details.
- Treat changes to `grant_workspace_access` as privileged and request an independent security review.