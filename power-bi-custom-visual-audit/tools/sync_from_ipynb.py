"""Convert a Fabric .ipynb back into the delimited .py notebook source.

Inverse of build_notebook.py. Needed when a notebook is edited directly (in
Fabric or an IDE) and those edits have to be folded back into the .py, which
remains the source of truth.

Usage:
    python tools/sync_from_ipynb.py notebooks/custom_visuals_audit.ipynb src/custom_visuals_audit.py

Round-trip fidelity is verified by tools/build_notebook.py; run both and diff.
"""
import json
import sys
from pathlib import Path

SRC = Path(sys.argv[1])
DST = Path(sys.argv[2])

MARKDOWN_DELIM = "# MARKDOWN ********************"
CODE_DELIM = "# CELL ********************"

nb = json.loads(SRC.read_text(encoding="utf-8"))

chunks = []
for cell in nb["cells"]:
    source = "".join(cell["source"]).rstrip("\n")
    if cell["cell_type"] == "markdown":
        # Re-apply the comment marker build_notebook.py strips back off.
        body = "\n".join(
            f"# {line}" if line.strip() else "#" for line in source.split("\n")
        )
        chunks.append(f"{MARKDOWN_DELIM}\n{body}")
    elif cell["cell_type"] == "code":
        chunks.append(f"{CODE_DELIM}\n\n{source}")

DST.write_text("\n\n".join(chunks) + "\n", encoding="utf-8")

md = sum(1 for c in nb["cells"] if c["cell_type"] == "markdown")
code = sum(1 for c in nb["cells"] if c["cell_type"] == "code")
print(f"Wrote {DST} - {len(nb['cells'])} cells ({md} markdown, {code} code)")
