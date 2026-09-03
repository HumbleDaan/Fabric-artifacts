"""Convert the delimited .py notebook source into a Fabric-importable .ipynb."""
import hashlib
import json
import re
import sys
from pathlib import Path

SRC = Path(sys.argv[1])
DST = Path(sys.argv[2])

DELIM = re.compile(r"^# (MARKDOWN|CELL) \*{4,}\s*$")

cells = []
kind = None
buf = []


def flush():
    if kind is None:
        return
    text = "\n".join(buf).strip("\n")
    if not text.strip():
        return
    cell_id = hashlib.sha256(f"{kind}\0{text}".encode("utf-8")).hexdigest()[:12]
    if kind == "MARKDOWN":
        # strip the leading "# " comment marker from markdown lines
        lines = [re.sub(r"^# ?", "", ln) for ln in text.split("\n")]
        cells.append({
            "cell_type": "markdown",
            "id": cell_id,
            "metadata": {"id": cell_id, "language": "markdown"},
            "source": [ln + "\n" for ln in lines][:-1] + [lines[-1]],
        })
    else:
        lines = text.split("\n")
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "id": cell_id,
            "metadata": {"id": cell_id, "language": "python"},
            "outputs": [],
            "source": [ln + "\n" for ln in lines][:-1] + [lines[-1]],
        })


for line in SRC.read_text(encoding="utf-8").split("\n"):
    m = DELIM.match(line)
    if m:
        flush()
        kind = m.group(1)
        buf = []
    else:
        buf.append(line)
flush()

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Synapse PySpark",
            "language": "Python",
            "name": "synapse_pyspark",
        },
        "language_info": {"name": "python"},
        "microsoft": {
            "language": "python",
            "language_group": "synapse_pyspark",
        },
        "widgets": {},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}

DST.write_text(json.dumps(nb, indent=1, ensure_ascii=False), encoding="utf-8")
md = sum(1 for c in cells if c["cell_type"] == "markdown")
code = sum(1 for c in cells if c["cell_type"] == "code")
print(f"Wrote {DST} - {len(cells)} cells ({md} markdown, {code} code)")
