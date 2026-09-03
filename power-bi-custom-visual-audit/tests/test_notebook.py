"""Validate the audit notebook: syntax-check every code cell, then unit-test the parsers
against synthetic PBIR and PBIRLegacy definitions matching the real-world structures."""
import base64
import json
import re
import sys
import types
from pathlib import Path

NB = Path(sys.argv[1])
nb = json.loads(NB.read_text(encoding="utf-8"))

code_cells = [c for c in nb["cells"] if c["cell_type"] == "code"]
print(f"Syntax-checking {len(code_cells)} code cells...")

for index, cell in enumerate(nb["cells"], 1):
    expected_language = "markdown" if cell["cell_type"] == "markdown" else "python"
    assert cell.get("id"), f"cell {index} has no stable id"
    assert cell.get("metadata", {}).get("id") == cell["id"], \
        f"cell {index} metadata.id does not match its cell id"
    assert cell.get("metadata", {}).get("language") == expected_language, \
        f"cell {index} has no {expected_language} language metadata"
print("  cell IDs and language metadata: OK")

failures = 0
for i, c in enumerate(code_cells, 1):
    src = "".join(c["source"])
    # strip notebook magics, which are not valid Python
    src = "\n".join("" if ln.strip().startswith(("%", "!")) else ln
                    for ln in src.split("\n"))
    try:
        compile(src, f"<cell {i}>", "exec")
    except SyntaxError as e:
        failures += 1
        print(f"  FAIL cell {i}: {e}")
if failures:
    sys.exit(f"{failures} cells failed to compile")
print("  all cells compile cleanly")

# ---------------------------------------------------------------------------
# Extract the parser cell and exercise it in isolation.
# ---------------------------------------------------------------------------
parser_src = None
for c in code_cells:
    s = "".join(c["source"])
    if "def parse_legacy" in s and "def parse_pbir" in s:
        parser_src = s
        break

if parser_src is None:
    # Utility notebooks (e.g. grant_workspace_access) have no parsers to exercise.
    print("  no parser cell - syntax check only")
    sys.exit(0)

from typing import Any, Dict, List, Optional, Set, Tuple

mod = types.ModuleType("parsers")
mod.__dict__.update({
    "base64": base64, "json": json, "re": re,
    "Any": Any, "Dict": Dict, "List": List,
    "Optional": Optional, "Set": Set, "Tuple": Tuple,
})
exec(compile(parser_src, "<parsers>", "exec"), mod.__dict__)

# --- Synthetic PBIRLegacy report ------------------------------------------
# Mirrors the real structures verified across 10 production report.json files:
#   * resourcePackages entries wrapped in a "resourcePackage" key, type 0 == CustomVisual
#   * publicCustomVisuals as a flat array of GUID strings
#   * visualContainers[].config as a JSON *string*
#   * a visual group (singleVisualGroup) with no visualType, as a flat sibling
legacy_report = {
    "publicCustomVisuals": ["HierarchySlicer1458836712039", "UnusedPublicVisual123"],
    "resourcePackages": [
        {"resourcePackage": {
            "name": "htmlContent443BE3AD55E043BF878BED274D3A6855",
            "type": 0,
            "items": [{"name": "htmlContent443BE3AD55E043BF878BED274D3A6855.pbiviz.json",
                       "path": "htmlContent443BE3AD55E043BF878BED274D3A6855.pbiviz.json",
                       "type": 5}]}},
        {"resourcePackage": {"name": "RegisteredResources", "type": 1,
                             "items": [{"name": "logo.png", "type": 100}]}},
        {"resourcePackage": {"name": "SharedResources", "type": 2,
                             "items": [{"name": "CY18SU07", "type": 202}]}},
    ],
    "sections": [
        {"name": "ReportSection1", "displayName": "Overview", "visualContainers": [
            {"config": json.dumps({"name": "v1", "singleVisual": {"visualType": "card"}})},
            {"config": json.dumps({"name": "v2", "singleVisual": {
                "visualType": "HierarchySlicer1458836712039"}})},
            {"config": json.dumps({"name": "grp1", "singleVisualGroup": {
                "displayName": "Group A", "groupMode": 0}})},
        ]},
        {"name": "ReportSection2", "displayName": "Detail", "visualContainers": [
            {"config": json.dumps({"name": "v3", "singleVisual": {
                "visualType": "htmlContent443BE3AD55E043BF878BED274D3A6855"}})},
            {"config": "{ this is not valid json"},
        ]},
    ],
    "pods": [{"boundSection": "ReportSection1", "config": "{}", "name": "Pod"}],
}

legacy_parts = {"report.json": json.dumps(legacy_report).encode()}
assert mod.detect_format(legacy_parts) == "PBIRLegacy", mod.detect_format(legacy_parts)

reg, place, warns = mod.parse_legacy(legacy_parts)
assert reg == {
    "htmlContent443BE3AD55E043BF878BED274D3A6855": False,   # private / embedded
    "HierarchySlicer1458836712039": True,                    # AppSource
    "UnusedPublicVisual123": True,                           # registered, never placed
}, reg
used = {p["visual_type"] for p in place}
assert used == {"card", "HierarchySlicer1458836712039",
                "htmlContent443BE3AD55E043BF878BED274D3A6855"}, used
assert "UnusedPublicVisual123" not in used, "registered-but-unused must not appear as used"
assert len(place) == 3, f"malformed config and visual group must be skipped, got {len(place)}"
assert {p["page_display_name"] for p in place} == {"Overview", "Detail"}
# The malformed visualContainer config must be reported, not silently dropped.
assert warns, "malformed config must produce a warning"
print("  parse_legacy: OK (registered/used split, group + malformed-config skip, page names)")
print(f"  parse_legacy warnings surfaced: {len(warns)}")

# --- Synthetic PBIR report -------------------------------------------------
pbir_parts = {
    "definition/report.json": json.dumps({
        "publicCustomVisuals": ["deneb7E15AEF80B9E4D4F8E12924291ECE89A"],
        "resourcePackages": [
            {"name": "privateViz9988", "type": "CustomVisual",
             "items": [{"name": "privateViz9988.pbiviz.json",
                        "type": "CustomVisualMetadata"}]},
            {"name": "SharedResources", "type": "SharedResources", "items": []},
        ],
    }).encode(),
    "definition/pages/pg1/page.json": json.dumps({"displayName": "Sales Overview"}).encode(),
    "definition/pages/pg1/visuals/vA/visual.json": json.dumps(
        {"name": "vA", "visual": {"visualType": "deneb7E15AEF80B9E4D4F8E12924291ECE89A"}}).encode(),
    "definition/pages/pg1/visuals/vB/visual.json": json.dumps(
        {"name": "vB", "visual": {"visualType": "columnChart"}}).encode(),
    "definition/pages/pg2/visuals/vC/visual.json": json.dumps(
        {"name": "vC", "visual": {"visualType": "privateViz9988"}}).encode(),
    "StaticResources/privateViz9988.pbiviz.json": json.dumps({
        "visual": {"guid": "privateViz9988", "version": "1.2.0"},
        "author": {"name": "Internal BI Team"},
        "capabilities": {"privileges": [
            {"name": "WebAccess", "essential": True,
             "parameters": ["https://telemetry.example.com"]},
            {"name": "ExportContent", "essential": False},
        ]},
    }).encode(),
}

assert mod.detect_format(pbir_parts) == "PBIR", mod.detect_format(pbir_parts)
reg2, place2, warns2 = mod.parse_pbir(pbir_parts)
assert reg2 == {"privateViz9988": False,
                "deneb7E15AEF80B9E4D4F8E12924291ECE89A": True}, reg2
used2 = {p["visual_type"] for p in place2}
assert used2 == {"deneb7E15AEF80B9E4D4F8E12924291ECE89A", "columnChart",
                 "privateViz9988"}, used2
by_name = {p["visual_name"]: p for p in place2}
assert by_name["vA"]["page_display_name"] == "Sales Overview"
assert by_name["vC"]["page_display_name"] == "pg2", "missing page.json must fall back to folder"
print("  parse_pbir: OK (flat resourcePackages, page displayName resolution, fallback)")

meta, meta_warns = mod.extract_pbiviz_metadata(pbir_parts)
# Keys are normalised to lower case so they join against `visual_key`.
assert "privateviz9988" in meta, meta
m = meta["privateviz9988"]
assert m["pbiviz_version"] == "1.2.0"
assert m["pbiviz_author"] == "Internal BI Team"
assert m["pbiviz_privileges"] == "ExportContent,WebAccess", m["pbiviz_privileges"]
assert m["pbiviz_web_access_urls"] == "https://telemetry.example.com"
print("  extract_pbiviz_metadata: OK (version, author, privileges, WebAccess URLs)")

# --- Degenerate inputs -----------------------------------------------------
assert mod.parse_legacy({}) == ({}, [], ["PBIR-Legacy report.json is missing or invalid JSON."])
assert mod.parse_pbir({})[:2] == ({}, [])
assert mod.detect_format({}) == "Unknown"
assert mod.parse_legacy({"report.json": b"not json"})[:2] == ({}, [])
# A missing/unparseable definition must warn rather than look like a clean empty result.
assert mod.parse_legacy({"report.json": b"not json"})[2], "invalid JSON must warn"
# publicCustomVisuals absent entirely (~40% of real reports)
r3, _, _ = mod.parse_legacy({"report.json": json.dumps(
    {"sections": [], "resourcePackages": []}).encode()})
assert r3 == {}, r3
# dict-shaped publicCustomVisuals entries
r4, _, _ = mod.parse_legacy({"report.json": json.dumps(
    {"publicCustomVisuals": [{"name": "DictShaped1"}], "sections": []}).encode()})
assert r4 == {"DictShaped1": True}, r4
print("  degenerate inputs: OK (empty, malformed, absent keys, dict-shaped entries)")

# --- decode_parts ----------------------------------------------------------
dp, dp_warns = mod.decode_parts({"definition": {"parts": [
    {"path": "report.json", "payload": base64.b64encode(b'{"a":1}').decode()},
    {"path": "bad.json", "payload": "!!!not-base64!!!"},
    {"path": "none.json", "payload": None},
]}})
assert "report.json" in dp and dp["report.json"] == b'{"a":1}'
assert "none.json" not in dp
assert dp_warns, "a part without a payload must be reported"
print("  decode_parts: OK (base64 decode, bad payload tolerated + warned)")

# ---------------------------------------------------------------------------
# as_bool - guards the risk tiering against Delta round-tripping bools to strings.
# ---------------------------------------------------------------------------
import pandas as pd  # noqa: E402

helper_src = None
for c in code_cells:
    s = "".join(c["source"])
    if "def as_bool" in s and "def write_run_table" in s:
        helper_src = s
        break
assert helper_src, "helper cell (write_run_table/as_bool) not found"

hmod = types.ModuleType("helpers")
hmod.__dict__.update({"pd": pd, "json": json, "Optional": Optional, "Dict": Dict,
                      "Any": Any, "List": List, "Tuple": Tuple, "Set": Set,
                      "cf": __import__("concurrent.futures", fromlist=["futures"]),
                      "contextvars": __import__("contextvars"),
                      "threading": __import__("threading"),
                      "datetime": __import__("datetime"), "time": __import__("time"),
                      "re": re,
                      "MAX_RETRIES": 6, "RETRY_BASE_SECONDS": 5,
                      "TABLE_PREFIX": "cv_audit", "RUN_ID": "TEST"})
# write_run_table/log reference notebook globals at call time only, so exec is safe.
exec(compile(helper_src, "<helpers>", "exec"), hmod.__dict__)

as_bool = hmod.as_bool

# real bools
assert list(as_bool(pd.Series([True, False, True]))) == [True, False, True]
# the dangerous case: bools round-tripped as strings. .astype(bool) would give
# [True, True, True] here because every non-empty string is truthy.
assert list(pd.Series(["True", "False"]).astype(bool)) == [True, True], \
    "sanity: naive astype(bool) really is broken on strings"
assert list(as_bool(pd.Series(["True", "False", "true", "FALSE"]))) == \
    [True, False, True, False]
# nulls and mixed representations
assert list(as_bool(pd.Series([True, None, False]))) == [True, False, False]
assert list(as_bool(pd.Series(["1", "0", "yes", "no"]))) == [True, False, True, False]
print("  as_bool: OK (real bools, string round-trip, nulls, 1/0/yes/no)")

# ---------------------------------------------------------------------------
# _prepare_for_spark - builds an explicit Delta schema. Spark schema inference
# fails outright on an all-null object column, so this must never regress.
# Stub pyspark.sql.types so the check runs without a Spark runtime.
# ---------------------------------------------------------------------------
_stub = types.ModuleType("pyspark.sql.types")


class _T:
    def __repr__(self):
        return type(self).__name__


for _name in ("BooleanType", "DoubleType", "LongType", "StringType"):
    setattr(_stub, _name, type(_name, (_T,), {}))


class _StructField:
    def __init__(self, name, dataType, nullable=True):
        self.name, self.dataType, self.nullable = name, dataType, nullable


class _StructType:
    def __init__(self, fields):
        self.fields = fields


_stub.StructField = _StructField
_stub.StructType = _StructType
sys.modules["pyspark.sql.types"] = _stub
sys.modules.setdefault("pyspark", types.ModuleType("pyspark"))
sys.modules.setdefault("pyspark.sql", types.ModuleType("pyspark.sql"))

frame = pd.DataFrame({
    "flag": [True, False],
    "count": [1, 2],
    "score": [1.5, 2.5],
    "name": ["a", "b"],
    # The column that used to break Spark inference: object dtype, entirely null.
    "pbiviz_version": [None, None],
    # Nested values must be serialised, not handed to Spark raw.
    "privileges": [["WebAccess"], {"k": "v"}],
})
out, schema = hmod._prepare_for_spark(frame)
types_by_name = {f.name: repr(f.dataType) for f in schema.fields}

assert types_by_name["flag"] == "BooleanType", types_by_name
assert types_by_name["count"] == "LongType", types_by_name
assert types_by_name["score"] == "DoubleType", types_by_name
assert types_by_name["pbiviz_version"] == "StringType", \
    "an all-null object column must still get an explicit StringType"
assert out["pbiviz_version"].tolist() == [None, None]
assert out["privileges"].tolist() == ['["WebAccess"]', '{"k": "v"}'], \
    "list/dict values must be JSON-serialised before reaching Spark"
assert types_by_name["run_id"] == "StringType" and set(out["run_id"]) == {"TEST"}
print("  _prepare_for_spark: OK (explicit schema, all-null column, nested serialisation)")

print("\nAll validation passed.")
