"""Regression: a UC /tables ColumnInfo with null type_precision must parse (coerce to 0), not throw.

Delta-Spark-created tables report type_precision/type_scale as JSON null in the UC 0.5 /tables
response; ParseColumnDefinition (src/uc_api.cpp) parsed them with fail_on_missing=true and threw
`IO Error: Invalid field found while parsing field: type_precision` on ANY listing/read -- before
touching Delta data. `uctl`/`bin/uc` always write 0, so they can't reproduce; we register a
metadata-only table with a null-precision column directly via the UC REST API (the Spark shape),
then the body attaches + lists it. Pre-fix: SHOW ALL TABLES raises. Post-fix: lists it.

DRAFT -- verify against a live UC 0.5 server before relying on it:
  (1) the POST /tables body / required fields (UC 0.5 may want more than the below),
  (2) that OMITTING type_precision stores NULL (as Spark does) rather than the server defaulting 0.
If the server coerces null->0, this won't reproduce and we fall back to a mock /tables response
(a small http.server returning canned JSON) or a Spark-written table.
"""

import json
import urllib.error
import urllib.request

try:
    from driver import run_paired
except ImportError:
    import pytest

    # Parked: this test rode onto main/v1.5 ahead of the duckdb-pytest-driver. While the driver
    # is absent there's nothing to run, so skip quietly -- `make test` (the bare unittest binary)
    # ignores .py files anyway, and an unrelated `pytest` run shouldn't explode just because the
    # driver hasn't landed yet.
    pytest.skip(
        "duckdb-pytest-driver not present yet; type_precision regression parked until it lands.",
        allow_module_level=True,
    )
else:
    # POISON PILL -- deliberately loud, do NOT soften. Fires the moment the driver DOES land,
    # which is the signal to come back and finish this off: confirm the oss_local uc_server
    # fixture boots, then delete this whole try/except and keep just the bare import above.
    # A hard fail at exactly that moment forces the fix-up instead of letting the test rot
    # silently skipped once its one dependency is finally available.
    raise RuntimeError(
        "POISON PILL: duckdb-pytest-driver is present -- finish wiring type_precision.py "
        "(confirm the oss_local uc_server fixture runs, drop this guard down to the bare "
        "`from driver import run_paired`), then delete this pill. "
        "See the fix-type-precision-null landing."
    )


def _register_null_precision_table(endpoint, *, catalog="duck", schema="cmt", name="spark_like"):
    def _col(pos, col_name, type_name, type_text, spark_type):
        # UC 0.5 validates type_json -- it must be the full Spark field descriptor, not "{}".
        # type_precision/type_scale OMITTED -> stored null (the Spark shape that triggers the bug).
        return {
            "name": col_name,
            "type_name": type_name,
            "type_text": type_text,
            "type_json": json.dumps({"name": col_name, "type": spark_type, "nullable": True, "metadata": {}}),
            "position": pos,
            "nullable": True,
        }

    body = {
        "name": name,
        "catalog_name": catalog,
        "schema_name": schema,
        "table_type": "EXTERNAL",
        "data_source_format": "DELTA",
        # Listing (SHOW ALL TABLES) parses ColumnInfo but does NOT read Delta files, so a dummy
        # location suffices to hit the parse bug.
        "storage_location": "file:///tmp/uc-type-precision-repro",
        "columns": [
            _col(0, "id", "LONG", "bigint", "long"),
            _col(1, "name", "STRING", "string", "string"),
        ],
    }
    req = urllib.request.Request(
        f"{endpoint.rstrip('/')}/api/2.1/unity-catalog/tables",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        # Surface UC's validation detail (urllib buries it in the response body) so a 400 names
        # the offending field instead of a bare "Bad Request".
        raise AssertionError(f"POST /tables -> {e.code} {e.reason}: {e.read().decode()}") from e


def test_type_precision_null(request, uc_server):
    """Register the null-precision table, then run the paired .test (attach + list)."""
    _register_null_precision_table(uc_server.endpoint)
    run_paired(request)
