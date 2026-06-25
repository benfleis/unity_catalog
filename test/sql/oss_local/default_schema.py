"""Driver for default_schema.test (negative test, same-stem pairing).

OSS UC exposes no default namespace, so attaching a UC catalog WITHOUT DEFAULT_SCHEMA
leaves auto-detection unable to resolve one; accessing a table through the catalog's
implicit default schema then errors. Kept until default-schema auto-detection is
supported (then flip to a positive no-DEFAULT_SCHEMA case). Seeds duck.managed.id_name
so the failure is unambiguously the default-schema resolution, not a missing table.
"""

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

SCHEMA = "managed"
TABLE = "id_name"
COLUMNS = "id INT, name STRING"


def test_default_schema(request, uc_server):
    with step(f"ensuring seed table duck.{SCHEMA}.{TABLE}"):
        uctl("drop", SCHEMA, TABLE, check=False)  # idempotent clean slate
        uctl("create", SCHEMA, TABLE, COLUMNS)
    try:
        run_paired(request)
    finally:
        with step(f"dropping seed table duck.{SCHEMA}.{TABLE}"):
            uctl("drop", SCHEMA, TABLE, check=False)
