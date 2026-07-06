"""Driver for attach.test (same-stem pairing -> one test).

Declares the running OSS UC server via the `uc_server` fixture (oss_local/conftest.py)
and seeds an empty catalog-managed duck.cmt.id_name; the body exercises
attach/detach/USE semantics and writes+reads a row through it.
"""

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

SCHEMA = "cmt"
TABLE = "id_name"
COLUMNS = "id INT, name STRING"


def test_attach(request, uc_server):
    with step(f"ensuring seed table duck.{SCHEMA}.{TABLE}"):
        uctl("drop", SCHEMA, TABLE, check=False)  # idempotent clean slate
        uctl("create", SCHEMA, TABLE, COLUMNS)
    try:
        run_paired(request)
    finally:
        with step(f"dropping seed table duck.{SCHEMA}.{TABLE}"):
            uctl("drop", SCHEMA, TABLE, check=False)
