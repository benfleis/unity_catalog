"""Driver for catalog_managed.test (same-stem pairing -> one test).

Declares the running OSS UC server as a resource via the `uc_server` fixture
(test/py/uc/server.py) -- our first require/resource declaration. Resource scope is
*server only*: the catalog-managed table the body reads/writes is seeded imperatively
here via `uctl` (not yet modeled as its own resource).

Lifecycle: uc_server (session) stands up a fresh ducklabs server -> this driver seeds a
clean duck.managed.id_name -> run_paired runs the .test body through the unittest binary
-> table dropped -> (session end) server torn down.
"""

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

SCHEMA = "managed"
TABLE = "id_name"
COLUMNS = "id INT, name STRING"


def test_catalog_managed(request, uc_server):
    # server-only resource: give the body a clean, empty catalog-managed table.
    with step(f"ensuring seed table duck.{SCHEMA}.{TABLE}"):
        uctl("drop", SCHEMA, TABLE, check=False)  # idempotent clean slate
        uctl("create", SCHEMA, TABLE, COLUMNS)
    try:
        run_paired(request)
    finally:
        with step(f"dropping seed table duck.{SCHEMA}.{TABLE}"):
            uctl("drop", SCHEMA, TABLE, check=False)
