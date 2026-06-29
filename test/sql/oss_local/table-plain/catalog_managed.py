"""Driver for table-plain/catalog_managed.test (same-stem pairing).

The plain half of the catalog-managed PROTOCOL contrast: seeds an EXTERNAL table
(duck.plain.id_name) and the body asserts reading does NOT call LoadTable and
writing does NOT call UpdateTable (vs table-cmt/catalog_managed, which asserts they
ARE called). The shared data round-trip is in oss_local/rw.test.

Server-only resource (uc_server fixture, from oss_local/conftest.py); the table is
seeded imperatively via `uctl`.
"""

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

SCHEMA = "plain"
TABLE = "id_name"
COLUMNS = "id INT, name STRING"


def test_catalog_managed(request, uc_server):
    with step(f"ensuring seed table duck.{SCHEMA}.{TABLE}"):
        uctl("drop", SCHEMA, TABLE, check=False)  # idempotent clean slate
        uctl("create", SCHEMA, TABLE, COLUMNS)
    try:
        run_paired(request)
    finally:
        with step(f"dropping seed table duck.{SCHEMA}.{TABLE}"):
            uctl("drop", SCHEMA, TABLE, check=False)
