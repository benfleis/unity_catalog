"""Driver for table-cmt/http_logs.test (same-stem pairing -> one test).

Seeds duck.cmt.id_name and lets the body exercise HTTP prefetch logging
(enable_logging('HTTP') + duckdb_logs_parsed('HTTP')). Ports the old
local_oss_unity_catalog/http_logs.test. Server-only resource (uc_server fixture, from
oss_local/conftest.py); the table is seeded imperatively via `uctl`.
"""

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

SCHEMA = "cmt"
TABLE = "id_name"
COLUMNS = "id INT, name STRING"


def test_http_logs(request, uc_server):
    with step(f"ensuring seed table duck.{SCHEMA}.{TABLE}"):
        uctl("drop", SCHEMA, TABLE, check=False)  # idempotent clean slate
        uctl("create", SCHEMA, TABLE, COLUMNS)
    try:
        run_paired(request)
    finally:
        with step(f"dropping seed table duck.{SCHEMA}.{TABLE}"):
            uctl("drop", SCHEMA, TABLE, check=False)
