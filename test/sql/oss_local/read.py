"""Driver for read.test (same-stem pairing -> one test).

Declares the OSS UC server (uc_server, oss_local/conftest.py) and seeds two empty
catalog-managed tables; the body writes a known multi-row dataset, reads it back, and
checks catalog metadata -- the duck-model analogue of the upstream sample-data read test.
"""

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

SCHEMA = "managed"
TABLES = {
    "scores": "id INT, name STRING, score INT",
    "people": "name STRING, age INT, country STRING",
}


def test_read(request, uc_server):
    with step(f"ensuring seed tables duck.{SCHEMA}.{{{','.join(TABLES)}}}"):
        for table, cols in TABLES.items():
            uctl("drop", SCHEMA, table, check=False)  # idempotent clean slate
            uctl("create", SCHEMA, table, cols)
    try:
        run_paired(request)
    finally:
        with step(f"dropping seed tables duck.{SCHEMA}.{{{','.join(TABLES)}}}"):
            for table in TABLES:
                uctl("drop", SCHEMA, table, check=False)
