"""Shared read/write driver for rw.test (same-stem pairing).

The data behavior (write -> read-back -> metadata) is IDENTICAL for catalog-managed
(duck.cmt) and plain (duck.plain) tables -- that's the invariant. So ONE
parametrized driver + ONE body (rw.test, keyed on ${UC_TEST_SCHEMA}) covers both,
instead of duplicating the round-trip per table type. The table-type-specific protocol
assertions (does reading call LoadTable / writing call UpdateTable?) are isolated in
table-cmt/ and table-plain/.

Server-only resource (uc_server fixture, from oss_local/conftest.py); the table is
seeded imperatively via `uctl` in the right schema per parameter.
"""

import pytest

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

TABLE = "id_name"
COLUMNS = "id INT, name STRING"


@pytest.mark.parametrize("schema", ["cmt", "plain"])
def test_rw(request, uc_server, schema):
    with step(f"ensuring seed table duck.{schema}.{TABLE}"):
        uctl("drop", schema, TABLE, check=False)  # idempotent clean slate
        uctl("create", schema, TABLE, COLUMNS)
    try:
        run_paired(request, env={"UC_TEST_SCHEMA": schema})
    finally:
        with step(f"dropping seed table duck.{schema}.{TABLE}"):
            uctl("drop", schema, TABLE, check=False)
