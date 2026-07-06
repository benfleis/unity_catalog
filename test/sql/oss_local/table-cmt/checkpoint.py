"""Driver for table-cmt/checkpoint.test (same-stem pairing).

Catalog-managed (CMT) counterpart of table-plain/checkpoint.py, retargeted at
duck.cmt (catalog-managed / coordinated commits) instead of duck.plain (plain
Delta). MANAGED tables nest their log under
<data_dir>/duck/cmt/__unitystorage/<uuid>/_delta_log/..., so the body's final
assertion scopes the glob to duck/cmt/ and COUNTs (it cannot filter on the table
name -- parse_path(file)[-3] is a UUID, not id_name).

Reuses the convention table `id_name` (not bespoke per-checkpoint tables) to keep the
seed surface minimal -- the body exercises unity_catalog_checkpoint_table's name
resolution forms against the one table, advancing a version between each. Injects the
container's host bind-mount dir as ${UC_TEST_DATA} so the body's glob resolves. Server
only (uc_server fixture, from oss_local/conftest.py).
"""

from driver import run_paired, step

from uc import uctl  # uc_server fixture comes from oss_local/conftest.py

SCHEMA = "cmt"
TABLE = "id_name"
COLUMNS = "id INT, name STRING"


def test_checkpoint(request, uc_server):
    with step(f"ensuring seed table duck.{SCHEMA}.{TABLE}"):
        uctl("drop", SCHEMA, TABLE, check=False)  # idempotent clean slate
        uctl("create", SCHEMA, TABLE, COLUMNS)
    try:
        run_paired(request, env={"UC_TEST_DATA": uc_server.data_dir})
    finally:
        with step(f"dropping seed table duck.{SCHEMA}.{TABLE}"):
            uctl("drop", SCHEMA, TABLE, check=False)
