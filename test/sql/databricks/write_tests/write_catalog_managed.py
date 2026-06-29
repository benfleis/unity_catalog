"""Driver for write_catalog_managed.test — carries the target @requires.

The @requires declares the one resource this test needs: an rw clone of the premade
source `simple_table`, in the cmt x managed cell (catalog-managed commit protocol,
UC-managed storage). The `resources` fixture provisions a `cmt__managed__<token>`
cell schema, clones `simple_table` into it BARE, injects UC_TEST_CATALOG/SCHEMA into
the body via run_paired(env=...), and tears the schema down afterward. The same
@requires + provisioner also drive `pytest --cli` for an interactive session.
"""

from driver import requires, run_paired


@requires(
    source="${UC_TEST_CATALOG}.source.simple_table",
    access="rw",
    commit="cmt",
    storage="managed",
)
def test_write_catalog_managed(request, resources):
    run_paired(request, env=resources.env)
