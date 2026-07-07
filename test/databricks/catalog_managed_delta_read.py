"""Driver for catalog_managed_delta_read.test -- read catalog-managed tables via Delta.

Paired read: @requires(access="ro") references a premade catalog-managed duckdb_testing
table (no DDL) and injects UC_TEST_CATALOG/SCHEMA; the body verifies max_catalog_version
is passed to the kernel for both staged- and promoted-commit tables.
"""

from driver import requires, run_paired


@requires(source="duckdb_testing.main.id_day_managed_spark", access="ro")
def test_catalog_managed_delta_read(request, resources):
    run_paired(request, env=resources.env)
