"""Driver for attach.test -- ATTACH/DETACH/USE mechanics for the unity_catalog extension.

Merged from the former basic.test + aliases.test (near-duplicates: same flow, differing
only in the secret TYPE spelling). One body now covers the attach/detach/use flow AND the
`TYPE UC` secret-type alias. Paired read: @requires(access="ro") references a premade
duckdb_testing table (no DDL) and injects UC_TEST_CATALOG/SCHEMA so the body attaches +
reads through env.
"""

from driver import requires, run_paired


@requires(source="duckdb_testing.main.simple_table", access="ro")
def test_attach(request, resources):
    run_paired(request, env=resources.env)
