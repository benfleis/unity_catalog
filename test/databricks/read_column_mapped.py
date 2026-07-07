"""Driver for read_column_mapped.test -- read-only reads of column-mapped Delta tables.

Paired read: @requires(access="ro") references a premade duckdb_testing table (no DDL)
and injects UC_TEST_CATALOG/SCHEMA; the body attaches + reads the column-mapped /
schema-evolved tables through env.
"""

from driver import requires, run_paired


@requires(source="duckdb_testing.main.evolution_column_change", access="ro")
def test_read_column_mapped(request, resources):
    run_paired(request, env=resources.env)
