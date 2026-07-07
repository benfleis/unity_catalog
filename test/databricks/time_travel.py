"""Driver for time_travel.test -- read-only Delta time travel against a premade table.

Paired read: @requires(access="ro") references the preloaded duckdb_testing table (no
DDL) and injects UC_TEST_CATALOG/SCHEMA, so the body attaches + reads AT (VERSION => n)
entirely through env. See test/databricks/README.md for the preloaded data.
"""

from driver import requires, run_paired


@requires(source="duckdb_testing.main.evolution_simple_id_mode", access="ro")
def test_time_travel(request, resources):
    run_paired(request, env=resources.env)
