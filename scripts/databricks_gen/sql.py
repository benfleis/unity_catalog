"""Atomic Databricks table management over the SQL Statement Execution API.

No Spark, no pandas: every operation is a SINGLE SQL statement run on a serverless SQL
warehouse via the databricks-sdk (a light HTTP client -- works on any Python, unlike
databricks-connect/pyspark). Small atomic functions, composable into whatever seeding or
teardown a test or tool needs.

Config (env):
  DATABRICKS_ENDPOINT    -- workspace host (the SDK `host`).
  DATABRICKS_TOKEN       -- PAT.
  DATABRICKS_WAREHOUSE_ID-- the SQL warehouse the statements run on.

`build_*` are PURE (return the statement string); the same-named verbs execute it -- so a
caller that needs a dry-run plan uses the builder, and a caller that runs it uses the verb.
The databricks-sdk import is deferred into `_client`, so importing this module is cheap and
safe where the SDK isn't installed (e.g. offline test collection).
"""

import os
import time

_HOST_ENV = "DATABRICKS_ENDPOINT"
_TOKEN_ENV = "DATABRICKS_TOKEN"
_WAREHOUSE_ENV = "DATABRICKS_WAREHOUSE_ID"

# Delta property preset for catalog-managed tables (the UC 2x2's `cmt` half). Databricks
# domain knowledge, so it lives here -- both the provisioner and the CLI apply it.
CATALOG_MANAGED_PROPS = {
    "delta.feature.catalogManaged": "supported",
    "delta.enableRowTracking": "false",
}

# DuckDB logical types -> Databricks (Spark) SQL types, for instantiating a duckdb-
# canonicalized Fixture as a Databricks table (used by the CLI's `create --source` and the
# provisioner's fixture seeding, via driver.fixtures.map_columns).
DATABRICKS_TYPE_MAP = {
    "INTEGER": "INT",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",
    "VARCHAR": "STRING",
    "DOUBLE": "DOUBLE",
    "FLOAT": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "DECIMAL": "DECIMAL",
}


def _client():
    """A databricks-sdk WorkspaceClient from DATABRICKS_ENDPOINT/TOKEN (no ~/.databrickscfg needed)."""
    from databricks.sdk import WorkspaceClient

    host, token = os.environ.get(_HOST_ENV), os.environ.get(_TOKEN_ENV)
    if not (host and token):
        raise ValueError(
            f"{_HOST_ENV} and {_TOKEN_ENV} must be set (workspace host + PAT)."
        )
    return WorkspaceClient(host=host, token=token)


def execute(sql, *, catalog=None, schema=None):
    """Run ONE SQL statement on the warehouse; return rows as list[tuple] (``[]`` for DDL).

    Blocks until the statement finishes: an inline wait, then a poll loop for a long CTAS.
    Raises RuntimeError on a failed statement, ValueError if the warehouse/creds env is unset.
    """
    from databricks.sdk.service.sql import StatementState

    warehouse = os.environ.get(_WAREHOUSE_ENV)
    if not warehouse:
        raise ValueError(
            f"{_WAREHOUSE_ENV} is not set (the SQL warehouse the statements run on)."
        )
    w = _client()
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse,
        statement=sql,
        catalog=catalog,
        schema=schema,
        wait_timeout="30s",
    )
    while resp.status and resp.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        time.sleep(1)
        resp = w.statement_execution.get_statement(resp.statement_id)
    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        detail = (
            resp.status.error.message if (resp.status and resp.status.error) else state
        )
        raise RuntimeError(f"Databricks SQL failed ({state}): {detail}\n  {sql}")
    rows = resp.result.data_array if resp.result else None
    return [tuple(r) for r in (rows or [])]


def sql_literal(v):
    """Render a Python scalar as a Databricks SQL literal (for INSERT ... VALUES)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


# --------------------------------------------------------------------------- #
# Pure statement builders (no I/O) — the dry-run plan uses these.
# --------------------------------------------------------------------------- #


def _tblproperties(properties):
    if not properties:
        return ""
    items = ", ".join(f"'{k}' = '{v}'" for k, v in properties.items())
    return f"\n  TBLPROPERTIES ({items})"


def build_create_table(
    fqn, columns=None, *, as_select=None, properties=None, location=None, replace=True
):
    """Build a CREATE [OR REPLACE] TABLE statement (exactly one of `columns` or `as_select`)."""
    if bool(columns) == bool(as_select):
        raise ValueError(
            "build_create_table needs exactly one of `columns` or `as_select`"
        )
    sql = f"{'CREATE OR REPLACE TABLE' if replace else 'CREATE TABLE IF NOT EXISTS'} {fqn}"
    if columns:
        sql += f" ({columns})"
    if location:
        sql += f"\n  LOCATION '{location}'"
    sql += _tblproperties(properties)
    if as_select:
        sql += f"\n  AS {as_select}"
    return sql


def build_insert(fqn, rows, *, columns=None):
    """Build an INSERT INTO ... VALUES statement from `rows` (list of python-scalar tuples)."""
    cols = f" ({columns})" if columns else ""
    values = ", ".join(
        "(" + ", ".join(sql_literal(v) for v in row) + ")" for row in rows
    )
    return f"INSERT INTO {fqn}{cols} VALUES {values}"


# --------------------------------------------------------------------------- #
# Atomic verbs (build + execute).
# --------------------------------------------------------------------------- #


def create_schema(fqn):
    execute(f"CREATE SCHEMA IF NOT EXISTS {fqn}")


def drop_schema(fqn, *, cascade=True):
    execute(f"DROP SCHEMA IF EXISTS {fqn}" + (" CASCADE" if cascade else ""))


def create_table(fqn, columns=None, **kwargs):
    execute(build_create_table(fqn, columns, **kwargs))


def drop_table(fqn):
    execute(f"DROP TABLE IF EXISTS {fqn}")


def alter_table(fqn, action):
    """ALTER TABLE fqn <action> -- e.g. alter_table(t, \"SET TBLPROPERTIES ('k' = 'v')\")."""
    execute(f"ALTER TABLE {fqn} {action}")


def insert(fqn, rows, *, columns=None):
    """INSERT INTO fqn VALUES ... -- no-op on empty `rows`."""
    if not rows:
        return
    execute(build_insert(fqn, rows, columns=columns))


def select(sql):
    """Run a SELECT (or any result-returning statement); return rows as list[tuple]."""
    return execute(sql)


# --------------------------------------------------------------------------- #
# Multi-statement SQL files (Databricks Delta artifact defs) — the `from-sql` core,
# reusable by the CLI and the provisioner.
# --------------------------------------------------------------------------- #


def split_statements(sql):
    """Split multi-statement SQL on TOP-LEVEL `;` only.

    A `;` inside a 'single-quoted' string is not a boundary (`''` is an escaped quote).
    Line comments are stripped by `run_sql_file`. The SQL API has no multi-statement call,
    so this client-side split is unavoidable; being quote-aware is what keeps it honest.
    """
    out, buf, in_str = [], [], False
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "'":
            buf.append(c)
            if (
                in_str and i + 1 < n and sql[i + 1] == "'"
            ):  # '' -> escaped quote, stay in string
                buf.append("'")
                i += 2
                continue
            in_str = not in_str
        elif c == ";" and not in_str:
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
        else:
            buf.append(c)
        i += 1
    stmt = "".join(buf).strip()
    if stmt:
        out.append(stmt)
    return out


def run_sql_file(path, *, table, location=None, dry_run=False):
    """Run a raw Databricks SQL def file (Delta artifacts: evolution / column mapping /
    catalog-managed) verbatim -- for tables that aren't a portable fixture shape.

    Strips full-line `--` comments, substitutes `{table_name}` (and `{location}` if the file
    uses it -- required then), splits quote-aware on `;`, and executes each statement.
    Returns the statement list (executed unless dry_run). A `location` passed for a file that
    has no `{location}` is ignored (lenient, for programmatic callers).
    """
    with open(path) as f:
        text = f.read()
    text = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("--"))
    text = text.replace("{table_name}", table)
    if "{location}" in text:
        if not location:
            raise ValueError(f"{path} uses {{location}} but no location was provided")
        text = text.replace("{location}", location)
    statements = split_statements(text)
    if not dry_run:
        for stmt in statements:
            execute(stmt)
    return statements
