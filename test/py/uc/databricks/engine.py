"""Databricks provisioning engine — concrete impl of driver's Provisioner protocol.

Turns a list[Requirement] into Databricks fixtures + a duckdb init script.

CELL GRAMMAR (pinned): an `rw` requirement is isolated in a schema named
    <commit>__<storage>__<token>      e.g.  cmt__managed__brave_otter
where <token> is the SQL-safe per-invocation id. Tables live BARE in that schema,
so a body references `simple_table` (not `simple_table_catalog_managed`) once the
default schema points at the cell.

THE 2x2 (commit x storage), per requirement:
                managed (UC-managed, no LOCATION)   external (explicit LOCATION)
  cmt   (catalog-managed commit protocol)  catalog-managed props, no LOCATION   catalog-managed props + LOCATION
  plain (no catalog-managed props)         no props, no LOCATION (UC-managed)    no props + LOCATION

`ro` requirements reference the source directly (no DDL); their binding is the
source FQN.

ASSUMPTIONS (cannot be verified here — no Databricks; see REPORT):
  - cmt + external (catalog-managed props WITH an explicit LOCATION) is accepted
    by Databricks. The existing generator only ever emits cmt-without-LOCATION, so
    this combination is UNVERIFIED. cmt + managed (the milestone target) mirrors
    the proven generator path exactly.
  - DROP SCHEMA ... CASCADE drops cloned managed tables and their storage.
"""

import os
import sys
from dataclasses import dataclass, field

import pytest

from driver import find_duckdb, step  # find_duckdb: resolve tools from one build

# The generator + cleaner live in scripts/databricks_data_gen/. The conftest puts
# `scripts` on sys.path; make this importable directly too so the engine is
# self-sufficient when loaded by the --cli flow.
# databricks -> uc -> py -> test -> <repo root>  (4 dirs up from this file's dir)
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
_GEN_DIR = os.path.join(_REPO_ROOT, "scripts", "databricks_data_gen")
if _GEN_DIR not in sys.path:
    sys.path.insert(0, _GEN_DIR)

# Reuse the generator's table_props / build_create_sql / S3 layout verbatim so
# emitted DDL matches the proven bulk path; copy_one_table is the single-table
# provision path. Heavy deps (databricks.connect) are imported lazily inside those
# functions' get_spark_session(), so importing the module is cheap and safe in
# environments without databricks-connect (e.g. --provision-dry-run).
import generate_databricks_test_data as gen  # noqa: E402

CATALOG_MANAGED = gen.CATALOG_MANAGED
S3_BUCKET = gen.S3_BUCKET


@dataclass
class TableBinding:
    """How a body should reference one provisioned requirement."""

    requirement_name: str  # bare name a body uses
    fqn: str  # fully-qualified physical table the name resolves to
    access: str  # ro | rw


@dataclass
class Bindings:
    """Result of provision(): everything --cli / a run path needs.

    catalog       : the Databricks catalog (for ATTACH).
    default_schema: the cell schema to ATTACH with as DEFAULT_SCHEMA (mono-cell).
                    For an rw spec this is its cell; for an all-ro plan it falls
                    back to the source schema of the first spec.
    tables        : per-requirement TableBinding (bodies reference these bare).
    cell_schemas  : the set of cell schemas created (for teardown / display).
    env           : env mapping a future run path would inject into the body.
    plan          : human-readable provision commands (the dry-run "plan").
    token         : the provision token (cell-schema suffix).
    """

    catalog: str
    default_schema: str
    token: str
    tables: list = field(default_factory=list)
    cell_schemas: list = field(default_factory=list)
    env: dict = field(default_factory=dict)
    plan: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Env expansion + naming
# ---------------------------------------------------------------------------


def _expand(s: str) -> str:
    """Expand ${VAR} from the environment; raise on an unset referenced var."""
    out = os.path.expandvars(s)
    if "${" in out:
        # Clean UsageError (not a bare KeyError → INTERNALERROR) so a missing var
        # reads as a user fix, not a harness crash.
        raise pytest.UsageError(
            f"--cli: unresolved environment variable in @requires source {s!r} "
            f"(expanded to {out!r}). Set it in the environment "
            "(e.g. UC_TEST_CATALOG, or run under run_databricks_env)."
        )
    return out


def _default_catalog_env():
    """Default the neutral UC_TEST_CATALOG so `--cli` works without a wrapper.

    Honors an explicit UC_TEST_CATALOG (set by you, run_databricks_env, or CI);
    else falls back to the standard write-test catalog. Override by exporting
    UC_TEST_CATALOG.
    """
    os.environ.setdefault("UC_TEST_CATALOG", "duckdb_write_testing")


def cell_schema_name(commit: str, storage: str, token: str) -> str:
    """The pinned cell grammar: <commit>__<storage>__<token>."""
    return f"{commit}__{storage}__{token}"


def _split_source(source_fqn: str):
    """catalog.schema.table -> (catalog, schema, table)."""
    parts = source_fqn.split(".")
    if len(parts) != 3:
        raise ValueError(
            f"@requires source must be catalog.schema.table, got {source_fqn!r}"
        )
    return parts[0], parts[1], parts[2]


# ---------------------------------------------------------------------------
# Per-spec DDL (full 2x2). cmt+managed reuses the generator's build_create_sql
# (proven path); the other cells build DDL locally for LOCATION/props control.
# ---------------------------------------------------------------------------


def _provision_command(catalog, cell, source_fqn, dest_table, commit, storage):
    """The concrete shell command that provisions one rw table (for the plan).

    cmt+managed maps onto the new `copy-one --catalog-managed` CLI path; the other
    cells have no single CLI flag yet, so we annotate them.
    """
    base = (
        f"python scripts/databricks_data_gen/generate_databricks_test_data.py "
        f"copy-one {source_fqn} {catalog}.{cell} --dest-table {dest_table}"
    )
    if commit == "cmt" and storage == "managed":
        return base + " --catalog-managed"
    return base + f"   # (+ commit={commit} storage={storage}: engine-built DDL)"


def _build_table_sql(catalog, cell, source_fqn, dest_table, commit, storage):
    """CREATE OR REPLACE TABLE SQL for one rw table covering the full 2x2."""
    full = f"{catalog}.{cell}.{dest_table}"
    location = f"s3://{S3_BUCKET}/{catalog}/{cell}/{dest_table}"

    if commit == "cmt" and storage == "managed":
        # Proven generator path: catalog-managed props, no LOCATION.
        return gen.build_create_sql(full, location, source_fqn, [CATALOG_MANAGED])

    # General path: choose props + LOCATION explicitly (the generator's builder
    # couples no-LOCATION to catalog-managed, so build it here for the other cells).
    props = {}
    if commit == "cmt":
        props.update(gen.table_props[CATALOG_MANAGED])
    location_clause = (
        "" if storage == "managed" else f"\n            LOCATION '{location}'"
    )
    if props:
        items = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        tblproperties = f"\n            TBLPROPERTIES ({items})"
    else:
        tblproperties = ""
    return (
        f"CREATE OR REPLACE TABLE {full}"
        f"{location_clause}"
        f"{tblproperties}\n"
        f"            AS\n"
        f"            SELECT * FROM {source_fqn}"
    )


# ---------------------------------------------------------------------------
# Credentials (the dbx Provider's connection creds)
# ---------------------------------------------------------------------------

_CRED_VARS = ("DATABRICKS_TOKEN", "DATABRICKS_ENDPOINT", "DATABRICKS_REGION")


def _require_creds():
    """Raise unless DATABRICKS_{TOKEN,ENDPOINT,REGION} are already in the environment.

    Creds are a RUN-scoped resource: the launching shell front-loads them
    (`scripts/run_databricks_env pytest …`, or `op run -- pytest …`); pytest does NOT
    fetch them. Fetching from inside pytest pops 1Password once per xdist worker
    (separate processes, per-process state) and has no clean once-before-fork home —
    see test/py/driver/PYTEST.md ("global, once, before any worker"). So we only
    verify here; the conftest hook turns this UsageError into a graceful skip.
    """
    missing = [k for k in _CRED_VARS if not os.environ.get(k)]
    if missing:
        raise pytest.UsageError(
            f"Databricks creds not set: {', '.join(missing)}. Front-load them in the "
            "launching shell, e.g. `scripts/run_databricks_env pytest …` (or "
            "`op run -- pytest …`)."
        )


def ensure_env(*, dry_run=False):
    """Make the Databricks env ready for a test or a provision step.

    Catalog default (cheap, always) + a creds CHECK (skipped on dry_run so
    --co / --provision-dry-run never need creds). The creds themselves come from the
    launching shell, not from pytest — see _require_creds / PYTEST.md. Safe to call
    repeatedly. The conftest hook calls this so the run path (run_paired) gets the
    catalog default + the creds check; --cli calls it too.
    """
    _default_catalog_env()
    if not dry_run:
        _require_creds()


# ---------------------------------------------------------------------------
# Provisioner protocol impl
# ---------------------------------------------------------------------------


class DatabricksProvisioner:
    """Concrete Provisioner (driver/provision.py) for Databricks Unity Catalog."""

    def __init__(self, config=None):
        # config lets make_init resolve the duckdb build dir from the SAME build the
        # driver runs the unittest binary from (--build / $BUILD_DIR / --duckdb-bin),
        # instead of a hardcoded build/release. See uc.oss.OssProvisioner.
        self._config = config

    def provision(self, specs, token, *, dry_run=False, params=None) -> Bindings:
        """Provision fixtures for `specs` under `token`. See module docstring.

        dry_run=True resolves + prints the plan and builds the Bindings (schema
        names, env, would-be commands/DDL) but executes NO DDL.
        """
        if not specs:
            # The framework allows --repl on a test with no @requires (bare REPL). The
            # databricks backend has no minimal-REPL path wired yet (it needs a cell to
            # ATTACH), so fail with a clean message rather than a bare ValueError.
            raise pytest.UsageError(
                "--repl on a databricks test needs @requires (no minimal-REPL path "
                "wired for databricks). Add @requires, or pick an OSS test for a bare REPL."
            )

        ensure_env(dry_run=dry_run)  # UC_TEST_CATALOG default + creds check (skipped on dry_run)

        # Catalog/default come from the first spec's (expanded) source — all specs
        # in one test are expected to share a catalog (mono-cell binding).
        first_cat, first_schema, _ = _split_source(_expand(specs[0].source))
        bindings = Bindings(catalog=first_cat, default_schema=first_schema, token=token)

        spark = None
        if not dry_run:
            with step("connecting to Databricks (serverless)"):
                spark = gen.get_spark_session()  # creds ensured via ensure_env() above

        cell_for_default = None
        for spec in specs:
            source_fqn = _expand(spec.source)
            cat, schema, table = _split_source(source_fqn)
            bare = spec.resolved_name()

            if spec.access == "ro":
                bindings.tables.append(TableBinding(bare, source_fqn, "ro"))
                bindings.plan.append(f"[ro] reference {source_fqn} as {bare} (no DDL)")
                continue

            # rw: cell-encoded isolation schema + single-table clone with cell props.
            cell = cell_schema_name(spec.commit, spec.storage, token)
            if cell not in bindings.cell_schemas:
                bindings.cell_schemas.append(cell)
            if cell_for_default is None:
                cell_for_default = cell

            full = f"{cat}.{cell}.{bare}"
            cmd = _provision_command(
                cat, cell, source_fqn, bare, spec.commit, spec.storage
            )
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {cat}.{cell}"
            create_table_sql = _build_table_sql(
                cat, cell, source_fqn, bare, spec.commit, spec.storage
            )

            bindings.tables.append(TableBinding(bare, full, "rw"))
            bindings.plan.append(cmd)
            bindings.plan.append(f"    {create_schema_sql};")
            bindings.plan.append(f"    {create_table_sql};")

            if not dry_run:
                with step(
                    f"clone {source_fqn} -> {full} ({spec.commit}/{spec.storage})"
                ):
                    spark.sql(create_schema_sql)
                    spark.sql(create_table_sql)

        # Mono-cell default schema: the (first) rw cell if any, else source schema.
        if cell_for_default is not None:
            bindings.default_schema = cell_for_default

        # Env a future run path would inject (body reads these; see the .test).
        bindings.env = {
            "UC_TEST_CATALOG": bindings.catalog,
            "UC_TEST_SCHEMA": bindings.default_schema,
        }

        if dry_run:
            print("provision plan (NO DDL executed):")
            for line in bindings.plan:
                print(f"  {line}")
            print(f"cell schema(s): {bindings.cell_schemas or '(none — all ro)'}")
            print(f"DEFAULT_SCHEMA: {bindings.default_schema}")

        return bindings

    def teardown(self, token, bindings=None) -> None:
        """DROP each cell schema CASCADE. Reuses clean_test_data.drop_tables."""
        import clean_test_data

        cell_schemas = list(bindings.cell_schemas) if bindings else []
        if not cell_schemas:
            print(f"teardown: nothing to drop for token={token}")
            return
        catalog = (
            bindings.catalog
            if bindings
            else os.environ.get("UC_TEST_CATALOG")
        )
        for cell in cell_schemas:
            with step(f"drop cell schema {catalog}.{cell} CASCADE"):
                clean_test_data.drop_tables(f"{catalog}.{cell}", dry_run=False)

    def make_init(self, bindings: Bindings, *, redact: bool = False) -> str:
        """duckdb init SQL for `duckdb -unsigned -init`.

        Mirrors write_catalog_managed.test: LOAD local extensions, CREATE SECRET,
        ATTACH the catalog with the cell as DEFAULT_SCHEMA, USE it. Extension paths come
        from the SAME build as the resolved tools (find_duckdb); redact=True (used by
        --provision-dry-run, which prints this without launching) falls back to the
        env/default so it needs no built binary, and masks the token.
        """
        if redact:
            # dry-run print: must not require a built binary.
            build_dir = os.environ.get("BUILD_DIR", os.path.join(_REPO_ROOT, "build", "release"))
        else:
            wd = getattr(self._config, "sqllogic_working_dir", None) or os.getcwd()
            build_dir = os.path.dirname(find_duckdb(self._config, wd))

        def ext(name):
            return os.path.join(
                build_dir, "extension", name, f"{name}.duckdb_extension"
            )

        token = (
            "***REDACTED***"
            if redact
            else os.environ.get("DATABRICKS_TOKEN", "${DATABRICKS_TOKEN}")
        )
        endpoint = os.environ.get("DATABRICKS_ENDPOINT", "${DATABRICKS_ENDPOINT}")
        region = os.environ.get("DATABRICKS_REGION", "${DATABRICKS_REGION}")

        table_lines = "\n".join(
            f".print '  {t.requirement_name}  ->  {t.fqn}  ({t.access})'"
            for t in bindings.tables
        )

        return f"""-- Auto-generated by uc.databricks.engine.make_init for `duckdb -unsigned -init`.
-- -unsigned (launch flag) is required to LOAD locally-built extensions.
LOAD '{ext("parquet")}';
LOAD '{ext("httpfs")}';
LOAD '{ext("delta")}';
LOAD '{ext("unity_catalog")}';

CREATE SECRET (
    TYPE UC,
    TOKEN '{token}',
    ENDPOINT '{endpoint}',
    AWS_REGION '{region}'
);

ATTACH '{bindings.catalog}' AS unity (TYPE unity_catalog, DEFAULT_SCHEMA '{bindings.default_schema}');
USE unity;

.print ''
.print '== pytest --cli ready =='
.print 'Attached: unity -> {bindings.catalog}, DEFAULT_SCHEMA {bindings.default_schema}'
.print 'Provisioned (reference tables BARE; the cell IS the default schema):'
{table_lines}
.print ''
"""
