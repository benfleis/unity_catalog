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
import subprocess
import sys
from dataclasses import dataclass, field

import pytest

from driver import Fixture, find_duckdb, step  # find_duckdb: resolve tools from one build
from duckdb_pytest_driver.fixtures import canonicalize, load_fixture, map_columns, resolve_seed

# The databricks_gen library (atomic SQL primitives over the SDK) lives in scripts/.
# databricks -> uc -> py -> test -> <repo root>  (4 dirs up from this file's dir); put
# `scripts` on sys.path so `import databricks_gen` resolves even when the --cli flow loads
# this engine without the root conftest. databricks_gen imports the SDK lazily, so this
# stays cheap -- test COLLECTION never pulls in databricks-sdk.
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
_SCRIPTS_DIR = os.path.join(_REPO_ROOT, "scripts")
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

from databricks_gen import (  # noqa: E402  (needs _SCRIPTS_DIR on path)
    CATALOG_MANAGED_PROPS,
    DATABRICKS_TYPE_MAP,
    create_table,
    drop_schema,
    execute,
    insert,
    run_sql_file,
)

# Account config (S3 bucket, catalogs) -- env-overridable, in one place (see config.py).
from . import config  # noqa: E402

# Two definition sources the provisioner instantiates from:
#   _FIXTURES  -- portable driver fixtures (id_name) for the write/attach tests: create + insert.
#   _DATA_DIR  -- Databricks Delta-artifact defs (evolution / column-mapping / catalog-managed)
#                 for the RO read tests: run verbatim via run_sql_file (`<table>.sql`, plus an
#                 optional `<table>.insert.sql` for the DuckDB UC write path).
_FIXTURES = os.path.join(_REPO_ROOT, "test", "fixtures")
_DATA_DIR = os.path.join(_REPO_ROOT, "test", "databricks", "data")


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
    os.environ.setdefault("UC_TEST_CATALOG", config.WRITE_CATALOG)


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


# ---------------------------------------------------------------------------
# Credentials (the Databricks provider's connection creds)
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
        # RO variant tables are provisioned from their def ONCE per session (per worker under
        # xdist); this guards re-provisioning. Mirrors uc.oss.OssProvisioner._shared_ro.
        self._shared_ro = set()

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

        # `access` decides POLICY (namespace + lifecycle), NOT how to instantiate:
        #   rw -> an isolated per-test cell in the write catalog, dropped on teardown;
        #   ro -> the shared table the source FQN names, instantiated once per session.
        # The instantiation itself (fixture vs Databricks def) is _instantiate's job.
        write_catalog = os.environ["UC_TEST_CATALOG"]  # ensure_env defaulted it (config.WRITE_CATALOG)
        bindings = Bindings(catalog=write_catalog, default_schema="main", token=token)

        cell_for_default = None
        for spec in specs:
            bare = spec.resolved_name()

            if spec.access == "rw":
                cell = cell_schema_name(spec.property("commit"), spec.property("storage"), token)
                target = f"{write_catalog}.{cell}.{bare}"
                if cell not in bindings.cell_schemas:
                    bindings.cell_schemas.append(cell)
                    bindings.plan.append(f"CREATE SCHEMA IF NOT EXISTS {write_catalog}.{cell};")
                    if not dry_run:
                        execute(f"CREATE SCHEMA IF NOT EXISTS {write_catalog}.{cell}")
                if cell_for_default is None:
                    cell_for_default = cell
                self._instantiate(spec, target, dry_run, bindings)
                bindings.tables.append(TableBinding(bare, target, "rw"))
            else:
                # ro sources are FQN strings naming a shared, premade/def table.
                target = _expand(spec.source)
                bindings.catalog, bindings.default_schema = _split_source(target)[:2]
                if target in self._shared_ro:
                    bindings.plan.append(f"[ro] {target} already provisioned this session")
                else:
                    self._instantiate(spec, target, dry_run, bindings)
                    if not dry_run:
                        self._shared_ro.add(target)
                bindings.tables.append(TableBinding(bare, target, "ro"))

        # Mono-cell default schema: the (first) rw cell if any, else the ro source schema.
        if cell_for_default is not None:
            bindings.default_schema = cell_for_default

        # Env a run path injects (body reads these; see the .test).
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

    def _instantiate(self, spec, target, dry_run, bindings):
        """Instantiate `target` from `spec`'s definition. Dispatches on definition TYPE (the
        instantiation method) -- independent of `access`, which set target + lifecycle above.
        """
        if isinstance(spec.source, Fixture):
            self._instantiate_fixture(spec, target, dry_run, bindings)
        else:
            self._instantiate_def(target, dry_run, bindings)

    def _instantiate_fixture(self, spec, target, dry_run, bindings):
        """Seed a portable Fixture into `target` with the 2x2 props/location (create + insert).

        The one path for write/attach tests. Dry-run only NAMES the fixture + cell (no duckdb
        canonicalization, no I/O -- mirrors the OSS provisioner); the real path canonicalizes
        -> columns + seed, then create_table (with the cell's commit/storage props/location) +
        insert.
        """
        name = spec.source.name  # fixture logical name -- pure, no I/O
        cell_desc = f"{spec.property('commit') or 'plain'}/{spec.property('storage') or 'managed'}"
        bindings.plan.append(f"[{spec.access}] seed {target} from fixture {name!r} ({cell_desc})")
        if dry_run:
            return
        props = dict(CATALOG_MANAGED_PROPS) if spec.property("commit") == "cmt" else None
        location = self._s3_location(target) if spec.property("storage") == "external" else None
        definition = load_fixture(spec.source, [_FIXTURES])
        tbl = canonicalize(self._duckdb_cli(), definition)
        cols = ", ".join(f"{n} {t}" for n, t in map_columns(tbl, DATABRICKS_TYPE_MAP))
        rows = resolve_seed(spec.source.seed, tbl.seed_data)
        with step(f"seed {target} from fixture {name!r} ({cell_desc})"):
            create_table(target, cols, properties=props, location=location)
            if rows:
                insert(target, rows)

    def _instantiate_def(self, target, dry_run, bindings):
        """Run a Databricks Delta-artifact def (test/databricks/data/<table>.sql) verbatim; a
        companion <table>.insert.sql seeds via the DuckDB UC write path (the cmt__duckdb case).
        No def -> the table is treated as premade (reference only, no DDL).
        """
        _, _, table = _split_source(target)
        def_path = os.path.join(_DATA_DIR, f"{table}.sql")
        if not os.path.isfile(def_path):
            bindings.plan.append(f"[ro] reference {target} (premade, no def)")
            return
        with open(def_path) as f:
            external = "{location}" in f.read()
        location = self._s3_location(target) if external else None
        insert_path = os.path.join(_DATA_DIR, f"{table}.insert.sql")
        bindings.plan.append(f"[ro] provision {target} from {os.path.basename(def_path)}")
        if os.path.isfile(insert_path):
            bindings.plan.append(f"    + DuckDB UC insert ({os.path.basename(insert_path)})")
        if dry_run:
            return
        with step(f"provision {target} from {os.path.basename(def_path)}"):
            run_sql_file(def_path, table=target, location=location)
            if os.path.isfile(insert_path):
                self._duckdb_insert(insert_path)

    def _s3_location(self, target):
        """The S3 LOCATION for an `external` target: s3://<bucket>/<cat>/<schema>/<table>."""
        cat, schema, table = _split_source(target)
        return f"s3://{config.S3_BUCKET}/{cat}/{schema}/{table}"

    def _duckdb_cli(self):
        """The duckdb CLI from the same build the driver resolves the unittest binary from."""
        wd = getattr(self._config, "sqllogic_working_dir", None) or os.getcwd()
        return find_duckdb(self._config, wd)

    def _duckdb_insert(self, path):
        """Run a companion .insert.sql through the build's duckdb (the UC write path). Its
        ${DATABRICKS_*} creds are expanded from the env (present on the run path)."""
        wd = getattr(self._config, "sqllogic_working_dir", None) or os.getcwd()
        duckdb_bin = find_duckdb(self._config, wd)
        with open(path) as f:
            sql = os.path.expandvars(f.read())
        proc = subprocess.run([duckdb_bin, "-unsigned", "-c", sql], capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"DuckDB UC insert failed ({os.path.basename(path)}):\n{proc.stderr.strip()}")

    def teardown(self, token, bindings=None) -> None:
        """DROP each cell schema CASCADE (databricks_gen.drop_schema)."""
        cell_schemas = list(bindings.cell_schemas) if bindings else []
        if not cell_schemas:
            print(f"teardown: nothing to drop for token={token}")
            return
        catalog = bindings.catalog if bindings else os.environ.get("UC_TEST_CATALOG")
        for cell in cell_schemas:
            with step(f"drop cell schema {catalog}.{cell} CASCADE"):
                drop_schema(f"{catalog}.{cell}", cascade=True)

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
