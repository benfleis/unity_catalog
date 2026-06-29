"""OSS UC provisioner for the --repl flow (sibling of uc.databricks.DatabricksProvisioner).

--repl ONLY today -- the run path uses the uc_server fixture + uctl seeding (see
test/sql/oss_local/). With NO @requires this still yields a useful REPL: a fresh
container + the locally-built extensions loaded + the `duck` catalog attached.
@requires(storage=managed|external) just selects which schema to ATTACH as the default
(the storage axis values map onto schemas: managed -> cmt, external -> plain).

Note: OSS has no clone-from-source step (uctl creates EMPTY tables and @requires carries
no column spec), so this does NOT seed per-spec tables. The REPL attaches `duck`; create
or seed interactively (SQL) or from another shell via `uctl create <schema> <table>
"<cols>"`. That's the deliberate difference from DatabricksProvisioner, which clones a
premade source table.
"""

import os
from dataclasses import dataclass, field

from uc import REPO_ROOT, server, uctl

# Locally-built extensions the body needs; LOADed by full path (duckdb -unsigned).
_EXTS = ("parquet", "httpfs", "delta", "unity_catalog")

# OSS test convention: one small table seeded in each schema so a --repl session has the
# shape the tests use. @requires carries no columns and OSS has no clone-from-source, so
# the shape is fixed here (matches the rw/contrast drivers).
_SEED_TABLE = "id_name"
_SEED_COLUMNS = "id INT, name STRING"


@dataclass
class OssBindings:
    """Result of provision(): what make_init / teardown need (cf. databricks Bindings)."""

    catalog: str
    default_schema: str
    token: str
    data_dir: str = None  # container's host bind-mount (None on dry_run)
    seeded: list = field(default_factory=list)  # (schema, table) pairs to drop on teardown
    env: dict = field(default_factory=dict)
    plan: list = field(default_factory=list)


# The generic @requires(storage=...) axis carries the framework values
# "managed"/"external"; OSS seeds schemas named cmt/plain, so map storage -> schema
# here. Unknown values pass through unchanged (robust to future axis values).
_STORAGE_TO_SCHEMA = {"managed": "cmt", "external": "plain"}


def _storage_to_schema(storage):
    return _STORAGE_TO_SCHEMA.get(storage, storage)


def _default_schema_for(specs):
    """DEFAULT_SCHEMA to ATTACH: first rw spec's storage mapped to its schema, else cmt.

    OSS maps the `storage` axis onto the seeded schema name (managed -> cmt, the
    catalog-managed schema; external -> plain) -- see scripts/oss_uc_image/uctl.
    """
    for s in specs:
        if s.access == "rw":
            return _storage_to_schema(s.storage)
    return "cmt"


class OssProvisioner:
    """Provisioner protocol impl (driver/provision.py) for the OSS UC ducklabs container."""

    def provision(self, specs, token, *, dry_run=False, params=None) -> OssBindings:
        os.environ.setdefault("UC_TEST_CATALOG", server._CATALOG)  # "duck"
        catalog = os.environ["UC_TEST_CATALOG"]
        # A parametrized test's `schema` param (e.g. test_rw[cmt]) picks the REPL
        # context; else fall back to the first rw @requires storage, else cmt.
        default_schema = (params or {}).get("schema")
        if default_schema not in server._SEED_SCHEMAS:
            default_schema = _default_schema_for(specs)

        b = OssBindings(catalog=catalog, default_schema=default_schema, token=token)
        b.plan.append(f"start OSS UC container {server.IMAGE} on {server.ENDPOINT}")
        for schema in server._SEED_SCHEMAS:
            b.plan.append(f'uctl create {schema} {_SEED_TABLE} "{_SEED_COLUMNS}"')
        b.plan.append(
            f"ATTACH '{catalog}' AS duck (TYPE unity_catalog, DEFAULT_SCHEMA '{default_schema}')"
        )
        b.env = {"UC_TEST_CATALOG": catalog, "UC_TEST_SCHEMA": default_schema}

        if not dry_run:
            srv = server.start_container()
            b.data_dir = srv.data_dir
            # Seed the convention table in BOTH schemas so the REPL matches any OSS test
            # (cmt = catalog-managed, plain) -- --repl can't see the
            # parametrized [cmt]/[plain] selection, so provide both.
            for schema in server._SEED_SCHEMAS:
                uctl("drop", schema, _SEED_TABLE, check=False)  # idempotent clean slate
                uctl("create", schema, _SEED_TABLE, _SEED_COLUMNS)
                b.seeded.append((schema, _SEED_TABLE))
        else:
            print("provision plan (NO container started):")
            for line in b.plan:
                print(f"  {line}")
        return b

    def make_init(self, b: OssBindings, *, redact: bool = False) -> str:
        """duckdb init SQL for `duckdb -unsigned -init`.

        LOAD local extensions, CREATE SECRET (the OSS token is the literal 'not-used',
        so nothing to redact), ATTACH duck with the chosen DEFAULT_SCHEMA, USE it.
        Extension paths resolve from $BUILD_DIR (default build/release), matching
        DatabricksProvisioner.make_init.
        """
        build_dir = os.environ.get(
            "BUILD_DIR", os.path.join(str(REPO_ROOT), "build", "release")
        )

        def ext(name):
            return os.path.join(build_dir, "extension", name, f"{name}.duckdb_extension")

        loads = "\n".join(f"LOAD '{ext(n)}';" for n in _EXTS)
        return f"""-- Auto-generated by uc.oss.OssProvisioner for `duckdb -unsigned -init`.
-- -unsigned (launch flag) is required to LOAD locally-built extensions.
{loads}

CREATE SECRET (
    TYPE UNITY_CATALOG,
    TOKEN 'not-used',
    ENDPOINT '{server.ENDPOINT}',
    AWS_REGION 'us-east-2'
);

ATTACH '{b.catalog}' AS duck (TYPE unity_catalog, DEFAULT_SCHEMA '{b.default_schema}');
USE duck;

.print ''
.print '== pytest --repl (OSS UC) ready =='
.print 'Attached: duck -> {b.catalog}, DEFAULT_SCHEMA {b.default_schema}'
.print 'Seeded: duck.cmt.{_SEED_TABLE}, duck.plain.{_SEED_TABLE}  ({_SEED_COLUMNS})'
.print 'USE duck.plain for the plain table; USE duck.cmt for catalog-managed.'
.print ''
"""

    def teardown(self, token, bindings=None) -> None:
        """Drop seeded tables, then stop the container (and clean its data dir)."""
        for schema, table in (getattr(bindings, "seeded", None) or []):
            uctl("drop", schema, table, check=False)
        data_dir = getattr(bindings, "data_dir", None) if bindings else None
        server.stop_container(data_dir)
