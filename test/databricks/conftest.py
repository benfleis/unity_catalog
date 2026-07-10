"""Databricks subtree conftest: registers the Databricks provisioner + creds.

Scoping the registration to this conftest means the framework's --cli flow finds
the Databricks backend ONLY when a test under test/databricks/ is selected —
resolution is by test location, not a hardcoded backend (see driver/provision.py
"REGISTRATION SEAM"). The root conftest already puts test/py on sys.path, so
`from uc.databricks import ...` resolves here.

Creds are fetched ONCE on the controller and broadcast to every worker via the driver's broadcast
seam (register_broadcast/get_broadcast), so a bare `pytest test/databricks` is wrapper-free -- no
per-worker `op`. Creds are a TERMINAL PREREQUISITE: if the databricks backend is in play and they
can't be obtained, the run FAILS LOUD (not a silent skip) -- see pytest_configure /
pytest_runtest_setup. To run credential-free, exclude these tests: `pytest -m 'not databricks'`.
"""

import os
import pathlib

import pytest

from driver import get_broadcast, register_broadcast, register_provisioner
from uc.databricks import DatabricksProvisioner
from uc.databricks.engine import cred_failure_detail, ensure_env, have_core_creds, load_creds

_CREDS_KEY = "databricks_creds"
_DBX_DIR = pathlib.Path(__file__).parent


def _no_creds_message():
    return (
        f"Databricks credentials unavailable ({cred_failure_detail()}).\n"
        "Fix the creds (environment or 1Password), or exclude these tests: "
        "pytest -m 'not databricks'."
    )


def pytest_configure(config):
    register_provisioner(config, DatabricksProvisioner(config), scope=str(_DBX_DIR))
    config.addinivalue_line("markers", "databricks: live Databricks tests (require credentials).")
    # Creds are an invocation-level (class-1) resource: fetched ONCE on the controller (env-wins,
    # else 1Password) and broadcast to every worker via the driver seam -- never per worker.
    register_broadcast(config, _CREDS_KEY, load_creds)
    # Adopt them into THIS process's env (controller for non-xdist / --repl; workers via
    # workerinput) so the SDK + `{DATABRICKS_*}` body substitution see them unchanged.
    os.environ.update(get_broadcast(config, _CREDS_KEY, {}) or {})
    # Explicit `pytest test/databricks…` loads this as an INITIAL conftest, so pytest_configure runs
    # on the CONTROLLER before workers spawn: creds are a hard prerequisite -> fail + STOP now (clean
    # red ERROR, no tests) with op's reason. (`pytest test` loads this only at worker collection, too
    # late for configure -> the runtest_setup guard below covers that path.)
    if not hasattr(config, "workerinput") and not have_core_creds():
        raise pytest.UsageError(_no_creds_message())


def _no_selection(config):
    """True if the invocation named no selection of its own -- no path/nodeid, no -k, no -m.
    (Verified: under xdist a worker sees the ORIGINAL args here, not its assigned node-ids.)"""
    o = config.option
    return not o.file_or_dir and not o.keyword and not o.markexpr


def pytest_collection_modifyitems(config, items):
    """Mark test/databricks items `databricks`; on a BARE invocation (no path / -k / -m) deselect
    them, so the default `pytest` is a fast, credential-free SMOKE run (OSS only).

    Any explicit selection opts the live tier back in and is respected verbatim:
      pytest test/databricks   pytest -m databricks   pytest -k <name>   pytest test (everything).
    Only the "no selection at all" case gets the smoke default -- a static addopts `-m`/testpaths
    would instead filter globally (even when you point straight at test/databricks).
    """
    for item in items:
        path = getattr(item, "path", None)
        if path is None:
            continue
        try:
            path.relative_to(_DBX_DIR)
        except ValueError:
            continue
        item.add_marker(pytest.mark.databricks)
    if _no_selection(config):
        live = [it for it in items if it.get_closest_marker("databricks")]
        if live:
            config.hook.pytest_deselected(items=live)
            items[:] = [it for it in items if not it.get_closest_marker("databricks")]


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """Databricks tests REQUIRE creds -- a terminal prerequisite, so FAIL (not skip) when absent.

    Creds are fetched UP FRONT, never mid-run -- an `op` auth prompt (biometric / lapsed session)
    must land at invocation, not minutes into a suite (fatal for a benchmark run). Sources: the
    controller broadcast for an explicit `pytest test/databricks` run, or the launching shell's env
    (`run_databricks_env`). A whole-suite `pytest` that keeps databricks tests without up-front
    creds fails here -- run `pytest test/databricks` (fetches once, up front), use the wrapper, or
    exclude with `-m 'not databricks'`. A *hook* (not a fixture) so it covers .py drivers AND bare
    .test items. tryfirst: before the `resources` fixture provisions.
    """
    ensure_env(dry_run=True)  # catalog default only (no creds check)
    if not have_core_creds():
        pytest.fail(_no_creds_message(), pytrace=False)
