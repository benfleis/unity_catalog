"""Shared fixtures for oss_local tests.

`uc_server` is registered here -- once for the whole oss_local dir -- so every test shares ONE
OSS UC container per invocation. The container is booted first-worker-wins (shared across xdist
workers; see uc.server) and torn down once by the controller in pytest_sessionfinish below, so
OSS tests distribute across workers normally -- no xdist_group pinning.
"""

import pathlib

import pytest

from driver import register_provisioner
from uc.oss import OssProvisioner
from uc.server import (  # noqa: F401  (uc_server re-exported dir-wide)
    _invocation_id,
    reclaim_stale,
    teardown_shared,
    uc_server,
)

_OSS_DIR = pathlib.Path(__file__).parent


def pytest_configure(config):
    """Register the OSS provisioner so `--repl` works for oss_local tests.

    Scoped to this conftest (the subtree) -- resolution is by test location, same seam
    as the databricks provisioner (driver/provision.py). --repl only; the run path uses
    the uc_server fixture + uctl.
    """
    register_provisioner(config, OssProvisioner(config), scope=_OSS_DIR)
    config.addinivalue_line(
        "markers", "oss_local: OSS-local UC tests (uc_server container)."
    )


@pytest.hookimpl(trylast=True)
def pytest_sessionstart(session):
    """Controller-only, BEFORE any worker races the start-lock: reclaim a container/state/lock leaked
    by a prior interrupted/killed run. This is the ROBUST recovery net -- under xdist, Ctrl-C is owned
    by xdist and pytest_keyboard_interrupt/atexit don't reliably fire on the controller, so rather
    than try to tear down on the way out, we clean up any leak at the NEXT session start.

    trylast so xdist's own sessionstart has set up the worker nodes (and, via pytest_configure_node,
    computed this run's shared run-id on the controller config) -- so _invocation_id() returns the
    real id rather than the "single" fallback.
    """
    config = session.config
    if getattr(config, "workerinput", None) is not None:
        return  # worker: never reclaims the shared container
    reclaim_stale(_invocation_id(config))


def pytest_sessionfinish(session, exitstatus):
    """Tear down the shared OSS UC container once, on the controller only.

    Workers share one container (uc.server), so none may stop it -- it must outlive them all. The
    controller's sessionfinish runs after every worker has finished. No-op if never started (e.g. a
    databricks-only run) or when called on a worker.
    """
    if getattr(session.config, "workerinput", None) is not None:
        return  # worker
    teardown_shared()


def pytest_collection_modifyitems(items):
    """Mark every oss_local item `oss_local` so `-m oss_local` selects the whole subtree.

    No xdist_group: the uc_server container is shared first-worker-wins across workers (uc.server),
    so OSS tests distribute normally.
    """
    oss_local = pytest.mark.oss_local
    for item in items:
        path = getattr(item, "path", None)
        if path is None:
            continue
        try:
            path.relative_to(_OSS_DIR)
        except ValueError:
            continue
        item.add_marker(oss_local)


@pytest.fixture(autouse=True)
def _uc_test_catalog(monkeypatch):
    """Inject ${UC_TEST_CATALOG} for OSS bodies (the seeded ducklabs catalog is `duck`).

    Scoped per-test via monkeypatch so it auto-restores -- it never leaks to a
    databricks test in a mixed run (whose provisioner resolves its own catalog).
    The run_paired subprocess inherits this env, so the body's `${UC_TEST_CATALOG}`
    substitutes to `duck`.
    """
    monkeypatch.setenv("UC_TEST_CATALOG", "duck")
