"""Shared fixtures for oss_local tests.

`uc_server` (session-scoped) is registered here -- once for the whole oss_local dir --
so every test shares ONE OSS UC container per run. A fixture *imported into* each test
module is registered once per module, so a "session" fixture would otherwise start a
container per file.
"""

import pathlib

import pytest

from driver import register_provisioner
from uc.oss import OssProvisioner
from uc.server import uc_server  # noqa: F401  -- re-exported as a dir-wide fixture

_OSS_DIR = pathlib.Path(__file__).parent


def pytest_configure(config):
    """Register the OSS provisioner so `--repl` works for oss_local tests.

    Scoped to this conftest (the subtree) -- resolution is by test location, same seam
    as the databricks provisioner (driver/provision.py). --repl only; the run path uses
    the uc_server fixture + uctl.
    """
    register_provisioner(config, OssProvisioner(config), scope=_OSS_DIR)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    """Pin every oss_local test to one xdist worker.

    `uc_server` is session-scoped on a fixed name/port, so it assumes a single worker.
    Tagging the dir's items with a shared xdist_group makes the default
    `--dist=loadgroup` co-locate them on one worker -- one container, no port
    collision -- so a bare `pytest` (`-n auto`) just works without `-n0`. (Items
    outside this dir, e.g. databricks, are left ungrouped and distribute normally.)

    tryfirst is REQUIRED: xdist's own pytest_collection_modifyitems (worker-side,
    xdist/remote.py) READS the xdist_group marker to append `@group` to the nodeid for
    loadgroup scheduling. We must ADD the marker before xdist reads it; without
    tryfirst the registration order can flip (it did after the subdir restructure +
    importlib), leaving items ungrouped -> distributed -> the uc_server container
    races `docker run --name uc-duck` across workers.
    """
    mark = pytest.mark.xdist_group("oss_uc_server")
    for item in items:
        path = getattr(item, "path", None)
        if path is None:
            continue
        try:
            path.relative_to(_OSS_DIR)
        except ValueError:
            continue
        item.add_marker(mark)


@pytest.fixture(autouse=True)
def _uc_test_catalog(monkeypatch):
    """Inject ${UC_TEST_CATALOG} for OSS bodies (the seeded ducklabs catalog is `duck`).

    Scoped per-test via monkeypatch so it auto-restores -- it never leaks to a
    databricks test in a mixed run (whose provisioner resolves its own catalog).
    The run_paired subprocess inherits this env, so the body's `${UC_TEST_CATALOG}`
    substitutes to `duck`.
    """
    monkeypatch.setenv("UC_TEST_CATALOG", "duck")
