"""Databricks subtree conftest: registers the Databricks provisioner.

Scoping the registration to this conftest means the framework's --cli flow finds
the Databricks backend ONLY when a test under test/databricks/ is selected —
resolution is by test location, not a hardcoded backend (see driver/provision.py
"REGISTRATION SEAM"). The root conftest already puts test/py on sys.path, so
`from uc.databricks import ...` resolves here.
"""

import os

import pytest

from driver import register_provisioner
from uc.databricks import DatabricksProvisioner
from uc.databricks.engine import ensure_env


def pytest_configure(config):
    register_provisioner(
        config, DatabricksProvisioner(config), scope=os.path.dirname(__file__)
    )


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    """Make databricks tests run when creds are present, skip gracefully when not.

    A *hook* (not a fixture) so it covers BOTH .py drivers and bare .test items —
    the latter are custom sqllogic items that never run fixtures. Defaults
    UC_TEST_CATALOG and CHECKS that creds are in the env (it never fetches them — creds
    are front-loaded by the launching shell, `scripts/run_databricks_env pytest …`; see
    uc.databricks.engine._require_creds and test/py/driver/PYTEST.md for why fetching
    in-process is wrong under xdist). If creds are absent the test SKIPS rather than
    errors — a bare `pytest` without the wrapper just sees databricks skips, no flag
    needed. tryfirst so the check runs before the `resources` fixture provisions.
    """
    try:
        ensure_env()
    except pytest.UsageError as e:
        pytest.skip(f"Databricks unavailable: {e}")
