"""OSS Unity Catalog server -- our first require/resource declaration.

A running OSS UC "ducklabs" docker server, declared per the driver framework's resource
model (test/py/driver/README.md, "Resources & disposition"): a flat triple of
acquire-mode x create-disposition x destroy-disposition, *executed* by a pytest fixture.
Fixture scopes are the provision/share/release engine -- we don't hand-roll a manager.

Chosen policy: always-fresh per session.
  acquire-mode = shared         (tests share one server; they dirty tables, not the service)
  create       = ALWAYS_CREATE  (force a fresh container at session start)
  destroy      = ALWAYS_DESTROY (stop it at session end)
Isolation is by-lifecycle (recreate fresh, fixed name/port) -- cheap for local UC.

NOTE (xdist): this is session-scoped on a fixed port, so it assumes a single worker for
the tests that use it. Under `-n auto` run these in one xdist_group (or `-p no:xdist`)
until per-worker isolation is added.
"""

import json
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

import pytest

from driver import step
from uc import SCRIPTS_DIR


@dataclass(frozen=True)
class ResourceSpec:
    """Flat resource declaration (see driver README). The fixture executes it."""

    identity: str
    acquire_mode: str  # "shared" | "exclusive"
    create: str  # "NEVER_CREATE" | "MAY_CREATE" | "ALWAYS_CREATE"
    destroy: str  # "NEVER_DESTROY" | "MAY_DESTROY" | "ALWAYS_DESTROY"


OSS_UC_SERVER = ResourceSpec(
    identity="oss-uc-server",
    acquire_mode="shared",
    create="ALWAYS_CREATE",
    destroy="ALWAYS_DESTROY",
)

# Fixed name/port (by-lifecycle isolation). A host client resolves the server's absolute
# file:// table paths only if the bind-mount path matches, which the kit `run` script
# guarantees (identical host==container data dir); hence we provision via `run`.
CONTAINER = os.environ.get("UC_DUCK_CONTAINER", "uc-duck")
PORT = int(os.environ.get("UC_DUCK_PORT", "8080"))
IMAGE = os.environ.get("UC_DUCK_IMAGE", "ghcr.io/benfleis/unitycatalog-ducklabs:local")
ENDPOINT = f"http://127.0.0.1:{PORT}"

_CATALOG = "duck"
_SEED_SCHEMAS = ("cmt", "plain")  # entrypoint seeds these after the catalog
_READY_URL = f"{ENDPOINT}/api/2.1/unity-catalog/schemas?catalog_name={_CATALOG}"
_READY_TIMEOUT_S = 120


@dataclass(frozen=True)
class UcServer:
    """What the fixture yields to a test/driver."""

    endpoint: str
    container: str
    data_dir: str


def _docker(*args, check=True):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check)


def _wait_ready(timeout_s):
    """Poll until the seeded catalog AND its cmt/plain schemas exist, or time out.

    The entrypoint seeds the `duck` catalog first, then its schemas, so waiting only on
    the catalog races the seed -- `uctl create` then 404s with SCHEMA_NOT_FOUND. Wait on
    the schemas instead (plain is seeded last, so its presence implies cmt too).
    """
    deadline = time.time() + timeout_s
    last = "no attempt"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(_READY_URL, timeout=3) as resp:
                names = {s.get("name") for s in json.load(resp).get("schemas", [])}
            missing = [s for s in _SEED_SCHEMAS if s not in names]
            if not missing:
                return
            last = f"catalog up; schemas present={sorted(names)}, still missing={missing}"
        except (urllib.error.URLError, OSError) as e:  # 404 (catalog not yet) / refused while booting
            last = repr(e)
        time.sleep(1)
    raise RuntimeError(
        f"OSS UC not ready at {_READY_URL} after {timeout_s}s "
        f"(need schemas {list(_SEED_SCHEMAS)} in catalog {_CATALOG!r}): {last}"
    )


def start_container():
    """Start a fresh OSS UC container on the fixed name/port; return UcServer.

    Shared by the `uc_server` fixture (run path) and OssProvisioner (--repl, which runs
    no fixtures so the provisioner owns the container).
    """
    data_dir = tempfile.mkdtemp(prefix="uc-duck-data-")
    env = {
        **os.environ,
        "FINAL_IMAGE": IMAGE,
        "UC_DUCK_CONTAINER": CONTAINER,
        "UC_DUCK_PORT": str(PORT),
    }
    with step(f"starting OSS UC docker image ({IMAGE})"):
        _docker("rm", "-f", CONTAINER, check=False)  # ALWAYS_CREATE: force a fresh container
        # `run` = the kit's single source of truth for the docker-run line (identical-path mount).
        # Suppress its stdout info-block so step() is the sole provisioning trace (the
        # --steps / --repl narration channel); stderr stays for failures.
        subprocess.run([str(SCRIPTS_DIR / "run"), data_dir], env=env, check=True, stdout=subprocess.DEVNULL)
        _wait_ready(_READY_TIMEOUT_S)  # waits until the seeded duck.cmt/plain schemas exist
    return UcServer(endpoint=ENDPOINT, container=CONTAINER, data_dir=data_dir)


def stop_container(data_dir=None):
    """Stop the OSS UC container (run used --rm, so stop removes it) + clean its data dir."""
    with step("stopping OSS UC docker image"):
        _docker("stop", CONTAINER, check=False)  # ALWAYS_DESTROY
    if data_dir:
        with step("removing test temporary dir"):
            shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def uc_server():
    """Provision/acquire the OSS UC server resource per OSS_UC_SERVER (see module docstring)."""
    spec = OSS_UC_SERVER
    assert (
        spec.create == "ALWAYS_CREATE" and spec.destroy == "ALWAYS_DESTROY"
    ), f"only ALWAYS_CREATE/ALWAYS_DESTROY is wired today; got {spec}"

    srv = start_container()
    try:
        yield srv
    finally:
        stop_container(srv.data_dir)
