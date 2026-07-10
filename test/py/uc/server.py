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

NOTE (xdist): ONE shared container per invocation across all workers. It's a host singleton
(fixed name/port), booted first-worker-wins under a filesystem lock and torn down once by the
controller (oss_local/conftest.py pytest_sessionfinish) so it outlives every worker using it.
No xdist_group / single-worker pinning needed -- OSS tests distribute normally.
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


# Published while the session `uc_server` fixture holds the container. OssProvisioner
# reads it to tell the RUN path (reuse this container, don't touch its lifecycle) from
# the --repl path (no fixtures run, so the provisioner owns a fresh container itself).
_ACTIVE_SERVER = None


def active_server():
    """The session container if `uc_server` is active, else None (e.g. the --repl path)."""
    return _ACTIVE_SERVER


def stop_container(data_dir=None):
    """Stop the OSS UC container (run used --rm, so stop removes it) + clean its data dir."""
    with step("stopping OSS UC docker image"):
        _docker("stop", CONTAINER, check=False)  # ALWAYS_DESTROY
    if data_dir:
        with step("removing test temporary dir"):
            shutil.rmtree(data_dir, ignore_errors=True)


# --- shared-container coordination (first-worker-wins across xdist workers) ----------------
#
# The container is a host singleton (fixed name/port) started ONCE per invocation and shared by
# all workers: the first worker to need it wins a filesystem lock and boots it; the rest block on
# the lock, then reuse. Teardown is the controller's job (see conftest pytest_sessionfinish) so the
# container outlives every worker. State/lock live at fixed host paths keyed by the container name
# (its host-singleton nature); the invocation id (driver run-id) tags state so a stale container
# from a prior run is replaced (ALWAYS_CREATE), not reused.
_STATE_PATH = os.path.join(tempfile.gettempdir(), f"{CONTAINER}.state.json")
_LOCK_PATH = os.path.join(tempfile.gettempdir(), f"{CONTAINER}.start.lock")
_LOCK_STALE_S = _READY_TIMEOUT_S + 60  # steal a lock held longer than a full boot (crashed holder)


class _StartLock:
    """Minimal cross-process mutex (no filelock dep): O_EXCL create = held, unlink = released.
    Steals a lock file older than _LOCK_STALE_S (a holder that crashed mid-boot)."""

    def __enter__(self):
        while True:
            try:
                fd = os.open(_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return self
            except FileExistsError:
                try:
                    if time.time() - os.path.getmtime(_LOCK_PATH) > _LOCK_STALE_S:
                        os.unlink(_LOCK_PATH)  # crashed holder -> steal
                        continue
                except OSError:
                    pass
                time.sleep(0.2)

    def __exit__(self, *exc):
        try:
            os.unlink(_LOCK_PATH)
        except OSError:
            pass
        return False


def _invocation_id(config):
    """This invocation's shared id (the driver run-id): identical on controller + all workers."""
    wi = getattr(config, "workerinput", None)
    if wi and "sqllogic_run_id" in wi:
        return wi["sqllogic_run_id"]
    return getattr(config, "_sqllogic_run_id", None) or "single"


def _container_running():
    r = _docker("inspect", "-f", "{{.State.Running}}", CONTAINER, check=False)
    return r.returncode == 0 and r.stdout.strip() == "true"


def _read_state():
    try:
        with open(_STATE_PATH) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _ensure_shared_container(invocation_id):
    """Return the ONE shared UcServer for this invocation, booting it (under the lock) iff no live
    container for this invocation exists yet. First-worker-wins: losers block on the lock, then
    reuse the winner's container."""
    with _StartLock():
        info = _read_state()
        if info and info.get("invocation") == invocation_id and _container_running():
            return UcServer(endpoint=info["endpoint"], container=info["container"], data_dir=info["data_dir"])
        srv = start_container()  # ALWAYS_CREATE: docker rm -f first -> fresh + ready before we publish
        with open(_STATE_PATH, "w") as f:
            json.dump(
                {"invocation": invocation_id, "endpoint": srv.endpoint,
                 "container": srv.container, "data_dir": srv.data_dir},
                f,
            )
        return srv


def teardown_shared():
    """Stop + clean the shared container. Controller-only (conftest pytest_sessionfinish); the
    container is shared, so it must outlive every worker that used it. No-op if never started."""
    info = _read_state()
    if not info:
        return
    stop_container(info.get("data_dir"))
    try:
        os.unlink(_STATE_PATH)
    except OSError:
        pass


@pytest.fixture(scope="session")
def uc_server(request):
    """Acquire the ONE shared OSS UC server for this invocation (first-worker-wins).

    Per OSS_UC_SERVER: ALWAYS_CREATE (fresh per invocation) / ALWAYS_DESTROY (the controller tears
    it down at session end). Shared across xdist workers via a filesystem lock, so OSS tests
    distribute normally -- no xdist_group.
    """
    spec = OSS_UC_SERVER
    assert (
        spec.create == "ALWAYS_CREATE" and spec.destroy == "ALWAYS_DESTROY"
    ), f"only ALWAYS_CREATE/ALWAYS_DESTROY is wired today; got {spec}"

    srv = _ensure_shared_container(_invocation_id(request.config))
    global _ACTIVE_SERVER
    _ACTIVE_SERVER = srv  # publish for OssProvisioner (run path reuses this container)
    try:
        yield srv
    finally:
        _ACTIVE_SERVER = None
        # No stop here: the container is shared across workers; the controller stops it once at
        # pytest_sessionfinish (oss_local/conftest.py).
