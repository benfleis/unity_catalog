"""Per-extension (uc) pytest helpers.

Real, per-extension package (NOT symlinked) -- see test/py/driver/README.md "Layout".
Holds shared paths + the `uctl` table-ops wrapper; resource declarations/fixtures live
in sibling modules (e.g. server.py: the OSS UC server resource).

Imported by drivers as `from uc import uctl` / `from uc.server import uc_server`.
"""

import os
import subprocess
import sys
from pathlib import Path

# test/py/uc/__init__.py -> repo root is 3 dirs up (uc -> py -> test -> root).
# uc/ is a real dir (only test/py/driver is a symlink), so resolve() is safe here.
REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "scripts" / "oss_uc_image"
UCTL = os.environ.get("UC_UCTL", str(SCRIPTS_DIR / "uctl"))


def uctl(*args, check=True):
    """Run the image kit's `uctl <args>` (wraps `docker exec <container> bin/uc ...`).

    Container name via UC_DUCK_CONTAINER (uctl reads it); the `uc_server` fixture
    starts that container. Surfaces stdout/stderr on failure. Examples:

        uctl("create", "managed", "id_name", "id INT, name STRING")
        uctl("drop", "managed", "id_name", check=False)   # ignore "doesn't exist"
    """
    r = subprocess.run([UCTL, *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        sys.stderr.write(r.stdout + r.stderr)
        r.check_returncode()
    return r
