#!/usr/bin/env python3

"""Setup/teardown for write.test.

Creates an empty catalog-managed source table `duck.managed.id_name` in the
running OSS-UC "duck" image so the paired .test can INSERT into it. Plain script
on purpose -- NOT wired to pytest. Usage:

    python test/sql/oss_local/write.py            # setup (default)
    build/release/test/unittest "test/sql/oss_local/write.test"
    python test/sql/oss_local/write.py teardown   # drop the table

Table ops go through the image kit's `uctl` helper (scripts/oss_uc_image/uctl),
which wraps `docker exec <container> bin/uc ...`. Assumes the duck image is already
running (kit run) on :8080. Container name via DUCK_UC_CONTAINER (uctl reads it);
uctl path overridable via UC_UCTL.
"""

import os
import subprocess
import sys
from pathlib import Path

# scripts/oss_uc_scripts/uctl lives at the repo root, 3 dirs up from test/sql/oss_local.
UCTL = os.environ.get(
    "UC_UCTL",
    str(Path(__file__).resolve().parents[3] / "scripts" / "oss_uc_scripts" / "uctl"),
)

# while MANAGED refers to UC storage management, in current image it also is coupled with catalog-managed
SCHEMA = "managed"
TABLE = "id_name"
COLUMNS = "id INT, name STRING"


def _uctl(*args, check=True):
    """Run the kit's `uctl <args>`; surface output on failure."""
    r = subprocess.run([UCTL, *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        sys.stderr.write(r.stdout + r.stderr)
        r.check_returncode()
    return r


def setup():
    """Create an empty id_name table; drop-then-create gives a clean slate."""
    _uctl("drop", SCHEMA, TABLE, check=False)  # ignore "doesn't exist"
    _uctl("create", SCHEMA, TABLE, COLUMNS)
    print(f"created duck.{SCHEMA}.{TABLE} ({COLUMNS})")


def teardown():
    _uctl("drop", SCHEMA, TABLE, check=False)
    print(f"dropped duck.{SCHEMA}.{TABLE}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "setup"
    actions = {"setup": setup, "teardown": teardown}
    if cmd not in actions:
        sys.exit(f"usage: {sys.argv[0]} [setup|teardown]")
    actions[cmd]()
