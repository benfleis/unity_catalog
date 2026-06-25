"""Shared fixtures for oss_local tests.

`uc_server` (session-scoped) is registered here -- once for the whole oss_local dir --
so every test shares ONE OSS UC container per run. A fixture *imported into* each test
module is registered once per module, so a "session" fixture would otherwise start a
container per file.
"""

from uc.server import uc_server  # noqa: F401  -- re-exported as a dir-wide fixture
