PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=unity_catalog
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions that we need for crucial testing
DEFAULT_TEST_EXTENSION_DEPS=parquet;httpfs;tpch;tpcds

# uv should work if created as suggested in venv: below, but allow overrides
PYTHON_PIP=venv/bin/python3 -m pip
PYTHON_BIN=venv/bin/python3

ENV_DATABRICKS_CMD ?= scripts/run_databricks_env

# Include the Makefile from extension-ci-tools (build targets: release/debug/…)
include extension-ci-tools/makefiles/duckdb_extension.Makefile

venv:
	# The whole test venv is declared in pyproject.toml (the driver[xdist] editable from ../driver, plus
	# databricks-sdk); pytest config lives next door in pytest.ini. `uv sync` builds it from that one
	# manifest — no more separate editable-driver install + scripts/databricks_gen/requirements.txt.
	# Requires Python 3.12-3.14 (pinned in pyproject's requires-python). Then: `uv run pytest`.
	uv sync
	ln -sfn .venv venv

# The suite is pytest-driven (duckdb-pytest-driver): pytest collects the .test files and runs
# them through the unittest binary WITH provisioning (@requires fixtures, catalog setup, managed
# teardown). Point extension-ci-tools' test chain (test -> test_release -> test_release_internal)
# at pytest instead of its raw binary run, which would bypass the provisioner. So `make test` ==
# pytest; needs a built $(BUILD_DIR). OSS runs against the ducktest container; databricks skips
# without creds -- use `make test_databricks` (wraps run_databricks_env). (Make warns once about
# overriding test_release_internal -- expected.)
test_release_internal:
	${PYTHON_BIN} -m pytest test

.PHONY: test_databricks
test_databricks:
	${ENV_DATABRICKS_CMD} ${PYTHON_BIN} -m pytest test/databricks
