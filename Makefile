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
	# Must be Python 3.12-3.14. The duckdb-pytest-driver (which brings pytest + xdist) is
	# installed editable separately; here we only add the databricks-gen SDK dep. With uv:
	#   uv venv --python 3.14 && ln -s .venv venv \
	#     && uv pip install -e <path-to>/duckdb-pytest-driver'[xdist]' \
	#     && uv pip install -r scripts/databricks_gen/requirements.txt
	python3 --version | grep -q '^Python 3[.]1[2-4][.]'
	python3 -m venv venv
	${PYTHON_PIP} install -r scripts/databricks_gen/requirements.txt

# The suite is pytest-driven (duckdb-pytest-driver): pytest collects the .test files and runs
# them through the unittest binary WITH provisioning (@requires fixtures, catalog setup, managed
# teardown). Point extension-ci-tools' test chain (test -> test_release -> test_release_internal)
# at pytest instead of its raw binary run, which would bypass the provisioner. So `make test` ==
# pytest; needs a built $(BUILD_DIR). OSS runs against the ducklabs container; databricks skips
# without creds -- use `make test_databricks` (wraps run_databricks_env). (Make warns once about
# overriding test_release_internal -- expected.)
test_release_internal:
	${PYTHON_BIN} -m pytest test

.PHONY: test_databricks
test_databricks:
	${ENV_DATABRICKS_CMD} ${PYTHON_BIN} -m pytest test/databricks
