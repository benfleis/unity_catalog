PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=uc_catalog
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Core extensions that we need for crucial testing
#DEFAULT_TEST_EXTENSION_DEPS=parquet;httpfs;delta
DEFAULT_TEST_EXTENSION_DEPS=parquet;httpfs

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
