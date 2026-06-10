# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(unity_catalog
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# NOTE: replace with SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/../delta for local dev
duckdb_extension_load(delta
    GIT_URL https://github.com/benfleis/duckdb-delta
    GIT_TAG 0a77c6ec43a7febff36580960e5c1a8739d85420
    SUBMODULES extension-ci-tools
)
