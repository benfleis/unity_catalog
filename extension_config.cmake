# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(unity_catalog
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# NOTE: replace with SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/../delta for local dev
duckdb_extension_load(delta
    #GIT_URL https://github.com/duckdb/duckdb-delta
    #GIT_TAG 45c40878601b54b4188b09e08732fe0d576ad222
    GIT_URL https://github.com/benfleis/duckdb-delta
    GIT_TAG dee7b11dfa04e0ab467aa8c5dc04ce9083368ca5
    SUBMODULES extension-ci-tools
)
