# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(uc_catalog
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

duckdb_extension_load(delta
    #GIT_URL https://github.com/duckdb/duckdb-delta
    #GIT_TAG 48168a8ff954e9c3416f3e5affd201cf373b3250

    # XXX: temp until PR lands
    GIT_URL https://github.com/benfleis/duckdb-delta
    GIT_TAG fe9a092b2d032aa1c582de23b08f99811c36c7ef

    SUBMODULES extension-ci-tools
)
