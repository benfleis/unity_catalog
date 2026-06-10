# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(unity_catalog
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# NOTE: replace with SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/../delta for local dev
duckdb_extension_load(delta
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/../delta
)
