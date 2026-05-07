# Unity Catalog Extension

DuckDB extension for reading and writing [Delta Lake](https://delta.io/) tables via the [Unity Catalog](https://www.unitycatalog.io/) API.

For full documentation, see [duckdb.org/docs/extensions/unity_catalog](https://duckdb.org/docs/extensions/unity_catalog).

## Quick Start

```sql
INSTALL unity_catalog;
INSTALL delta;
LOAD delta;
LOAD unity_catalog;

CREATE SECRET (
    TYPE unity_catalog,
    TOKEN '⟨token⟩',
    ENDPOINT '⟨endpoint⟩',
    AWS_REGION '⟨region⟩'
);
ATTACH 'my_catalog' AS my_catalog (TYPE unity_catalog);
SELECT * FROM my_catalog.my_schema.my_table;
```

## Build Configuration

See `extension_config.cmake` to switch between a local delta checkout and the GitHub release.
