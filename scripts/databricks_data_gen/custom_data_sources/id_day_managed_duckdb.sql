-- id_day_managed_duckdb: catalog-managed table, structure created by Spark, data inserted by DuckDB.
-- The Spark CREATE creates the catalog-managed table (staged commits). DuckDB inserts data via
-- the preview API, which promotes commits to _delta_log/ (not staged). This table should be
-- readable on both fixed and unfixed builds (DuckDB-written commits don't require max_catalog_version).
-- Contrast with id_day_managed_spark: same schema, same rows, but write path differs.
-- Generate: python scripts/databricks_data_gen/generate_databricks_test_data.py from-custom-sql \
--             scripts/databricks_data_gen/custom_data_sources/id_day_managed_duckdb.sql duckdb_testing.main
-- Data is inserted separately by the test_data_prepare Makefile target via DuckDB UC extension.
CREATE OR REPLACE TABLE {table_name}
    TBLPROPERTIES (
        'delta.feature.catalogManaged' = 'supported',
        'delta.enableRowTracking' = 'false'
    )
    AS SELECT id, CAST(NULL AS STRING) AS day FROM range(1, 1) AS t(id) WHERE 1=0
