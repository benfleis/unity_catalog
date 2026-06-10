-- id_day_managed_spark: catalog-managed table, all data written by Spark (staged commits).
-- Used to verify that max_catalog_version is correctly passed to the Delta kernel so that
-- staged commits are visible. On an unfixed build this fails with:
--   "Staged commits in log_tail require max_catalog_version to be set"
-- Same schema as scan_plan_days_managed: id INT, day VARCHAR, 50 rows across 5 INSERTs.
-- Generate: python scripts/databricks_data_gen/generate_databricks_test_data.py from-custom-sql \
--             scripts/databricks_data_gen/custom_data_sources/id_day_managed_spark.sql duckdb_testing.main
CREATE OR REPLACE TABLE {table_name}
    TBLPROPERTIES (
        'delta.feature.catalogManaged' = 'supported',
        'delta.enableRowTracking' = 'false'
    )
    AS SELECT id, 'Mon' AS day FROM range(1, 11) AS t(id);

INSERT INTO {table_name} SELECT id, 'Tue' AS day FROM range(11, 21) AS t(id);
INSERT INTO {table_name} SELECT id, 'Wed' AS day FROM range(21, 31) AS t(id);
INSERT INTO {table_name} SELECT id, 'Thu' AS day FROM range(31, 41) AS t(id);
INSERT INTO {table_name} SELECT id, 'Fri' AS day FROM range(41, 51) AS t(id)
