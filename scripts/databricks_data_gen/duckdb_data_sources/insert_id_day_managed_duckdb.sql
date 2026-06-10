-- Inserts data into id_day_managed_duckdb via the DuckDB UC extension.
-- This uses the preview API write path, which promotes commits to _delta_log/
-- (not staged), in contrast to Spark-written id_day_managed_spark.
-- Run via test_data_prepare after id_day_managed_duckdb table has been created by Spark.
CREATE SECRET (TYPE UNITY_CATALOG, TOKEN '${DATABRICKS_TOKEN}', ENDPOINT '${DATABRICKS_ENDPOINT}', AWS_REGION '${DATABRICKS_REGION}');
ATTACH 'duckdb_testing' (TYPE UNITY_CATALOG, DEFAULT_SCHEMA 'main');
INSERT INTO duckdb_testing.main.id_day_managed_duckdb
SELECT id,
    CASE ((id-1)%5)
        WHEN 0 THEN 'Mon'
        WHEN 1 THEN 'Tue'
        WHEN 2 THEN 'Wed'
        WHEN 3 THEN 'Thu'
        ELSE 'Fri'
    END AS day
FROM range(1, 51) t(id);
