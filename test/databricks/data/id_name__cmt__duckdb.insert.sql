-- Insert the id_name rows into id_name__cmt__duckdb via the DuckDB UC write path
-- (preview API -> promoted commits, not staged), after the empty catalog-managed shell has
-- been created server-side (id_name__cmt__duckdb.sql). Run through the duckdb shell with the
-- databricks creds in the env, e.g.  `envsubst < this | $BUILD_DIR/duckdb`.
CREATE SECRET (TYPE UNITY_CATALOG, TOKEN '${DATABRICKS_TOKEN}', ENDPOINT '${DATABRICKS_ENDPOINT}', AWS_REGION '${DATABRICKS_REGION}');
ATTACH 'duckdb_testing' (TYPE UNITY_CATALOG, DEFAULT_SCHEMA 'main');
INSERT INTO duckdb_testing.main.id_name__cmt__duckdb VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five');
