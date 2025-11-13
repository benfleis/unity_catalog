import os
import sys
import argparse
from databricks.connect import DatabricksSession


def get_spark_session():
    token = os.environ.get('DATABRICKS_TOKEN')
    endpoint = os.environ.get('DATABRICKS_ENDPOINT')

    if not all([token, endpoint]):
        raise ValueError("Missing required environment variables: DATABRICKS_TOKEN and DATABRICKS_ENDPOINT")

    return DatabricksSession.builder.serverless().profile('DEFAULT').getOrCreate()


def copy_tables(source, destination, dry_run=True):
    spark = get_spark_session()

    source_catalog, source_schema = source.split('.')
    dest_catalog, dest_schema = destination.split('.')

    # List all tables in source
    tables = spark.sql(f"SHOW TABLES IN {source_catalog}.{source_schema}").collect()

    # Create the schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {dest_catalog}.{dest_schema}");

    for table in tables:
        table_name = table.tableName
        if dry_run:
            print(
                f"CREATE OR REPLACE TABLE {dest_catalog}.{dest_schema}.{table_name} LOCATION 's3://duckdb-databricks-testing-2/{dest_catalog}/{dest_schema}/{table_name}' AS SELECT * FROM {source_catalog}.{source_schema}.{table_name}")
        else:
            print(
                f"Copying table {source_catalog}.{source_schema}.{table_name} to {dest_catalog}.{dest_schema}.{table_name}")
            create_sql = f"""CREATE OR REPLACE TABLE {dest_catalog}.{dest_schema}.{table_name}
            LOCATION 's3://duckdb-databricks-testing-2/{dest_catalog}/{dest_schema}/{table_name}'
            AS
            SELECT *
            FROM {source_catalog}.{source_schema}.{table_name}"""
            spark.sql(create_sql)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='Source catalog.schema')
    parser.add_argument('destination', help='Destination catalog.schema')
    parser.add_argument('--dry-run', action='store_true', default=False, help='Print tables to be copied without copying them')
    args = parser.parse_args()

    copy_tables(args.source, args.destination, args.dry_run)


if __name__ == "__main__":
    main()
