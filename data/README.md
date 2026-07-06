# About

This dir contains fixtures for (a) direct reading, and/or (b) copying, but they should never be directly modified.

Quick layout explanation:

- `delta/` -- delta tables, NOT UC generated, ready for UC import as EXTERNAL tables
- `delta/spark/` -- spark generated
- `delta/duckdb/` -- duckdb generated
