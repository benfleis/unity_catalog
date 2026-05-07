#!/usr/bin/env python3
"""
Mock IRC scan plan server for POC testing.

Generates small Parquet test files whose schemas match the OSS Unity Catalog default tables
(marksheet, marksheet_uniform, numbers, user_countries).  Serves them via the three
scan-planning endpoints, routing by table name.

Usage:
    python uc/scripts/mock_scan_plan_server.py [--port 8081] [--async-first]
                                               [--plan-tasks-table <name>]

    --async-first          First POST /plan returns "submitted"; the subsequent GET returns
                           "completed".  Exercises the polling loop.
    --plan-tasks-table     Table name that returns plan-tasks tokens instead of inline
                           file-scan-tasks (default: plan_tasks_table).

Table routing:
    marksheet              → inline file-scan-tasks, marksheet schema
    marksheet_uniform      → inline file-scan-tasks, marksheet schema
    numbers                → inline file-scan-tasks, numbers schema
    user_countries         → inline file-scan-tasks, user_countries schema
    <plan-tasks-table>     → plan-tasks tokens; POST /tasks resolves each token to one file
    (any other name)       → falls back to marksheet data
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

TESTDATA_DIR = Path(__file__).parent.parent / "data" / "scan-plan-testdata"

# table_name → [FileScanTask, ...]  (inline file-scan-tasks)
TABLE_SCAN_TASKS: dict = {}

# plan-tasks-table name → {token: [FileScanTask]}
PLAN_TASK_MAP: dict = {}
PLAN_TASKS_TABLE = "plan_tasks_table"  # overridden by --plan-tasks-table

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

TABLE_QUERIES = {
    "marksheet": (
        "SELECT i::INTEGER AS id, 'name_'||i::VARCHAR AS name, (i*3)::INTEGER AS marks"
        " FROM range({start}, {end}) t(i)"
    ),
    "marksheet_uniform": (
        "SELECT i::INTEGER AS id, 'name_'||i::VARCHAR AS name, (i*3)::INTEGER AS marks"
        " FROM range({start}, {end}) t(i)"
    ),
    "numbers": (
        "SELECT i::INTEGER AS as_int, i::DOUBLE AS as_double"
        " FROM range({start}, {end}) t(i)"
    ),
    "user_countries": (
        "SELECT 'user_'||i::VARCHAR AS first_name,"
        "       (20 + (i % 50))::BIGINT AS age,"
        "       (['US','UK','CA','AU','DE'])[1 + (i % 5)] AS country"
        " FROM range({start}, {end}) t(i)"
    ),
}


def _make_task(path: Path) -> dict:
    return {
        "data-file": {
            "content": "data",
            "file-path": str(path.resolve()),
            "file-format": "parquet",
            "spec-id": 0,
            "partition": [],
            "file-size-in-bytes": path.stat().st_size,
            "record-count": 50,
        }
    }


def ensure_testdata():
    import duckdb

    TESTDATA_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()

    for tbl, query_tmpl in TABLE_QUERIES.items():
        tbl_dir = TESTDATA_DIR / tbl
        tbl_dir.mkdir(exist_ok=True)
        part1 = tbl_dir / "part1.parquet"
        part2 = tbl_dir / "part2.parquet"

        if not part1.exists() or not part2.exists():
            con.execute(
                f"COPY ({query_tmpl.format(start=1, end=51)}) TO '{part1}' (FORMAT parquet)"
            )
            con.execute(
                f"COPY ({query_tmpl.format(start=51, end=101)}) TO '{part2}' (FORMAT parquet)"
            )
            print(f"Generated {part1} and {part2}")

        TABLE_SCAN_TASKS[tbl] = [_make_task(part1), _make_task(part2)]

    con.close()

    # plan-tasks table: reuse marksheet files, split one-per-token
    pt_dir = TESTDATA_DIR / PLAN_TASKS_TABLE
    pt_dir.mkdir(exist_ok=True)
    ms_query = TABLE_QUERIES["marksheet"]
    pt1 = pt_dir / "part1.parquet"
    pt2 = pt_dir / "part2.parquet"
    if not pt1.exists() or not pt2.exists():
        con2 = duckdb.connect()
        con2.execute(
            f"COPY ({ms_query.format(start=1, end=51)}) TO '{pt1}' (FORMAT parquet)"
        )
        con2.execute(
            f"COPY ({ms_query.format(start=51, end=101)}) TO '{pt2}' (FORMAT parquet)"
        )
        con2.close()
        print(f"Generated {pt1} and {pt2}")

    PLAN_TASK_MAP[PLAN_TASKS_TABLE] = {
        "mock-plan-task-1": [_make_task(pt1)],
        "mock-plan-task-2": [_make_task(pt2)],
    }

    print(f"\nServing {len(TABLE_SCAN_TASKS)} tables from {TESTDATA_DIR}")
    for t, tasks in TABLE_SCAN_TASKS.items():
        print(f"  {t}: {len(tasks)} file-scan-tasks")
    print(f"  {PLAN_TASKS_TABLE}: plan-tasks (2 tokens)")


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def _file_scan_tasks_body(table: str) -> dict:
    tasks = TABLE_SCAN_TASKS.get(table) or TABLE_SCAN_TASKS.get("marksheet", [])
    return {
        "status": "completed",
        "plan-id": f"mock-{table}-1",
        "file-scan-tasks": tasks,
        "delete-files": [],
        "plan-tasks": [],
    }


def _plan_tasks_body(table: str) -> dict:
    tokens = list((PLAN_TASK_MAP.get(table) or {}).keys())
    return {
        "status": "completed",
        "plan-id": f"mock-{table}-plan-1",
        "file-scan-tasks": [],
        "delete-files": [],
        "plan-tasks": tokens,
    }


SUBMITTED_BODY = {"status": "submitted", "plan-id": "mock-async-1"}


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    async_first = False
    _first_post_done = False

    def log_message(self, fmt, *args):
        pass  # suppress default access log

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _send_json(self, code: int, body: dict):
        data = json.dumps(body, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _extract_table_endpoint(self):
        """Returns (table_name, endpoint) from the URL path.

        Handles /v1/{prefix}/namespaces/{ns}/tables/{table}/{endpoint}[/...]
        and     /v1/catalogs/{cat}/namespaces/{ns}/tables/{table}/{endpoint}[/...]
        """
        parts = self.path.split("/tables/", 1)
        if len(parts) < 2:
            return None, None
        segs = parts[1].split("/")
        table = segs[0]
        endpoint = segs[1] if len(segs) > 1 else ""
        return table, endpoint

    # ------------------------------------------------------------------
    # POST /plan  or  /tasks
    # ------------------------------------------------------------------

    def do_POST(self):
        raw = self._read_body()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {}

        table, endpoint = self._extract_table_endpoint()

        if endpoint == "tasks":
            self._handle_fetch_scan_tasks(table, payload)
        else:
            self._handle_plan(table, payload)

    def _handle_plan(self, table: str, payload: dict):
        filter_val = payload.get("filter")
        print(f"\nPOST plan  table={table!r}")
        if filter_val:
            print(f"  filter: {json.dumps(filter_val)}")
        else:
            print("  filter: (none)")

        if Handler.async_first and not Handler._first_post_done:
            Handler._first_post_done = True
            print("  → submitted (async-first)")
            self._send_json(200, SUBMITTED_BODY)
            return

        if table == PLAN_TASKS_TABLE:
            tokens = list((PLAN_TASK_MAP.get(table) or {}).keys())
            print(f"  → plan-tasks ({len(tokens)} tokens)")
            self._send_json(200, _plan_tasks_body(table))
        else:
            tasks = TABLE_SCAN_TASKS.get(table) or TABLE_SCAN_TASKS.get("marksheet", [])
            print(f"  → file-scan-tasks ({len(tasks)} files)")
            self._send_json(200, _file_scan_tasks_body(table))

    def _handle_fetch_scan_tasks(self, table: str, payload: dict):
        token = payload.get("plan-task", "")
        print(f"\nPOST tasks  table={table!r}  token={token!r}")
        token_map = PLAN_TASK_MAP.get(table) or {}
        tasks = token_map.get(token, [])
        body = {"file-scan-tasks": tasks, "plan-tasks": [], "delete-files": []}
        print(f"  → {len(tasks)} file-scan-tasks")
        self._send_json(200, body)

    # ------------------------------------------------------------------
    # GET /plan/{plan-id}  (poll)
    # ------------------------------------------------------------------

    def do_GET(self):
        table, _ = self._extract_table_endpoint()
        print(f"\nGET poll  table={table!r}")

        if table == PLAN_TASKS_TABLE:
            tokens = list((PLAN_TASK_MAP.get(table) or {}).keys())
            print(f"  → plan-tasks ({len(tokens)} tokens)")
            self._send_json(200, _plan_tasks_body(table))
        else:
            tasks = TABLE_SCAN_TASKS.get(table) or TABLE_SCAN_TASKS.get("marksheet", [])
            print(f"  → file-scan-tasks ({len(tasks)} files)")
            self._send_json(200, _file_scan_tasks_body(table))

    # ------------------------------------------------------------------
    # DELETE /plan/{plan-id}  (cancel)
    # ------------------------------------------------------------------

    def do_DELETE(self):
        table, _ = self._extract_table_endpoint()
        print(f"\nDELETE  table={table!r}")
        self.send_response(204)
        self.end_headers()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    global PLAN_TASKS_TABLE

    parser = argparse.ArgumentParser(description="Mock IRC scan plan server")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--async-first", action="store_true")
    parser.add_argument(
        "--plan-tasks-table",
        default="plan_tasks_table",
        help="Table name that returns plan-tasks tokens (default: plan_tasks_table)",
    )
    args = parser.parse_args()

    PLAN_TASKS_TABLE = args.plan_tasks_table
    ensure_testdata()

    Handler.async_first = args.async_first

    server = HTTPServer(("", args.port), Handler)
    print(f"\nListening on http://localhost:{args.port}")
    print(f"async-first: {args.async_first}")
    print(f"plan-tasks table: {PLAN_TASKS_TABLE!r}")
    print()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
