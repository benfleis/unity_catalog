#!/usr/bin/env python3
"""
Mock IRC scan plan server for POC testing.

Generates small Parquet test files whose schemas match the OSS Unity Catalog default tables
and serves them via the three scan-planning endpoints, routing by table name.

Table behaviour:
    marksheet         → single combined parquet, returned as 1 inline file-scan-task
    marksheet_uniform → same as marksheet
    numbers           → 1 plan-task token → resolves to 2 file-scan-tasks (part1 + part2)
    user_countries    → hybrid: 1 inline file-scan-task (part1) +
                                1 plan-task token → part2
    (any other name)  → falls back to marksheet behaviour

Usage:
    python uc/scripts/mock_scan_plan_server.py [--port 8081] [--async-first]

    --async-first   First POST /plan returns "submitted"; the subsequent GET returns
                    "completed".  Exercises the polling loop.
"""

import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

TESTDATA_DIR = Path(__file__).parent.parent / "data" / "scan-plan-testdata"

# table_name → list of inline FileScanTask dicts
TABLE_INLINE_TASKS: dict = {}

# table_name → {token: [FileScanTask]}  (populated for plan-task tables)
TABLE_PLAN_TASK_MAP: dict = {}

# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

TABLE_QUERIES = {
    "marksheet": (
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


def _make_task(path: Path, record_count: int) -> dict:
    return {
        "data-file": {
            "content": "data",
            "file-path": str(path.resolve()),
            "file-format": "parquet",
            "spec-id": 0,
            "partition": [],
            "file-size-in-bytes": path.stat().st_size,
            "record-count": record_count,
        }
    }


def ensure_testdata():
    import duckdb

    TESTDATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    # ------------------------------------------------------------------
    # marksheet / marksheet_uniform: single combined file, inline
    # ------------------------------------------------------------------
    ms_query = TABLE_QUERIES["marksheet"]
    for tbl in ("marksheet", "marksheet_uniform"):
        tbl_dir = TESTDATA_DIR / tbl
        tbl_dir.mkdir(exist_ok=True)
        combined = tbl_dir / "all.parquet"
        if not combined.exists():
            con.execute(
                f"COPY ({ms_query.format(start=1, end=101)}) TO '{combined}' (FORMAT parquet)"
            )
            print(f"Generated {combined}")
        TABLE_INLINE_TASKS[tbl] = [_make_task(combined, 100)]

    # ------------------------------------------------------------------
    # numbers: part1 + part2, served via a single plan-task token
    # ------------------------------------------------------------------
    num_dir = TESTDATA_DIR / "numbers"
    num_dir.mkdir(exist_ok=True)
    num_q = TABLE_QUERIES["numbers"]
    num_p1 = num_dir / "part1.parquet"
    num_p2 = num_dir / "part2.parquet"
    if not num_p1.exists() or not num_p2.exists():
        con.execute(f"COPY ({num_q.format(start=1,  end=51)})  TO '{num_p1}' (FORMAT parquet)")
        con.execute(f"COPY ({num_q.format(start=51, end=101)}) TO '{num_p2}' (FORMAT parquet)")
        print(f"Generated {num_p1} and {num_p2}")
    TABLE_INLINE_TASKS["numbers"] = []  # no inline files
    TABLE_PLAN_TASK_MAP["numbers"] = {
        "numbers-task-1": [_make_task(num_p1, 50), _make_task(num_p2, 50)],
    }

    # ------------------------------------------------------------------
    # user_countries: part1 inline, part2 via plan-task token (hybrid)
    # ------------------------------------------------------------------
    uc_dir = TESTDATA_DIR / "user_countries"
    uc_dir.mkdir(exist_ok=True)
    uc_q = TABLE_QUERIES["user_countries"]
    uc_p1 = uc_dir / "part1.parquet"
    uc_p2 = uc_dir / "part2.parquet"
    if not uc_p1.exists() or not uc_p2.exists():
        con.execute(f"COPY ({uc_q.format(start=1,  end=51)})  TO '{uc_p1}' (FORMAT parquet)")
        con.execute(f"COPY ({uc_q.format(start=51, end=101)}) TO '{uc_p2}' (FORMAT parquet)")
        print(f"Generated {uc_p1} and {uc_p2}")
    TABLE_INLINE_TASKS["user_countries"] = [_make_task(uc_p1, 50)]
    TABLE_PLAN_TASK_MAP["user_countries"] = {
        "user-countries-task-1": [_make_task(uc_p2, 50)],
    }

    con.close()

    print(f"\nTestdata ready in {TESTDATA_DIR}")
    for tbl in ("marksheet", "marksheet_uniform", "numbers", "user_countries"):
        n_inline = len(TABLE_INLINE_TASKS.get(tbl, []))
        n_tokens = len(TABLE_PLAN_TASK_MAP.get(tbl, {}))
        print(f"  {tbl}: {n_inline} inline task(s), {n_tokens} plan-task token(s)")


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def _completed_body(table: str) -> dict:
    inline = TABLE_INLINE_TASKS.get(table)
    if inline is None:
        inline = TABLE_INLINE_TASKS.get("marksheet", [])
    tokens = list((TABLE_PLAN_TASK_MAP.get(table) or {}).keys())
    return {
        "status": "completed",
        "plan-id": f"mock-{table}-1",
        "file-scan-tasks": inline,
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
        pass

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
        parts = self.path.split("/tables/", 1)
        if len(parts) < 2:
            return None, None
        segs = parts[1].split("/")
        return segs[0], (segs[1] if len(segs) > 1 else "")

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

        body = _completed_body(table)
        n_inline = len(body["file-scan-tasks"])
        n_tokens = len(body["plan-tasks"])
        print(f"  → completed: {n_inline} inline task(s), {n_tokens} plan-task token(s)")
        self._send_json(200, body)

    def _handle_fetch_scan_tasks(self, table: str, payload: dict):
        token = payload.get("plan-task", "")
        print(f"\nPOST tasks  table={table!r}  token={token!r}")
        tasks = (TABLE_PLAN_TASK_MAP.get(table) or {}).get(token, [])
        body = {"file-scan-tasks": tasks, "plan-tasks": [], "delete-files": []}
        print(f"  → {len(tasks)} file-scan-task(s)")
        self._send_json(200, body)

    def do_GET(self):
        table, _ = self._extract_table_endpoint()
        print(f"\nGET poll  table={table!r}")
        body = _completed_body(table)
        self._send_json(200, body)

    def do_DELETE(self):
        table, _ = self._extract_table_endpoint()
        print(f"\nDELETE  table={table!r}")
        self.send_response(204)
        self.end_headers()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Mock IRC scan plan server")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument("--async-first", action="store_true")
    args = parser.parse_args()

    ensure_testdata()
    Handler.async_first = args.async_first

    server = HTTPServer(("", args.port), Handler)
    print(f"\nListening on http://localhost:{args.port}")
    print(f"async-first: {args.async_first}")
    print()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
