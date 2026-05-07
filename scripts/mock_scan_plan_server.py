#!/usr/bin/env python3
"""
Mock IRC scan plan server for POC testing.

Implements the three scan-planning endpoints with canned responses.
Logs the received filter JSON on every POST so the serialization can be eyeballed.

Startup: generates two small Parquet test files in testdata/ (requires duckdb Python package).

Usage:
    python tools/mock_scan_plan_server.py [--port 8081] [--async-first]

    --async-first   First POST returns "submitted"; the subsequent GET returns "completed".
                    Use this to exercise the polling loop.
"""

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

TESTDATA_DIR = Path(__file__).parent.parent / "data" / "scan-plan-testdata"

FILE_SCAN_TASKS = []  # populated by ensure_testdata()


def ensure_testdata():
    import duckdb

    TESTDATA_DIR.mkdir(parents=True, exist_ok=True)
    part1 = TESTDATA_DIR / "part1.parquet"
    part2 = TESTDATA_DIR / "part2.parquet"

    if not part1.exists() or not part2.exists():
        con = duckdb.connect()
        # Schema matches OSS UC default table unity.default.marksheet (id INT, name STRING, marks INT)
        con.execute(
            f"COPY (SELECT i::INTEGER AS id, 'name_' || i::VARCHAR AS name, (i * 3)::INTEGER AS marks "
            f"FROM range(1, 51) t(i)) TO '{part1}' (FORMAT parquet)"
        )
        con.execute(
            f"COPY (SELECT i::INTEGER AS id, 'name_' || i::VARCHAR AS name, (i * 3)::INTEGER AS marks "
            f"FROM range(51, 101) t(i)) TO '{part2}' (FORMAT parquet)"
        )
        con.close()
        print(f"Generated {part1} and {part2}")

    def make_task(path):
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

    FILE_SCAN_TASKS.clear()
    FILE_SCAN_TASKS.extend([make_task(part1), make_task(part2)])
    print(f"Serving {len(FILE_SCAN_TASKS)} file-scan-tasks from {TESTDATA_DIR}")


# ---------------------------------------------------------------------------
# Canned responses
# ---------------------------------------------------------------------------

COMPLETED_BODY = {
    "status": "completed",
    "plan-id": "mock-1",
    "file-scan-tasks": FILE_SCAN_TASKS,  # reference; populated before server starts
    "delete-files": [],
    "plan-tasks": [],
}

SUBMITTED_BODY = {
    "status": "submitted",
    "plan-id": "mock-async-1",
}


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    async_first = False  # set by main()
    _first_post_done = False  # tracks whether we've served the "submitted" response

    def log_message(self, fmt, *args):  # suppress default access log; we log ourselves
        pass

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _send_json(self, code, body):
        data = json.dumps(body, indent=2).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        raw = self._read_body()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {}

        filter_val = payload.get("filter")
        print(f"\nPOST {self.path}")
        if filter_val:
            print(f"  filter: {json.dumps(filter_val)}")
        else:
            print("  filter: (none)")

        if Handler.async_first and not Handler._first_post_done:
            Handler._first_post_done = True
            print("  → submitted (async-first mode)")
            self._send_json(200, SUBMITTED_BODY)
        else:
            print(f"  → completed ({len(FILE_SCAN_TASKS)} tasks)")
            self._send_json(200, COMPLETED_BODY)

    def do_GET(self):
        print(f"\nGET {self.path}")
        print(f"  → completed ({len(FILE_SCAN_TASKS)} tasks)")
        self._send_json(200, COMPLETED_BODY)

    def do_DELETE(self):
        print(f"\nDELETE {self.path}")
        self.send_response(204)
        self.end_headers()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Mock IRC scan plan server")
    parser.add_argument("--port", type=int, default=8081)
    parser.add_argument(
        "--async-first",
        action="store_true",
        help="Return 'submitted' on first POST; 'completed' on subsequent GET",
    )
    args = parser.parse_args()

    ensure_testdata()

    Handler.async_first = args.async_first

    server = HTTPServer(("", args.port), Handler)
    print(f"\nMock scan plan server listening on http://localhost:{args.port}")
    print(f"async-first mode: {args.async_first}")
    print()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
