# Plan: UC Scan Planning API POC

## Context

Unity Catalog tables in the DuckDB UC extension currently scan by attaching an internal Delta
catalog per table, then letting the Delta kernel FFI walk the `_delta_log` to enumerate Parquet
files. This is heavyweight: it requires a full Delta log read for every query even when UC's
server already knows the exact files to scan.

The IRC-compatible Scan Plan API (`POST .../plan`, `GET .../plan/{id}`, `POST .../tasks`) lets
the server hand back the exact file list. This POC wires that path into UC: always try the scan
plan endpoint first, silently fall back to the existing Delta path if it's unavailable or fails.

---

## Architecture

### Current flow

```
UCTableEntry::GetScanFunction
  → table.RefreshCredentials   (injects S3 secret)
  → table.InternalAttach       (attaches internal Delta DB)
  → delta_catalog.LookupEntry  (gets Delta table entry)
  → delta_table.GetScanFunction → DeltaMultiFileList → Delta kernel FFI → Parquet files
```

### New flow (when scan plan endpoint responds)

Filter pushdown is a first-class goal — without it the scan plan API is just a slower way to
enumerate files. The key insight: DuckDB's `pushdown_complex_filter` callback on a custom
`TableFunction` is invoked *after* the optimizer resolves predicates. This happens entirely
within UC — no Delta involvement for the scan plan path.

```
UCTableEntry::GetScanFunction
  → table.RefreshCredentials
  → UnityCatalog::GetScanPlanEndpoint()  (lazy probe; cached after first attempt)
  → if endpoint available:
      return UCScanPlanTableFunction  (custom TableFunction; bind_data holds metadata)
  [Delta path unchanged as fallback]

UCScanPlanTableFunction::pushdown_complex_filter  (called post-optimizer, filters known)
  → SerializeFiltersToIRC(filters) → IRC Expression JSON
  → UCAPI::PlanTableScan(context, ..., filter_json)
      if "submitted": poll FetchPlanningResult (bounded retry)
  → bind parquet_scan with resulting file list; store in bind_data
  → clear `filters` vector  (server consumed them; parquet_scan re-applies row-level)

UCScanPlanTableFunction::init_global / function
  → delegate to parquet_scan using stored bind_data
```

### Cross-extension boundary

UC and Delta are isolated dynamic modules — no shared C++ headers, no direct symbol linkage.
Two side-channel mechanisms exist today (see CLAUDE.md for detail):

1. **Attach options** (batch, upfront) — all data must be known before `ATTACH`; nothing flows
   back. Filter pushdown impossible: `GetScanFunction` is called at bind time before the
   optimizer runs.

2. **Named table-function IPC** (runtime, lazy) — UC registers a named table function; Delta
   discovers it by name at attach time and calls it lazily during scan execution. DataChunk
   row 0 = inputs (incl. `Value::POINTER`), row 1 = outputs. Pattern used by
   `UCDeltaCCV2CommitExecute` / `DeltaTransaction`.

The scan plan path uses **neither** of these: filter pushdown is handled entirely within UC via
DuckDB's own `pushdown_complex_filter` callback on `UCScanPlanTableFunction`. No Delta
involvement for reading scan-planned tables.

### POC shortcuts (all responses fully decoded; these are acting-on deferrals only)

- Delete files decoded but not applied (no DV / equality-delete row filtering)
- Residual filter from `FileScanTask` not explicitly re-applied — parquet_scan handles
  row-level filtering; DuckDB does not re-apply the cleared filter
- Partition values not injected as virtual columns
- `plan_tasks` tokens are fetched via `fetchScanTasks` (Delta IPC path) but only single-token
  responses are handled; multi-token batching not yet implemented
- Credentials still come from existing `RefreshCredentials()` (not `loadCredentials`)
- `SerializeFiltersToIRC` handles common cases only; unsupported expression types fall back to
  a `true` literal (server returns more files; DuckDB parquet reader still filters correctly)

---

## Implementation Steps

### Step 1 — Mock server (`uc/scripts/mock_scan_plan_server.py`)

Minimal Flask server implementing:
- `POST /v1/<prefix>/namespaces/<ns>/tables/<tbl>/plan`
  → logs received `filter` field; returns `{"status":"completed","plan-id":"mock-1","file-scan-tasks":[...],"delete-files":[]}`
- `GET /v1/<prefix>/namespaces/<ns>/tables/<tbl>/plan/<plan_id>`
  → same completed payload (for poll path)
- `DELETE /v1/<prefix>/namespaces/<ns>/tables/<tbl>/plan/<plan_id>` → 204

The `file-scan-tasks` entries point to a small set of local Parquet files (can use any DuckDB
test parquet files from `d/data/` or generate them with DuckDB itself).

Run with: `python uc/scripts/mock_scan_plan_server.py`

---

### Step 2 — Structs and API methods

Fully decode every field from the OpenAPI spec even if some are unused in the POC.

**`uc/src/include/uc_api.hpp`** — add after `UCAPICommitsResult`:

```cpp
struct UCScanPlanDataFile {
    string content;             // "data"
    string file_path;
    string file_format;         // "parquet" | "avro" | "orc"
    int64_t spec_id = 0;
    int64_t file_size_in_bytes = 0;
    int64_t record_count = 0;
    int64_t first_row_id = -1;
    unordered_map<int32_t, int64_t> column_sizes;
    unordered_map<int32_t, int64_t> value_counts;
    unordered_map<int32_t, int64_t> null_value_counts;
    unordered_map<int32_t, int64_t> nan_value_counts;
    string lower_bounds_json;
    string upper_bounds_json;
};

enum class UCScanDeleteFileType { POSITION_DELETES, EQUALITY_DELETES };

struct UCScanDeleteFile {
    UCScanDeleteFileType content;
    string file_path;
    string file_format;
    int64_t file_size_in_bytes = 0;
    int64_t record_count = 0;
    vector<int32_t> equality_ids;   // equality-deletes only
    int64_t content_offset = -1;    // position-deletes only
    int64_t content_size_in_bytes = -1;
};

struct UCScanPlanFileScanTask {
    UCScanPlanDataFile data_file;
    vector<int32_t> delete_file_references; // indices into ScanTasks.delete_files
    string residual_filter_json;            // decoded but not yet re-applied
};

using UCScanPlanTask = string; // opaque token per spec

struct UCScanPlanResult {
    string status;   // "completed" | "submitted" | "failed" | "cancelled"
    string plan_id;
    vector<UCScanDeleteFile>       delete_files;
    vector<UCScanPlanFileScanTask> file_scan_tasks;
    vector<UCScanPlanTask>         plan_tasks;         // fetchScanTasks not called in POC
    vector<pair<string,string>>    storage_credentials; // prefix → config JSON
    string error_message;
    string error_type;
};
```

Add to `class UCAPI`:

```cpp
static UCScanPlanResult PlanTableScan(ClientContext &ctx,
                                      const string &catalog_name,
                                      const string &schema_name,
                                      const string &table_name,
                                      const UCCredentials &credentials,
                                      const string &scan_plan_endpoint,
                                      const string &filter_json = "");
static UCScanPlanResult FetchPlanningResult(ClientContext &ctx,
                                             const string &catalog_name,
                                             const string &schema_name,
                                             const string &table_name,
                                             const string &plan_id,
                                             const UCCredentials &credentials,
                                             const string &scan_plan_endpoint);
```

**`uc/src/uc_api.cpp`** — implement both methods:

`PlanTableScan`:
- URL: `{scan_plan_endpoint}/v1/{catalog_name}/namespaces/{schema_name}/tables/{table_name}/plan`
- Body: `{}` when `filter_json` is empty; `{"filter":<filter_json>}` when present (raw embed —
  `filter_json` is already valid IRC Expression JSON from the serializer)
- Fully parse response per structs above
- Use existing `MakeRequest()` (POST with JSON body)
- If status == `"submitted"`, poll `FetchPlanningResult` up to `MAX_POLL` times:

```cpp
constexpr int MAX_POLL = 20;
constexpr int POLL_SLEEP_MS = 500;
```

`FetchPlanningResult`:
- URL: `{scan_plan_endpoint}/v1/{catalog_name}/namespaces/{schema_name}/tables/{table_name}/plan/{plan_id}`
- GET request; parse same structure

Reuse existing helpers: `MakeRequest`, `TryGetStrFromObject`, `TryGetNumFromObject`, `YYJsonDoc`.

---

### Step 3 — Scan plan endpoint: explicit opt-in option

Scan planning is **opt-in only** — it activates when `scan_plan_endpoint` is set as an ATTACH
option. No auto-detection, no probe state machine.

**`uc/src/include/storage/unity_catalog.hpp`** — add to `UCCredentials`:

```cpp
string scan_plan_endpoint; // set via ATTACH option; empty = scan plan disabled
```

`UnityCatalog::GetScanPlanEndpoint()` simply returns `credentials.scan_plan_endpoint`.

**`uc/src/unity_catalog_extension.cpp`** — read optional `scan_plan_endpoint` attach option
and store in `UCCredentials`. Strip trailing slash.

> **Note:** A probe-and-cache mechanism (set UNAVAILABLE on HTTP 405, skip probe on subsequent
> queries) is a TODO. See TASKS.md. For now, any API failure propagates as an error on that
> query; the next query retries.

---

### Step 4 — Custom TableFunction with filter pushdown

**`uc/src/storage/uc_table_entry.cpp`**

`GetScanFunction` returns a custom `UCScanPlanTableFunction` when a scan plan endpoint is
available. The custom function defers the API call to its `pushdown_complex_filter` callback,
which fires after the optimizer has resolved predicates.

```cpp
struct UCScanPlanBindData : public FunctionData {
    // Pre-pushdown (set at bind time)
    string catalog_name, schema_name, table_name;
    UCCredentials credentials;
    string scan_plan_endpoint;
    // Post-pushdown (filled by pushdown_complex_filter)
    unique_ptr<FunctionData> parquet_bind_data;
    TableFunction            parquet_func;
    bool scan_plan_done = false;

    unique_ptr<FunctionData> Copy() const override { /* shallow ok for POC */ }
    bool Equals(const FunctionData &) const override { return false; }
};
```

`GetScanFunction`:

```cpp
TableFunction UCTableEntry::GetScanFunction(ClientContext &context,
                                             unique_ptr<FunctionData> &bind_data,
                                             const EntryLookupInfo &lookup_info) {
    auto &table_data = *table.table_data;

    auto scan_ep = table.catalog.GetScanPlanEndpoint(context);
    if (!scan_ep.empty()) {
        table.RefreshCredentials(context);
        auto bd = make_uniq<UCScanPlanBindData>();
        bd->catalog_name       = table_data.catalog_name;
        bd->schema_name        = table_data.schema_name;
        bd->table_name         = table_data.name;
        bd->credentials        = table.catalog.credentials;
        bd->scan_plan_endpoint = scan_ep;
        bind_data = std::move(bd);
        return MakeUCScanPlanTableFunction();
    }

    // Existing Delta path (unchanged)
    if (table_data.data_source_format != "DELTA") {
        throw NotImplementedException("Table '%s' is of unsupported format '%s'",
                                       table_data.name, table_data.data_source_format);
    }
    table.RefreshCredentials(context);
    table.InternalAttach(context);
    // ... rest unchanged
}
```

`MakeUCScanPlanTableFunction()` builds a `TableFunction` with:
- `pushdown_complex_filter` set to `UCScanPlanPushdownFilter`
- `init_global` / `init_local` / `function` delegating through `parquet_bind_data`
- `filter_pushdown = true`

`UCScanPlanPushdownFilter`:

```cpp
static void UCScanPlanPushdownFilter(ClientContext &context, LogicalGet &get,
                                      FunctionData *bind_data_p,
                                      vector<unique_ptr<Expression>> &filters) {
    auto &bd = (UCScanPlanBindData &)*bind_data_p;
    try {
        string filter_json = SerializeFiltersToIRC(filters, get);
        auto plan = UCAPI::PlanTableScan(context, bd.catalog_name, bd.schema_name,
                                          bd.table_name, bd.credentials,
                                          bd.scan_plan_endpoint, filter_json);
        if (plan.status == "completed" && !plan.file_scan_tasks.empty()) {
            bd.parquet_func      = GetParquetScanFunc(context);
            bd.parquet_bind_data = BindParquetFiles(context, plan, bd.parquet_func);
            bd.scan_plan_done    = true;
            filters.clear(); // server consumed; parquet_scan re-applies row-level
            return;
        }
    } catch (...) {}
    // On failure: leave filters in place. Note: fallback to Delta is not possible
    // from within the callback once the custom function was committed at bind time.
    // POC behaviour: throw so the user sees the error. Production: pre-register a
    // Delta fallback in bind_data and use it from init_global.
}
```

`BindParquetFiles` (static helper) — collects file paths from `plan.file_scan_tasks`, looks up
`parquet_scan` in the system catalog, calls its bind function, returns the resulting
`FunctionData`.

---

### Step 5 — Filter serialization (`SerializeFiltersToIRC`)

Static function in `uc_table_entry.cpp`:

```cpp
static string SerializeFiltersToIRC(const vector<unique_ptr<Expression>> &filters,
                                     const LogicalGet &get);
```

Walks DuckDB's bound expression tree and produces an IRC `Expression` JSON string. Handled
cases for the POC:

| DuckDB type | IRC output |
|---|---|
| `BoundComparisonExpression` (EQ/NE/LT/LTE/GT/GTE) | `LiteralExpression` |
| `BoundConjunctionExpression` (AND/OR) | `AndOrExpression` |
| `BoundOperatorExpression` (IS NULL / IS NOT NULL) | `UnaryExpression` |
| `BoundColumnRefExpression` | `Reference` (column name from `get.names`) |
| `BoundConstantExpression` | `PrimitiveTypeValue` (numeric / string) |

Unsupported expression types emit `{"type":"true"}` so the sub-expression is omitted from
server-side filtering. Correctness is preserved: parquet_scan still evaluates the full predicate;
only server-side selectivity is reduced for those terms.

If serialization of all filters fails, return `""` → `PlanTableScan` sends no filter → server
returns all files → parquet reader handles everything.

When multiple filters are present, wrap them in `{"type":"and","left":...,"right":...}`.

---

## Files modified / created

| File | Change |
|------|--------|
| `uc/scripts/mock_scan_plan_server.py` | **created** — stdlib mock server (no Flask); logs filter; all four OSS UC tables |
| `uc/src/include/uc_api.hpp` | added structs + method declarations |
| `uc/src/uc_api.cpp` | `PlanTableScan`, `FetchPlanningResult`, `FetchScanTasks`; debug logging on all three calls |
| `uc/src/include/storage/unity_catalog.hpp` | `scan_plan_endpoint` in `UCCredentials`; inline `GetScanPlanEndpoint()` |
| `uc/src/unity_catalog_extension.cpp` | reads `scan_plan_endpoint` attach option |
| `uc/src/storage/uc_table_entry.cpp` | `UCScanPlanBindData`, `MakeUCScanPlanTableFunction`, `UCScanPlanPushdownFilter`, `SerializeFiltersToIRC`, `BindParquetFiles` |
| `delta/src/delta_multi_file_list.cpp` | `InitScanPlanMode`; lazy `GetFileInternal` IPC path; filter pushdown disabled in scan-plan mode |
| `uc/src/functions/uc_scan_plan_fetch_tasks.cpp` | **created** — `UCDeltaScanPlanFetchTasks` IPC function; pops tokens, calls `FetchScanTasks`, returns paths |
| `uc/test/sql/local_oss_unity_catalog/scan_plan.test` | **created** — sqllogictest for all four OSS UC tables + filter pushdown |
| `uc/test/sql/databricks/scan_plan.test` | **created** — Databricks integration test (requires `DATABRICKS_SCAN_PLAN_ENDPOINT`) |

---

## OSS Unity Catalog default tables

Catalog `unity`, schema `default`. Used by the mock server and integration tests.

| Table | Columns | Notes |
|---|---|---|
| `marksheet` | `id INT, name VARCHAR, marks INT` | Managed Delta; 100 rows (id 1–100, marks = id×3) |
| `marksheet_uniform` | `id INT, name VARCHAR, marks INT` | External Delta (Uniform Iceberg); same schema/data as marksheet |
| `numbers` | `as_int INT, as_double DOUBLE` | External Delta |
| `user_countries` | `first_name VARCHAR, age BIGINT, country VARCHAR` | External Delta; partitioned by `country` |

Mock server behaviour (routes by table name in the URL):

| Table | Inline file-scan-tasks | Plan-task tokens |
|---|---|---|
| `marksheet` | 1 (all.parquet, 100 rows) | — |
| `marksheet_uniform` | 1 (all.parquet, 100 rows) | — |
| `numbers` | — | 1 token → part1 + part2 (50 rows each) |
| `user_countries` | 1 (part1, 50 rows) | 1 token → part2 (50 rows) |
| unknown | falls back to marksheet | — |

Files in `uc/data/scan-plan-testdata/<table>/`.

---

## Verification

1. **Mock — happy path** (sqllogictest covers this; manual verification below)
   ```bash
   python uc/scripts/mock_scan_plan_server.py &   # port 8081
   # In DuckDB:
   ATTACH 'unity' AS uc (TYPE UNITY_CATALOG, DEFAULT_SCHEMA 'default',
       scan_plan_endpoint 'http://127.0.0.1:8081');
   SELECT count(*) FROM uc.default.marksheet;  -- 100, via parquet (no Delta kernel)
   ```

2. **Filter pushdown**: IRC Expression appears in DuckDB debug logs.
   ```sql
   CALL enable_logging(level := 'DEBUG');
   SELECT count(*) FROM uc.default.marksheet WHERE marks > 297;
   SELECT message FROM duckdb_logs()
   WHERE message LIKE 'scan-plan POST%filter=%' AND message NOT LIKE '%filter=(none)%';
   ```

3. **Fallback check**: stop mock server; same query should succeed via Delta path (TODO: not yet
   automated — requires separate test setup).

4. **Async path**: `python uc/scripts/mock_scan_plan_server.py --async-first` returns
   `"submitted"` on first POST; verify polling loop fetches `"completed"` on GET (TODO: no
   automated test yet).

5. **Live endpoint**:
   ```sql
   ATTACH 'duckdb_write_testing' AS uc (TYPE UNITY_CATALOG,
       ENDPOINT 'https://$DATABRICKS_HOST',
       TOKEN '...',
       DEFAULT_SCHEMA 'source',
       scan_plan_endpoint 'https://$DATABRICKS_HOST/api/2.1/unity-catalog/iceberg-rest');
   SELECT * FROM uc.source.irc_simple_table;
   ```

---

## Post-POC work

### Current limitation: all-upfront file loading

`pushdown_complex_filter` runs synchronously and collects the complete file list before
`init_global` is called. Two consequences:

- **Inline `file-scan-tasks` with many files** — architecturally fine; just memory pressure
  from holding a large vector. Not a blocker in practice.
- **`plan-tasks` (server-side lazy batching)** — the server returns opaque task tokens instead
  of inline file paths when the result set is too large for a single response. The current
  design cannot support this at all: by the time we could fetch the next batch, the scan is
  already executing.

### Lazy file enumeration via `plan-tasks` — **implemented (POC)**

The POC uses **DataChunk IPC through Delta**: UC registers `__internal_uc_scan_plan_fetch_tasks`;
Delta's lazy `GetFileInternal` discovers and calls it, fetching file paths per plan-task token.
This reuses Delta's existing lazy file machinery without core DuckDB changes.

Current limitation: only single-token responses are handled. Multi-token batching (server
returns multiple `plan-tasks` requiring sequential `fetchScanTasks` calls) is a TODO.

A cleaner long-term path (**`UCMultiFileList`**) is to implement `MultiFileList` entirely
within UC and call `MultiFileFunction<ParquetMultiFileInfo>::MultiFileBindInternal` directly
(the internal bind path that accepts a `shared_ptr<MultiFileList>` — no core change needed).
This unifies greedy and lazy into one path, eliminates the Delta IPC machinery, and allows
per-file metadata from `UCScanPlanDataFile` (`file_size_in_bytes`, column bounds, etc.) to be
injected via `OpenFileInfo::extended_info` — which httpfs already consumes to skip HEAD
requests.

**Cross-extension IPC design note**: when a `Value::POINTER` is already being passed across
the extension boundary, the caller and callee are already coupled to the pointed-to type.
In that case, prefer passing a pointer to a virtual interface and calling methods directly
rather than marshalling inputs/outputs through DataChunk rows. DataChunk IPC is appropriate
only when the callee must remain genuinely agnostic about the caller's types.

### Other post-POC items

- **DV / equality-delete support** — IRC-compatible tables appear mutually exclusive with
  Deletion Vectors (see Open Questions in TASKS.md). If confirmed, DV tables must stay on the
  Delta reader path and are excluded from scan planning entirely.
- **Catalog-managed requirement** — scan plan API may require catalog-managed tables
  (see Open Questions in TASKS.md). Affects which tables are eligible.
- Expand `SerializeFiltersToIRC` to cover `NOT`, `IN`/`NOT IN`, transform terms
- Partition column injection from `FileScanTask` partition values
- Probe-and-cache availability per attach (HTTP 405 → set UNAVAILABLE flag; see TASKS.md)
- Replace `RefreshCredentials` with `loadCredentials?planId=...` for tighter credential scoping
