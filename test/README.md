# Testing this extension

Tests are **pytest-driven** via the `duckdb-pytest-driver`: each `.test` (SQLLogic) file is run
through the duckdb `unittest` binary, with Python `.py` drivers alongside for declarative
`@requires` provisioning (table fixtures, catalog setup) and managed teardown. The `.test` file
stays the central artifact; the Python is thin glue.

## Layout

- **`oss_local/`** — run against a **local OSS Unity Catalog** server (the ducktest docker image,
  started per session by the `uc_server` fixture). Runs in CI. `-m oss_local` selects the subtree.
  The image (`ghcr.io/benfleis/ducktest-unitycatalog`, `:ci` is the tag CI pulls) is built and
  published from [`scripts/oss_uc_image/`](../scripts/oss_uc_image/README.md) — see that README for the
  build + publish process.
- **`databricks/`** — run against a **live Databricks workspace** (creds via
  `scripts/env_databricks` → `DATABRICKS_*` + `DATABRICKS_WAREHOUSE_ID`). Not in CI; tests skip
  gracefully without creds. Read tables auto-provision from the Delta-artifact defs in
  `databricks/data/`; write tests seed the `id_name` fixture into an isolated cell and tear it down.
- **`fixtures/`** — neutral, backend-agnostic table fixtures (e.g. `id_name`), instantiated per
  backend via the driver's Fixture path. Shared by OSS and Databricks.
- **`databricks/data/`** — Databricks-specific Delta-artifact definitions (schema evolution, column
  mapping, catalog-managed staged/backfilled commits) that aren't portable fixtures; run verbatim
  via `databricks-gen from-sql`.

## Running

```bash
make test                                  # pytest over test/ (OSS runs; databricks skips w/o creds)
env_databricks pytest test/databricks  # the databricks suite, with creds
pytest -m oss_local                        # just the OSS subtree
```

## Coverage today (local OSS)

- delta.yaml v1 read/write on a catalog-managed (CMT) table: `LoadTable` / `UpdateTable`
  (`oss_local/table-cmt/catalog_managed.test`).
- Managed vs non-managed dispatch: `IsCatalogManaged()` selects the v1 path; a MANAGED table without
  the `catalogManaged` feature and EXTERNAL tables take the plain Delta path and do **not** call
  `LoadTable`/`UpdateTable` (asserted via `duckdb_logs()` counts — `oss_local/table-plain/`).
- Backfill watermark advance across DETACH/ATTACH (`set-latest-backfilled-version`).

## Testing gaps / TODO

These exercise code paths that the **file://-backed local OSS server can't reach today**. The shared
unblock is the same: stand up **OSS + S3** (e.g. MinIO) so storage is real object storage, bring a
table to a specific state, then apply a **coordinated out-of-band modification** to force the
condition. Most depend on the **pytest infra** being built up (cf. `lakekeeper/tests/python`) to
orchestrate server state + a side-channel writer between SQL steps. Each below needs pattern work.

- [ ] **Storage-credential vending (managed + external).** `RefreshCredentials`
      (`src/storage/uc_table_set.cpp`) early-returns on `file://`, so neither credential path runs in
      CI: managed via delta/v1 `/credentials` → `storage-credentials[].config` S3 keys, nor external
      via `temporary-table-credentials` → `aws_temp_credentials`. Only Databricks covers them today.
      **Unblock:** OSS + S3 backing. Also lets us cover the *longest-prefix* selection that's still a
      TODO in `GetTableCredentials` (currently takes the first `storage-credentials` entry).

- [ ] **`latest-table-version` omitted → max-commit fallback + incoherent warning.** The fallback and
      `UC_LOG_WARNING` in `UCAPI::LoadTable` (`src/uc_api.cpp`) are unexercised. The `D_ASSERT` there
      intentionally crashes the suite on a contract-violating server (latest-version present but ≠ max
      commit), so the goal is only to exercise the *absent-field* path. **Unblock:** a coordinated
      response shape (mock/proxy, or a server state) that returns non-empty `commits` with no
      `latest-table-version`. Assert read correctness + the warning line via `duckdb_logs()`.

- [ ] **etag optimistic-concurrency conflict (`assert-etag`).** No test triggers a
      `CommitVersionConflictException`. **Unblock:** bring a CMT table to a known etag, then commit
      out-of-band (second writer / API call) so the next `UpdateTable` sends a stale etag; assert the
      conflict surfaces. Also covers the `CommitState` snapshot under genuine read/write interleave.

- [ ] **Externally-written (Spark) staged commits + `max_catalog_version`.** DuckDB's own writes go
      through the backfill path, so the read-via-`max_catalog_version` path for commits that live
      permanently in `_staged_commits/` is never hit locally. **Unblock:** a non-DuckDB writer (Spark,
      or a script that stages a commit without promoting it to `_delta_log/`), then read and confirm
      visibility depends on `max_catalog_version`. This is also the fixture that would actually
      exercise the `latest-table-version` fallback meaningfully (no `_delta_log/` promotion to mask a
      wrong gate).

- [ ] **Backfill resume after an interrupted write.** Leave a commit acknowledged by UC (`UpdateTable`
      succeeded) but un-promoted to `_delta_log/`, then reattach and confirm `BackfillCommits` resumes
      it. **Unblock:** either a coordinated `exit`/kill between `UpdateTable` and the next attach, or
      directly stage the state. Note: for a DuckDB-written commit the next attach backfills it anyway,
      so pair with the Spark fixture above to make the gate observable.

- [ ] **Backfill copy failure.** `BackfillCommits` (`src/storage/uc_table_set.cpp`) stops + warns on a
      failed copy to avoid advancing the watermark past a gap; no test induces a failure. **Unblock:**
      make a `_delta_log/` destination unwritable (perms / read-only mount under S3) and assert the
      warning + that the watermark did not advance.

- [ ] **Concurrency / data races (TSan).** `commit_state` is now `MutexProtected<CommitState>`
      (`src/include/uc_mutex_protected.hpp`) so the etag/backfill pair can't be torn or read unlocked.
      Still uncovered by tests, and the two `TODO(race)` unlocked `internal_attached_database` derefs
      (`GetInternalCatalog` / `InternalCheckpoint`) remain. **Unblock:** a TSan build driving a
      concurrent reader (bind/scan → re-attach) and writer (commit) against one shared attach.
      **Target design** (the lock pass, not piecemeal): widen the `MutexProtected` pattern to also
      cover `is_dirty` and `internal_attached_database` — fold all three into one
      `MutexProtected<AttachState>` and drop the bare `attach_lock`, making unlocked access
      unrepresentable everywhere. Must stay a single protected struct to keep `InternalAttach`'s
      critical section atomic. See the `TODO(locks)` note on `attach_lock` in
      `src/include/storage/uc_table_set.hpp`.
