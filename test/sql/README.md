# SQL tests

Two suites:

- `local_oss_unity_catalog/` — run against a **local OSS Unity Catalog** server (gated by
  `require-env UC_TEST_SERVER_RUNNING`). Fixtures are seeded by `scripts/oss_uc_local/seed_tables`
  (the `duck` catalog: `duck.{managed,external}.id_name_{ro,rw}`). These run in CI.
- `databricks/` — run against a live Databricks workspace (gated by `DATABRICKS_*` env). Not in CI;
  see `databricks/README.md`.

Run: `make test` (or `make test_debug`).

## Coverage today (local OSS)

- delta.yaml v1 read/write on a catalog-managed (CMT) table: `LoadTable` / `UpdateTable`
  (`managed_catalog.test`).
- Managed vs non-managed dispatch: `IsCatalogManaged()` selects the v1 path; `marksheet` (MANAGED
  but no `catalogManaged` feature) and `duck.external.*` (EXTERNAL) take the plain Delta path and do
  **not** call `LoadTable`/`UpdateTable` (asserted via `duckdb_logs()` counts).
- Backfill watermark advance across DETACH/ATTACH (`set-latest-backfilled-version`).

## Testing gaps / TODO

These exercise code paths that the **file://-backed local OSS server can't reach today**. The shared
unblock is the same: stand up **OSS + S3** (e.g. MinIO) so storage is real object storage, bring a
table to a specific state, then apply a **coordinated out-of-band modification** to force the
condition. Most of these depend on the **pytest infra** being built up (cf. `lakekeeper/tests/python`)
to orchestrate server state + a side-channel writer between SQL steps. Each below needs pattern work.

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
