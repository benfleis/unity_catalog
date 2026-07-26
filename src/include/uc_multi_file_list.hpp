#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "uc_api.hpp"
#include "storage/unity_catalog.hpp"

namespace duckdb {

// Lazy MultiFileList driven by a UC scan plan result.
//
// Inline file-scan-tasks are pre-seeded into expanded_files at construction; plan-task
// tokens are exchanged lazily via UCAPI::FetchScanTasks as DuckDB requests each file.
// OpenFileInfo::extended_info is populated with file_size so we can skip HEAD requests.
// Nested plan-tasks returned by fetchScanTasks are re-queued and consumed on the next miss.
//
// Each data file's delete_file_references are resolved (against that response's OWN
// delete_files array — the indices are response-scoped, not global) into concrete
// UCScanDeleteFile records at the same time the file is added to expanded_files, so
// file_deletes[i] always lines up with expanded_files[i]. See uc_multi_file_list.cpp's
// BuildUCDeleteFilter for how these get turned into an actual DeleteFilter.
class UCMultiFileList : public LazyMultiFileList {
public:
	UCMultiFileList(ClientContext &context, const UCScanPlanResult &plan, UCCredentials credentials,
	                string catalog_name, string schema_name, string table_name, string scan_plan_endpoint);

public:
	//! The delete files (already resolved from delete_file_references) for expanded_files[file_idx].
	//! Empty if that file has no deletes. Only valid for an already-expanded index.
	const vector<UCScanDeleteFile> &GetDeleteFilesForFile(idx_t file_idx) const;
	//! expanded_files[file_idx].path — the literal scan-plan data_file.file_path, exactly as the
	//! server sent it. Used (not BaseFileReader::GetFileName()) when cross-referencing a
	//! positional delete file's own file_path column, so a match can't silently fail to fire if
	//! DuckDB's parquet reader ever normalizes/resolves the path differently than the server's
	//! literal string.
	const string &GetDataFilePath(idx_t file_idx) const;

protected:
	bool ExpandNextPath() const override;

private:
	//! Append `task`'s data file to expanded_files and its resolved deletes to file_deletes
	//! (delete_file_references are indices into `all_delete_files`, the SAME response's own
	//! delete_files array — must be resolved here, before that array goes out of scope).
	void AddFileScanTask(const UCScanPlanFileScanTask &task, const vector<UCScanDeleteFile> &all_delete_files) const;

private:
	mutable vector<string> remaining_tokens;
	//! Parallel to expanded_files (LazyMultiFileList); file_deletes[i] is expanded_files[i]'s deletes.
	mutable vector<vector<UCScanDeleteFile>> file_deletes;
	UCCredentials credentials;
	string catalog_name;
	string schema_name;
	string table_name;

	// TODO: remove once gating/automation to IRC API in place
	string scan_plan_endpoint;
};

// MultiFileReader that ignores path-based glob expansion and returns a pre-built UCMultiFileList.
// Used with get_multi_file_reader on a copy of parquet_scan to let parquet's own bind flow run
// (schema detection, options, etc.) while supplying our scan-plan-derived file list.
//
// FinalizeBind additionally wires up each file's DeleteFilter (positional / deletion-vector deletes;
// see BuildUCDeleteFilter) once DuckDB has resolved which reader instance backs which file index —
// mirrors the pattern the vendored delta extension uses for its own deletion vectors
// (delta_multi_file_reader.cpp's DeltaMultiFileReader::FinalizeBind).
class UCMultiFileReader : public MultiFileReader {
public:
	explicit UCMultiFileReader(shared_ptr<UCMultiFileList> list_p) : file_list(std::move(list_p)) {
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override {
		return file_list;
	}

	void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
	                  const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
	                  const vector<ColumnIndex> &global_column_ids, ClientContext &context,
	                  optional_ptr<MultiFileReaderGlobalState> global_state) override;

private:
	shared_ptr<UCMultiFileList> file_list;
};

// Thread-local staging area: set immediately before parquet_fn.bind(), consumed by the factory.
extern thread_local shared_ptr<UCMultiFileList> tl_uc_file_list;

// Factory registered as get_multi_file_reader on a parquet_scan copy.
// Picks up the pre-built list from the thread-local set in UCScanPlanPushdownFilter.
unique_ptr<MultiFileReader> UCMultiFileReaderFactory(const TableFunction &function);

// Build the DeleteFilter for one data file from its resolved delete files, or nullptr if it has
// none. Dispatches per UCScanDeleteFile::content:
//   POSITION_DELETES, content_offset < 0  -> a plain parquet/avro/orc (file_path, pos) delete
//     file (Iceberg v2); read via a nested parquet_scan bind/scan and filtered to rows whose
//     file_path matches `data_file_path` (one delete file can reference several data files).
//     avro/orc position-delete files are not implemented (parquet is what UC's Delta-via-UniForm
//     path actually produces) and raise NotImplementedException, same as the cases below.
//   POSITION_DELETES, content_offset >= 0 -> an Iceberg v3 deletion vector: a
//     `deletion-vector-v1` puffin blob at that byte range. Fully supported (uc_puffin.hpp).
//   EQUALITY_DELETES -> NOT supported. Applying these correctly needs an Iceberg field-id ->
//     DuckDB output-column mapping that nothing in this extension currently resolves (UC's own
//     scan never fetches column field-ids at all, only the parquet-embedded ones on the DELETE
//     file itself would be available) — raises NotImplementedException naming the gap rather
//     than silently matching by column name, which would be wrong under a renamed column.
unique_ptr<DeleteFilter> BuildUCDeleteFilter(ClientContext &context, const string &data_file_path,
                                             const vector<UCScanDeleteFile> &deletes);

} // namespace duckdb
