#pragma once

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
class UCMultiFileList : public LazyMultiFileList {
public:
	UCMultiFileList(ClientContext &context, const UCScanPlanResult &plan, UCCredentials credentials,
	                string catalog_name, string schema_name, string table_name, string scan_plan_endpoint);

protected:
	bool ExpandNextPath() const override;

private:
	mutable vector<string> remaining_tokens;
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
class UCMultiFileReader : public MultiFileReader {
public:
	explicit UCMultiFileReader(shared_ptr<UCMultiFileList> list_p) : file_list(std::move(list_p)) {
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &glob_input) override {
		return file_list;
	}

private:
	shared_ptr<UCMultiFileList> file_list;
};

// Thread-local staging area: set immediately before parquet_fn.bind(), consumed by the factory.
extern thread_local shared_ptr<UCMultiFileList> tl_uc_file_list;

// Factory registered as get_multi_file_reader on a parquet_scan copy.
// Picks up the pre-built list from the thread-local set in UCScanPlanPushdownFilter.
unique_ptr<MultiFileReader> UCMultiFileReaderFactory(const TableFunction &function);

} // namespace duckdb
