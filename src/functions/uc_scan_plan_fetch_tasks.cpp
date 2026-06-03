#include "functions/uc_table_functions.hpp"
#include "functions/uc_scan_plan_fetch_context.hpp"
#include "uc_api.hpp"

namespace duckdb {

static unique_ptr<GlobalTableFunctionState> UCDeltaScanPlanFetchTasksInit(ClientContext &, TableFunctionInitInput &) {
	return nullptr;
}

static unique_ptr<FunctionData> UCDeltaScanPlanFetchTasksBind(ClientContext &, TableFunctionBindInput &,
                                                              vector<LogicalType> &, vector<string> &) {
	throw InternalException(
	    "__internal_uc_scan_plan_fetch_tasks is only for internal use and should not be called directly");
}

// Called by Delta's MultiFileList (scan-plan mode) to fetch the next batch of file paths.
//
// DataChunk layout (Delta creates and owns the chunk):
//   col 0, row 0  — input:  Value::POINTER to UCScanPlanFetchContext
//   col 1, row 0  — output: Value::LIST(VARCHAR) file paths for this batch
//                           (empty list signals no more files)
void UCDeltaScanPlanFetchTasksExecute(ClientContext &context, TableFunctionInput &, DataChunk &output) {
	auto *fetch_ctx = reinterpret_cast<UCScanPlanFetchContext *>(output.GetValue(0, 0).GetPointer());

	lock_guard<mutex> lock(fetch_ctx->mtx);

	vector<Value> paths;
	if (!fetch_ctx->remaining_tokens.empty()) {
		string token = std::move(fetch_ctx->remaining_tokens.front());
		fetch_ctx->remaining_tokens.erase(fetch_ctx->remaining_tokens.begin());

		auto result =
		    UCAPI::FetchScanTasks(context, fetch_ctx->catalog_name, fetch_ctx->schema_name, fetch_ctx->table_name,
		                          token, fetch_ctx->credentials, fetch_ctx->scan_plan_endpoint);
		for (auto &task : result.file_scan_tasks) {
			paths.emplace_back(task.data_file.file_path);
		}
		for (auto &new_token : result.plan_tasks) {
			fetch_ctx->remaining_tokens.push_back(std::move(new_token));
		}
	}
	// Empty paths → no more files; Delta stops calling.

	output.SetValue(1, 0, Value::LIST(LogicalType::VARCHAR, paths));
	output.SetCardinality(1);
}

UCDeltaScanPlanFetchTasks::UCDeltaScanPlanFetchTasks()
    : TableFunction("__internal_uc_scan_plan_fetch_tasks", {LogicalType::POINTER}, UCDeltaScanPlanFetchTasksExecute,
                    UCDeltaScanPlanFetchTasksBind, UCDeltaScanPlanFetchTasksInit) {
}

} // namespace duckdb
