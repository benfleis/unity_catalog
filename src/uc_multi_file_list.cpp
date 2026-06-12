#include "uc_multi_file_list.hpp"
#include "uc_logging.hpp"

namespace duckdb {

static void AssertNoDeleteFiles(ClientContext &ctx, const UCScanPlanResult &result, const string &source) {
	// TODO: replace with DV hooks
	if (!result.delete_files.empty()) {
		UC_LOG_WARNING(ctx,
		                   "scan-plan: %s returned %zu delete file(s) — not yet supported; "
		                   "results may be incorrect if deletes are present",
		                   source, result.delete_files.size());
		D_ASSERT(result.delete_files.empty());
	}
}

static OpenFileInfo MakeOpenFileInfo(const UCScanPlanDataFile &df) {
	OpenFileInfo info(df.file_path);
	if (df.file_size_in_bytes > 0) {
		auto ext = make_shared_ptr<ExtendedOpenFileInfo>();
		ext->options["file_size"] = Value::UBIGINT((uint64_t)df.file_size_in_bytes);
		info.extended_info = std::move(ext);
	}
	return info;
}

UCMultiFileList::UCMultiFileList(ClientContext &context, const UCScanPlanResult &plan, UCCredentials credentials_p,
                                 string catalog_name_p, string schema_name_p, string table_name_p,
                                 string scan_plan_endpoint_p)
    : LazyMultiFileList(&context), credentials(std::move(credentials_p)), catalog_name(std::move(catalog_name_p)),
      schema_name(std::move(schema_name_p)), table_name(std::move(table_name_p)),
      scan_plan_endpoint(std::move(scan_plan_endpoint_p)) {
	AssertNoDeleteFiles(context, plan, "PlanTableScan");
	// Pre-seed inline file-scan-tasks — available immediately without a fetch round-trip.
	for (auto &task : plan.file_scan_tasks) {
		expanded_files.push_back(MakeOpenFileInfo(task.data_file));
	}
	for (auto &token : plan.plan_tasks) {
		remaining_tokens.push_back(token);
	}
	if (remaining_tokens.empty()) {
		all_files_expanded = true;
	}
}

bool UCMultiFileList::ExpandNextPath() const {
	if (remaining_tokens.empty()) {
		return false;
	}

	string token = std::move(remaining_tokens.front());
	remaining_tokens.erase(remaining_tokens.begin());

	auto &ctx = *context.get_mutable();
	auto result =
	    UCAPI::FetchScanTasks(ctx, catalog_name, schema_name, table_name, token, credentials, scan_plan_endpoint);
	AssertNoDeleteFiles(ctx, result, "FetchScanTasks");
	for (auto &task : result.file_scan_tasks) {
		expanded_files.push_back(MakeOpenFileInfo(task.data_file));
	}
	// Re-queue nested tokens (server may return additional plan-tasks from fetchScanTasks).
	for (auto &new_token : result.plan_tasks) {
		remaining_tokens.push_back(std::move(new_token));
	}

	return !remaining_tokens.empty();
}

// Thread-local staging area for UCScanPlanPushdownFilter → UCMultiFileReaderFactory handoff.
// Set immediately before parquet_fn.bind(); consumed (moved) by the factory on the same thread.
thread_local shared_ptr<UCMultiFileList> tl_uc_file_list;

unique_ptr<MultiFileReader> UCMultiFileReaderFactory(const TableFunction &) {
	D_ASSERT(tl_uc_file_list);
	return make_uniq<UCMultiFileReader>(std::move(tl_uc_file_list));
}

} // namespace duckdb
