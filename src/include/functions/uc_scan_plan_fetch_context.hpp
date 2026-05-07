#pragma once

#include <mutex>
#include "uc_api.hpp"
#include "storage/unity_catalog.hpp"

namespace duckdb {

// Heap-allocated context passed via Value::POINTER from UC to Delta and back.
// Owned by UCScanPlanBindData; lifetime spans the entire scan.
// Delta passes the pointer through the DataChunk IPC channel;
// __internal_uc_scan_plan_fetch_tasks pops one token per call and returns the file paths.
struct UCScanPlanFetchContext {
	string catalog_name;
	string schema_name;
	string table_name;
	UCCredentials credentials;
	string scan_plan_endpoint;
	vector<UCScanPlanTask> remaining_tokens; // consumed front-to-back
	mutex mtx;                               // protects remaining_tokens under parallel scan
};

} // namespace duckdb
