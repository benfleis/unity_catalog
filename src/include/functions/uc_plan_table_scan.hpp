#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

// __internal_uc_plan_table_scan(endpoint, catalog, schema, table, token [, filter => <json>])
//   -> table(status VARCHAR, plan_id VARCHAR, n_files BIGINT, n_delete_files BIGINT, n_plan_tasks BIGINT)
//
// Drives UCAPI::PlanTableScan directly (POST /plan + the submitted-status poll) against `endpoint`,
// bypassing catalog attach. INTERNAL: exposes the IRC scan-plan request/poll/cancel path for
// testing against a mock server (test/.../test_irc_api_retry.py) and for inspection. Not a stable
// or public API — the `__internal_` prefix is deliberate.
class UCInternalPlanTableScanFunction : public TableFunction {
public:
	UCInternalPlanTableScanFunction();
};

} // namespace duckdb
