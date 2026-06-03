#include "storage/unity_catalog.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_table_entry.hpp"
#include "storage/uc_table_set.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/database.hpp"

#include "uc_api.hpp"
#include "functions/uc_scan_plan_fetch_context.hpp"

#include "duckdb/planner/operator/logical_get.hpp"
#include "uc_irc_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry_retriever.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/table_column.hpp"

namespace duckdb {

// ---------------------------------------------------------------------------
// UCScanPlanBindData
//
// Holds pre-pushdown metadata (set at GetScanFunction time) and the
// post-pushdown parquet delegate state (filled by pushdown_complex_filter).
// ---------------------------------------------------------------------------

struct UCScanPlanBindData : public FunctionData {
	// Pre-pushdown: table identity and credentials (set at GetScanFunction time)
	string catalog_name;
	string schema_name;
	string table_name;
	string storage_location;  // passed as path arg to delta_scan in lazy mode
	vector<string> col_names; // table schema column names (for lazy delta path)
	vector<string> col_types; // table schema type strings  (for lazy delta path)
	UCCredentials credentials;
	string scan_plan_endpoint;

	// Post-pushdown: which delegate path is active
	bool scan_plan_done = false;
	bool use_lazy_delta = false;

	// Parquet delegate (greedy: all file-scan-tasks, no plan-tasks)
	unique_ptr<FunctionData>     parquet_bind_data;
	table_function_init_global_t parquet_init_global = nullptr;
	table_function_init_local_t  parquet_init_local  = nullptr;
	table_function_t             parquet_scan_fn      = nullptr;

	// Lazy delta delegate (plan-tasks present; Delta IPC fetches files on demand)
	shared_ptr<UCScanPlanFetchContext> fetch_ctx; // keeps context alive for Delta's raw ptr
	unique_ptr<FunctionData>     delta_bind_data;
	table_function_init_global_t delta_init_global = nullptr;
	table_function_init_local_t  delta_init_local  = nullptr;
	table_function_t             delta_scan_fn      = nullptr;

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("UCScanPlanBindData::Copy");
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};


// ---------------------------------------------------------------------------
// BindParquetFiles
//
// Looks up parquet_scan from the system catalog and calls its bind function
// with the file list from the scan plan result.
// ---------------------------------------------------------------------------

static unique_ptr<FunctionData> BindParquetFiles(ClientContext &context, const UCScanPlanResult &plan,
                                                  TableFunction &parquet_func) {
	vector<Value> file_values;
	file_values.reserve(plan.file_scan_tasks.size());
	for (auto &task : plan.file_scan_tasks) {
		file_values.push_back(Value(task.data_file.file_path));
	}

	vector<Value> inputs = {Value::LIST(LogicalType::VARCHAR, std::move(file_values))};
	named_parameter_map_t named_params;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	TableFunctionRef dummy_ref;
	TableFunctionBindInput bind_input(inputs, named_params, input_table_types, input_table_names,
	                                  nullptr, nullptr, parquet_func, dummy_ref);

	vector<LogicalType> return_types;
	vector<string> return_names;
	return parquet_func.bind(context, bind_input, return_types, return_names);
}

// ---------------------------------------------------------------------------
// UCScanPlanTableFunction callbacks
// ---------------------------------------------------------------------------

// pushdown_complex_filter: called by DuckDB's FilterPushdown optimizer (even
// with an empty filter list, so it also handles full-table scans).  Serializes
// filters → IRC JSON, calls PlanTableScan, then binds either parquet_scan (when
// only inline file-scan-tasks are returned) or delta_scan in scan-plan IPC mode
// (when the server also returns plan-task tokens for lazy streaming).
static void UCScanPlanPushdownFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                     vector<unique_ptr<Expression>> &filters) {
	auto &bd = reinterpret_cast<UCScanPlanBindData &>(*bind_data_p);
	try {
		string filter_json = SerializeFiltersToIRC(filters, get);
		auto plan = UCAPI::PlanTableScan(context, bd.catalog_name, bd.schema_name, bd.table_name,
		                                 bd.credentials, bd.scan_plan_endpoint, filter_json);

		if (plan.status == UCScanPlanStatus::COMPLETED) {
			if (plan.plan_tasks.empty()) {
				// ---- Greedy path: all files already inline ----
				auto &sys_cat   = Catalog::GetSystemCatalog(context);
				auto &pq_entry  = sys_cat.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "parquet_scan");
				auto parquet_fn = pq_entry.functions.GetFunctionByArguments(context,
				                      {LogicalType::LIST(LogicalType::VARCHAR)});

				bd.parquet_bind_data   = BindParquetFiles(context, plan, parquet_fn);
				// Populate virtual_columns (used for count(*) and other virtual-column scans).
				if (parquet_fn.get_virtual_columns) {
					parquet_fn.get_virtual_columns(context, bd.parquet_bind_data.get());
				}
				bd.parquet_init_global = parquet_fn.init_global;
				bd.parquet_init_local  = parquet_fn.init_local;
				bd.parquet_scan_fn     = parquet_fn.function;
				bd.use_lazy_delta      = false;
				bd.scan_plan_done      = true;
				// Leave filters intact: DuckDB turns them into TableFilters for
				// parquet row-group pruning + a LogicalFilter for row-level correctness.
				return;
			}

			// ---- Lazy path: server returned plan-task tokens ----
			// Build a UCScanPlanFetchContext that Delta's IPC hook will pop tokens from.
			auto fetch_ctx = make_shared_ptr<UCScanPlanFetchContext>();
			fetch_ctx->catalog_name      = bd.catalog_name;
			fetch_ctx->schema_name       = bd.schema_name;
			fetch_ctx->table_name        = bd.table_name;
			fetch_ctx->credentials       = bd.credentials;
			fetch_ctx->scan_plan_endpoint = bd.scan_plan_endpoint;
			for (auto &token : plan.plan_tasks) {
				fetch_ctx->remaining_tokens.push_back(token);
			}
			uint64_t ctx_ptr = reinterpret_cast<uint64_t>(fetch_ctx.get());

			// Inline file paths (already resolved)
			vector<Value> inline_path_vals;
			for (auto &task : plan.file_scan_tasks) {
				inline_path_vals.push_back(Value(task.data_file.file_path));
			}

			// Column schema values
			vector<Value> col_name_vals, col_type_vals;
			for (auto &n : bd.col_names) {
				col_name_vals.push_back(Value(n));
			}
			for (auto &t : bd.col_types) {
				col_type_vals.push_back(Value(t));
			}

			// Look up delta_scan from the system catalog (registered by the delta extension)
			auto &sys_cat = Catalog::GetSystemCatalog(context);
			auto &delta_fn_entry = sys_cat.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "delta_scan");
			auto delta_fn = delta_fn_entry.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

			// Bind delta_scan in scan-plan mode
			string scan_path = bd.storage_location.empty() ? "uc://scan_plan_mode" : bd.storage_location;
			vector<Value> inputs = {Value(scan_path)};
			named_parameter_map_t named_params;
			named_params["scan_plan_context"]        = Value::UBIGINT(ctx_ptr);
			named_params["scan_plan_col_names"]      = Value::LIST(LogicalType::VARCHAR, col_name_vals);
			named_params["scan_plan_col_types"]      = Value::LIST(LogicalType::VARCHAR, col_type_vals);
			named_params["scan_plan_inline_files"]   = Value::LIST(LogicalType::VARCHAR, inline_path_vals);
			named_params["scan_plan_catalog_name"]   = Value(bd.catalog_name);
			named_params["scan_plan_schema_name"]    = Value(bd.schema_name);
			named_params["pushdown_partition_info"]  = Value::BOOLEAN(false);

			vector<LogicalType> input_table_types;
			vector<string> input_table_names;
			TableFunctionRef dummy_ref;
			vector<LogicalType> return_types;
			vector<string> return_names;
			TableFunctionBindInput bind_input(inputs, named_params, input_table_types, input_table_names,
			                                  nullptr, nullptr, delta_fn, dummy_ref);

			bd.delta_bind_data   = delta_fn.bind(context, bind_input, return_types, return_names);
			// Populate virtual_columns so count(*) and similar scans work through our wrapper.
			if (delta_fn.get_virtual_columns) {
				delta_fn.get_virtual_columns(context, bd.delta_bind_data.get());
			}
			bd.delta_init_global = delta_fn.init_global;
			bd.delta_init_local  = delta_fn.init_local;
			bd.delta_scan_fn     = delta_fn.function;
			bd.fetch_ctx         = fetch_ctx;
			bd.use_lazy_delta    = true;
			bd.scan_plan_done    = true;
			// Filters already sent to server; Delta's ComplexFilterPushdown/DynamicFilterPushdown
			// return nullptr in scan-plan mode so they are not re-sent.  DuckDB still applies
			// them locally via the LogicalFilter it installs.
			return;
		}
	} catch (...) {
	}

	// TODO: distinguish "feature not available for this caller" from transient errors.
	// HTTP 405 confirmed as "not enabled" status from live endpoint.
	// On a feature-unavailable response, set a per-UnityCatalog atomic flag
	// (AVAILABLE/UNAVAILABLE, checked in GetScanPlanEndpoint) so all subsequent queries on
	// this attach silently fall back to the Delta path without retrying.  Transient errors
	// (5xx, network) must NOT set UNAVAILABLE — propagate per-query and allow retry.
	// Granularity: per-ATTACH (per UnityCatalog instance) — availability is per-caller.
	throw IOException("UC scan plan API call failed for table '%s'", bd.table_name);
}

// Advertise virtual columns so DuckDB uses COLUMN_IDENTIFIER_EMPTY (not ROW_ID) for
// count(*).  Parquet and delta both understand EMPTY; neither handles ROW_ID.
static virtual_column_map_t UCScanPlanGetVirtualColumns(ClientContext &, optional_ptr<FunctionData>) {
	virtual_column_map_t result;
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME,
	                         TableColumn("filename", LogicalType::VARCHAR)));
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX,
	                         TableColumn("file_index", LogicalType::UBIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

static unique_ptr<GlobalTableFunctionState> UCScanPlanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bd = reinterpret_cast<const UCScanPlanBindData &>(*input.bind_data);
	D_ASSERT(bd.scan_plan_done);
	if (bd.use_lazy_delta) {
		TableFunctionInitInput delta_input(bd.delta_bind_data.get(), input.column_ids, input.projection_ids, input.filters);
		return bd.delta_init_global(context, delta_input);
	}
	TableFunctionInitInput parquet_input(bd.parquet_bind_data.get(), input.column_ids, input.projection_ids, input.filters);
	return bd.parquet_init_global(context, parquet_input);
}

static unique_ptr<LocalTableFunctionState> UCScanPlanInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
	auto &bd = reinterpret_cast<const UCScanPlanBindData &>(*input.bind_data);
	D_ASSERT(bd.scan_plan_done);
	if (bd.use_lazy_delta) {
		TableFunctionInitInput delta_input(bd.delta_bind_data.get(), input.column_ids, input.projection_ids, input.filters);
		return bd.delta_init_local(context, delta_input, global_state);
	}
	TableFunctionInitInput parquet_input(bd.parquet_bind_data.get(), input.column_ids, input.projection_ids, input.filters);
	return bd.parquet_init_local(context, parquet_input, global_state);
}

static void UCScanPlanScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bd = reinterpret_cast<const UCScanPlanBindData &>(*data.bind_data);
	D_ASSERT(bd.scan_plan_done);
	if (bd.use_lazy_delta) {
		TableFunctionInput delta_input(bd.delta_bind_data.get(), data.local_state, data.global_state);
		bd.delta_scan_fn(context, delta_input, output);
		return;
	}
	TableFunctionInput parquet_input(bd.parquet_bind_data.get(), data.local_state, data.global_state);
	bd.parquet_scan_fn(context, parquet_input, output);
}

static TableFunction MakeUCScanPlanTableFunction() {
	TableFunction func("uc_scan_plan", {}, nullptr);
	func.pushdown_complex_filter = UCScanPlanPushdownFilter;
	func.init_global              = UCScanPlanInitGlobal;
	func.init_local               = UCScanPlanInitLocal;
	func.function                 = UCScanPlanScan;
	func.get_virtual_columns      = UCScanPlanGetVirtualColumns;
	func.filter_pushdown          = true;
	func.projection_pushdown      = true;
	return func;
}

// ---------------------------------------------------------------------------
// UCTableEntry
// ---------------------------------------------------------------------------

UCTableEntry::UCTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, TableInformation &table, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info), table(table) {
	this->internal = false;
}

unique_ptr<BaseStatistics> UCTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void UCTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                         ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

TableFunction UCTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("UCTableEntry::GetScanFunction called without entry lookup info");
}

TableFunction UCTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                            const EntryLookupInfo &lookup_info) {
	auto &table_data = table.table_data;
	D_ASSERT(table_data);

	// --- Scan plan path (try first when a scan plan endpoint is configured) ---
	auto scan_ep = table.catalog.GetScanPlanEndpoint();
	if (!scan_ep.empty()) {
		table.RefreshCredentials(context);
		auto bd                = make_uniq<UCScanPlanBindData>();
		bd->catalog_name       = table_data->catalog_name;
		bd->schema_name        = table_data->schema_name;
		bd->table_name         = table_data->name;
		bd->storage_location   = table_data->storage_location;
		bd->credentials        = table.catalog.credentials;
		bd->scan_plan_endpoint = scan_ep;
		for (auto &col : table_data->columns) {
			bd->col_names.push_back(col.name);
			bd->col_types.push_back(col.type_text);
		}
		bind_data = std::move(bd);
		return MakeUCScanPlanTableFunction();
	}

	// --- Delta path (unchanged fallback) ---
	if (table_data->data_source_format != "DELTA") {
		throw NotImplementedException("Table '%s' is of unsupported format '%s', ", table_data->name,
		                              table_data->data_source_format);
	}

	table.RefreshCredentials(context);
	table.InternalAttach(context);

	auto &delta_catalog = *table.GetInternalCatalog();
	auto &schema        = delta_catalog.GetSchema(context, DEFAULT_SCHEMA);
	auto transaction    = schema.GetCatalogTransaction(context);
	auto table_entry    = schema.LookupEntry(transaction, lookup_info);
	D_ASSERT(table_entry);

	auto &delta_table = table_entry->Cast<TableCatalogEntry>();
	return delta_table.GetScanFunction(context, bind_data, lookup_info);
}

virtual_column_map_t UCTableEntry::GetVirtualColumns() const {
	//! FIXME: requires changes in core to be able to delegate this
	return TableCatalogEntry::GetVirtualColumns();
}

vector<column_t> UCTableEntry::GetRowIdColumns() const {
	//! FIXME: requires changes in core to be able to delegate this
	return TableCatalogEntry::GetRowIdColumns();
}

TableStorageInfo UCTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

} // namespace duckdb
