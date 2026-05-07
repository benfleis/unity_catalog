#include "storage/unity_catalog.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_table_entry.hpp"
#include "storage/uc_table_set.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/database.hpp"

#include "uc_api.hpp"

// For IRC filter serialization and scan planning
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

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
	UCCredentials credentials;
	string scan_plan_endpoint;
	UnityCatalog *unity_catalog = nullptr; // raw ptr; used to mark endpoint unavailable on failure

	// Post-pushdown: parquet delegate (filled by UCScanPlanPushdownFilter)
	bool scan_plan_done = false;
	unique_ptr<FunctionData>     parquet_bind_data;
	table_function_init_global_t parquet_init_global = nullptr;
	table_function_init_local_t  parquet_init_local  = nullptr;
	table_function_t             parquet_scan_fn      = nullptr;

	unique_ptr<FunctionData> Copy() const override {
		throw NotImplementedException("UCScanPlanBindData::Copy");
	}
	bool Equals(const FunctionData &) const override {
		return false;
	}
};

// ---------------------------------------------------------------------------
// SerializeFiltersToIRC (Step 5)
//
// Walks DuckDB's bound expression tree and produces an IRC Expression JSON
// string.  Unrecognised nodes emit {"type":"true"} so the server returns
// the full file list; parquet_scan still applies the full predicate locally.
// ---------------------------------------------------------------------------

static string ValueToIRCJson(const Value &val) {
	switch (val.type().id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return to_string(val.GetValue<int64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return to_string(val.GetValue<double>());
	case LogicalTypeId::BOOLEAN:
		return val.GetValue<bool>() ? "true" : "false";
	case LogicalTypeId::VARCHAR: {
		string s = val.ToString();
		string result = "\"";
		for (char c : s) {
			if (c == '"') {
				result += "\\\"";
			} else if (c == '\\') {
				result += "\\\\";
			} else if (c == '\n') {
				result += "\\n";
			} else if (c == '\r') {
				result += "\\r";
			} else if (c == '\t') {
				result += "\\t";
			} else {
				result += c;
			}
		}
		result += "\"";
		return result;
	}
	default:
		return "";
	}
}

// Forward declaration for recursion.
static string ExprToIRCJson(const Expression &expr, const LogicalGet &get);

static string ExprToIRCJson(const Expression &expr, const LogicalGet &get) {
	switch (expr.GetExpressionClass()) {

	// BoundComparisonExpression: EQ / NE / LT / LTE / GT / GTE
	case ExpressionClass::BOUND_COMPARISON: {
		auto &cmp = reinterpret_cast<const BoundComparisonExpression &>(expr);

		const BoundColumnRefExpression *col = nullptr;
		const BoundConstantExpression  *con = nullptr;
		bool flipped = false;

		if (cmp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    cmp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col = reinterpret_cast<const BoundColumnRefExpression *>(cmp.left.get());
			con = reinterpret_cast<const BoundConstantExpression *>(cmp.right.get());
		} else if (cmp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		           cmp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col     = reinterpret_cast<const BoundColumnRefExpression *>(cmp.right.get());
			con     = reinterpret_cast<const BoundConstantExpression *>(cmp.left.get());
			flipped = true;
		} else {
			return "{\"type\":\"true\"}";
		}

		ExpressionType effective_type = flipped ? FlipComparisonExpression(expr.type) : expr.type;
		const char *irc_type = nullptr;
		switch (effective_type) {
		case ExpressionType::COMPARE_EQUAL:
			irc_type = "eq";    break;
		case ExpressionType::COMPARE_NOTEQUAL:
			irc_type = "not-eq"; break;
		case ExpressionType::COMPARE_LESSTHAN:
			irc_type = "lt";    break;
		case ExpressionType::COMPARE_GREATERTHAN:
			irc_type = "gt";    break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			irc_type = "lt-eq"; break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			irc_type = "gt-eq"; break;
		default:
			return "{\"type\":\"true\"}";
		}

		idx_t col_idx = col->binding.column_index;
		if (col_idx >= get.names.size()) {
			return "{\"type\":\"true\"}";
		}
		string val_json = ValueToIRCJson(con->value);
		if (val_json.empty()) {
			return "{\"type\":\"true\"}";
		}

		return string("{\"type\":\"") + irc_type + "\",\"term\":\"" + get.names[col_idx] +
		       "\",\"value\":" + val_json + "}";
	}

	// BoundConjunctionExpression: AND / OR
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj     = reinterpret_cast<const BoundConjunctionExpression &>(expr);
		const char *irc_type = (expr.type == ExpressionType::CONJUNCTION_AND) ? "and" : "or";

		if (conj.children.empty()) {
			return "{\"type\":\"true\"}";
		}
		if (conj.children.size() == 1) {
			return ExprToIRCJson(*conj.children[0], get);
		}

		// Build left-associative nested binary tree: ((c0 op c1) op c2) ...
		string result = ExprToIRCJson(*conj.children[0], get);
		for (idx_t i = 1; i < conj.children.size(); i++) {
			result = string("{\"type\":\"") + irc_type + "\",\"left\":" + result +
			         ",\"right\":" + ExprToIRCJson(*conj.children[i], get) + "}";
		}
		return result;
	}

	// BoundOperatorExpression: IS NULL / IS NOT NULL
	case ExpressionClass::BOUND_OPERATOR: {
		if (expr.type != ExpressionType::OPERATOR_IS_NULL &&
		    expr.type != ExpressionType::OPERATOR_IS_NOT_NULL) {
			return "{\"type\":\"true\"}";
		}
		auto &op = reinterpret_cast<const BoundOperatorExpression &>(expr);
		if (op.children.size() != 1 ||
		    op.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return "{\"type\":\"true\"}";
		}
		auto &col = reinterpret_cast<const BoundColumnRefExpression &>(*op.children[0]);
		if (col.binding.column_index >= get.names.size()) {
			return "{\"type\":\"true\"}";
		}
		const char *irc_type = (expr.type == ExpressionType::OPERATOR_IS_NULL) ? "is-null" : "not-null";
		return string("{\"type\":\"") + irc_type + "\",\"term\":\"" +
		       get.names[col.binding.column_index] + "\"}";
	}

	default:
		return "{\"type\":\"true\"}";
	}
}

// Serialize a vector of filter expressions to a single IRC Expression JSON.
// Multiple filters are ANDed together.  Returns "" when filters is empty.
static string SerializeFiltersToIRC(const vector<unique_ptr<Expression>> &filters, const LogicalGet &get) {
	if (filters.empty()) {
		return "";
	}
	if (filters.size() == 1) {
		return ExprToIRCJson(*filters[0], get);
	}
	string result = ExprToIRCJson(*filters[0], get);
	for (idx_t i = 1; i < filters.size(); i++) {
		result = "{\"type\":\"and\",\"left\":" + result +
		         ",\"right\":" + ExprToIRCJson(*filters[i], get) + "}";
	}
	return result;
}

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
// filters → IRC JSON, calls PlanTableScan, binds parquet_scan with the result.
static void UCScanPlanPushdownFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                     vector<unique_ptr<Expression>> &filters) {
	auto &bd = reinterpret_cast<UCScanPlanBindData &>(*bind_data_p);
	try {
		string filter_json = SerializeFiltersToIRC(filters, get);
		auto plan = UCAPI::PlanTableScan(context, bd.catalog_name, bd.schema_name, bd.table_name,
		                                 bd.credentials, bd.scan_plan_endpoint, filter_json);

		if (plan.status == UCScanPlanStatus::COMPLETED) {
			// Resolve any plan-task tokens before binding parquet_scan.
			// Note: FetchScanTasks is called eagerly here (post-optimizer, filters already sent
			// to the server in PlanTableScan).  True lazy streaming via Delta's MultiFileList
			// would require a different bind-time hook and would forfeit filter pushdown.
			for (auto &token : plan.plan_tasks) {
				auto batch = UCAPI::FetchScanTasks(context, bd.catalog_name, bd.schema_name,
				                                   bd.table_name, token, bd.credentials,
				                                   bd.scan_plan_endpoint);
				for (auto &task : batch.file_scan_tasks) {
					plan.file_scan_tasks.push_back(std::move(task));
				}
			}
			plan.plan_tasks.clear();

			auto &sys_cat   = Catalog::GetSystemCatalog(context);
			auto &pq_entry  = sys_cat.GetEntry<TableFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "parquet_scan");
			auto parquet_fn = pq_entry.functions.GetFunctionByArguments(context,
			                      {LogicalType::LIST(LogicalType::VARCHAR)});

			bd.parquet_bind_data  = BindParquetFiles(context, plan, parquet_fn);
			bd.parquet_init_global = parquet_fn.init_global;
			bd.parquet_init_local  = parquet_fn.init_local;
			bd.parquet_scan_fn     = parquet_fn.function;
			bd.scan_plan_done      = true;
			// Leave filters in place: DuckDB converts them to TableFilters for
			// parquet row-group pruning and adds a LogicalFilter for row-level
			// correctness.  The server used them only for file-level pruning.
			return;
		}
	} catch (...) {}

	// Failure: mark endpoint permanently unavailable so next query takes Delta path.
	if (bd.unity_catalog) {
		bd.unity_catalog->MarkScanPlanUnavailable();
	}
	throw IOException("UC scan plan API call failed; detach and re-attach to use Delta fallback");
}

static unique_ptr<GlobalTableFunctionState> UCScanPlanInitGlobal(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bd = reinterpret_cast<const UCScanPlanBindData &>(*input.bind_data);
	D_ASSERT(bd.scan_plan_done);
	TableFunctionInitInput parquet_input(bd.parquet_bind_data.get(), vector<column_t>(input.column_ids),
	                                     input.projection_ids, input.filters);
	return bd.parquet_init_global(context, parquet_input);
}

static unique_ptr<LocalTableFunctionState> UCScanPlanInitLocal(ExecutionContext &context,
                                                                TableFunctionInitInput &input,
                                                                GlobalTableFunctionState *global_state) {
	auto &bd = reinterpret_cast<const UCScanPlanBindData &>(*input.bind_data);
	D_ASSERT(bd.scan_plan_done);
	TableFunctionInitInput parquet_input(bd.parquet_bind_data.get(), vector<column_t>(input.column_ids),
	                                     input.projection_ids, input.filters);
	return bd.parquet_init_local(context, parquet_input, global_state);
}

static void UCScanPlanScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bd = reinterpret_cast<const UCScanPlanBindData &>(*data.bind_data);
	D_ASSERT(bd.scan_plan_done);
	TableFunctionInput parquet_input(bd.parquet_bind_data.get(), data.local_state, data.global_state);
	bd.parquet_scan_fn(context, parquet_input, output);
}

static TableFunction MakeUCScanPlanTableFunction() {
	TableFunction func("uc_scan_plan", {}, nullptr);
	func.pushdown_complex_filter = UCScanPlanPushdownFilter;
	func.init_global              = UCScanPlanInitGlobal;
	func.init_local               = UCScanPlanInitLocal;
	func.function                 = UCScanPlanScan;
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
		auto bd               = make_uniq<UCScanPlanBindData>();
		bd->catalog_name      = table_data->catalog_name;
		bd->schema_name       = table_data->schema_name;
		bd->table_name        = table_data->name;
		bd->credentials       = table.catalog.credentials;
		bd->scan_plan_endpoint = scan_ep;
		bd->unity_catalog     = &table.catalog;
		bind_data             = std::move(bd);
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
