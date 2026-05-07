//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/uc_table_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
class UnityCatalog;
class UCSchemaEntry;

class UCDeltaCCV2Commit : public TableFunction {
public:
	UCDeltaCCV2Commit();
};

class UCTableDataPath : public TableFunction {
public:
	explicit UCTableDataPath(UCSchemaEntry &schema);
};

// IPC table function called by Delta's MultiFileList to fetch the next batch of file paths
// from a plan-task token.  Delta passes a Value::POINTER to a UCScanPlanFetchContext in
// col 0 row 0; this function pops the next token, calls FetchScanTasks, and writes the
// resulting file paths as a LIST(VARCHAR) into col 1 row 0.
class UCDeltaScanPlanFetchTasks : public TableFunction {
public:
	UCDeltaScanPlanFetchTasks();
};

} // namespace duckdb
