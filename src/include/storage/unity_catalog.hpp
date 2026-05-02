//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/unity_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "storage/uc_schema_set.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {
class UCSchemaEntry;

struct UCCredentials {
	string endpoint;
	string token;
	string aws_region; // not really credentials; required to query S3 tables
	string scan_plan_endpoint; // explicitly set via attach option; empty = probe on first use
};

class UCClearCacheFunction : public TableFunction {
public:
	UCClearCacheFunction();

	static void ClearCacheOnSetting(ClientContext &context, SetScope scope, Value &parameter);
};

class UnityCatalog : public Catalog {
public:
	explicit UnityCatalog(AttachedDatabase &db_p, const string &internal_name, AttachOptions &attach_options,
	                      UCCredentials credentials, const string &default_schema,
	                      string catalog_name = "unity_catalog");
	~UnityCatalog();

	string internal_name;
	AccessMode access_mode;
	UCCredentials credentials;

	string catalog_name;

public:
	void Initialize(bool load_builtin) override;

	string GetCatalogType() override {
		return catalog_name;
	}
	static bool IsUnityCatalog(Catalog &cat) {
		const auto &t = cat.GetCatalogType();
		// be paranoid, support old aliases
		return (t == "unity_catalog" || t == "uc_catalog" || t == "uc");
	}

	bool SupportsTimeTravel() const override {
		return true;
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	string GetDefaultSchema() const override;
	void OnDetach(ClientContext &context) override;

	//! Whether or not this is an in-memory UC database
	bool InMemory() override;
	string GetDBPath() override;

	void ClearCache();

	// Returns the scan plan endpoint to use for this catalog, or "" to skip scan planning.
	// On the first successful PlanTableScan the caller should do nothing; on any failure
	// the caller should set scan_plan_unavailable = true so subsequent queries skip the
	// scan plan path entirely.
	string GetScanPlanEndpoint();

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	UCSchemaSet schemas;
	string default_schema;

	// Set to true by GetScanFunction on any scan plan failure to skip the path permanently.
	std::atomic<bool> scan_plan_unavailable {false};
};

} // namespace duckdb
