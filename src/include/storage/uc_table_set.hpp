//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/uc_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/uc_catalog_set.hpp"
#include "storage/uc_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class UCResult;
class UCCatalog;
class UCSchemaEntry;

class TableInformation {
public:
	TableInformation(unique_ptr<CatalogEntry> dummy) : dummy(std::move(dummy)) {}
public:
	optional_ptr<TableCatalogEntry> GetEntry(idx_t version);
public:
	mutex entry_lock;
	//! Map of delta version to TableCatalogEntry for the table
	unordered_map<idx_t, unique_ptr<CatalogEntry>> schema_versions;
	//! Dummy entry created from the "List tables" API result, presumably the latest schema version
	//! Only used for things like SHOW TABLES
	unique_ptr<CatalogEntry> dummy;
};

class UCTableSet {
public:
	explicit UCTableSet(UCSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);
	void AlterTable(ClientContext &context, AlterTableInfo &info);
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);

	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	void DropEntry(ClientContext &context, DropInfo &info);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	void ClearEntries();

protected:
	void LoadEntries(ClientContext &context, const EntryLookupInfo &lookup);

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);
private:
	UCCatalog &catalog;
	UCSchemaEntry &schema;
	mutex entry_lock;
	case_insensitive_map_t<TableInformation> tables;
	bool is_loaded = false;
};

} // namespace duckdb
