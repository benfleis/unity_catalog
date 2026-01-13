//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/uc_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/uc_catalog_set.hpp"
#include "storage/uc_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;
class UCCatalog;

class UCSchemaSet {
public:
	explicit UCSchemaSet(UCCatalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	virtual void DropEntry(ClientContext &context, DropInfo &info);
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
	void ClearEntries();

protected:
	void LoadEntries(ClientContext &context, const EntryLookupInfo &lookup);
private:
	UCCatalog &catalog;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<SchemaCatalogEntry>> schemas;
	bool is_loaded = false;
};

} // namespace duckdb
