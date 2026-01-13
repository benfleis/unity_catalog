#include "uc_api.hpp"
#include "uc_utils.hpp"

#include "storage/uc_catalog.hpp"
#include "storage/uc_table_set.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/uc_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

UCTableSet::UCTableSet(UCSchemaEntry &schema) : catalog(schema.ParentCatalog().Cast<UCCatalog>()), schema(schema) {
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, UCAPIColumnDefinition &coldef) {
	return {coldef.name, UCUtils::TypeToLogicalType(context, coldef.type_text)};
}

optional_ptr<TableCatalogEntry> TableInformation::GetEntry(idx_t version) {
	lock_guard<mutex> l(entry_lock);
	auto it = schema_versions.find(version);
	if (it == schema_versions.end()) {
		return nullptr;
	}
	auto &entry = it->second;
	return entry->Cast<TableCatalogEntry>();
};

void UCTableSet::LoadEntries(ClientContext &context, const EntryLookupInfo &lookup) {
	auto &transaction = UCTransaction::Get(context, catalog);

	auto &uc_catalog = catalog.Cast<UCCatalog>();
	auto tables = UCAPI::GetTables(context, catalog, schema.name, uc_catalog.credentials);

	for (auto &table : tables) {
		D_ASSERT(schema.name == table.schema_name);
		CreateTableInfo info;
		for (auto &col : table.columns) {
			info.columns.AddColumn(CreateColumnDefinition(context, col));
		}

		info.table = table.name;
		auto table_entry = make_uniq<UCTableEntry>(catalog, schema, info);
		table_entry->table_data = make_uniq<UCAPITable>(table);

		CreateEntry(std::move(table_entry));
	}
}

optional_ptr<CatalogEntry> UCTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	throw NotImplementedException("UCTableSet::CreateTable");
}

void UCTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
	throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
	throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
	throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
	throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
	throw NotImplementedException("UCTableSet::AlterTable");
}

optional_ptr<CatalogEntry> UCTableSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("UCTableSet::CreateEntry called with empty name");
	}
	auto it = tables.find(result->name);
	if (it != tables.end()) {
		//! Already exists ??
		return nullptr;
	}
	tables.emplace(result->name, std::move(entry));
	return result;
}

optional_ptr<CatalogEntry> UCTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context, lookup);
	}
	lock_guard<mutex> l(entry_lock);
	auto &name = lookup.GetEntryName();
	auto entry = tables.find(name);
	if (entry == tables.end()) {
		return nullptr;
	}
	auto &table_info = entry->second;
	return table_info.dummy.get();
}

void UCTableSet::ClearEntries() {
	tables.clear();
	is_loaded = false;
}

void UCTableSet::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("UCTableSet::DropEntry");
}

void UCTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	if (!is_loaded) {
		is_loaded = true;
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, "__DEFAULT__");
		LoadEntries(context, lookup);
	}
	lock_guard<mutex> l(entry_lock);
	for (auto &table : tables) {
		callback(*table.second.dummy);
	}
}

} // namespace duckdb
