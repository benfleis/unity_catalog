#include "storage/unity_catalog_set.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/uc_schema_entry.hpp"

namespace duckdb {

UnityCatalogSet::UnityCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> UnityCatalogSet::GetEntry(ClientContext &context, const string &name) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void UnityCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("UnityCatalogSet::DropEntry");
}

void UnityCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void UnityCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> UnityCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("UnityCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void UnityCatalogSet::ClearEntries() {
	entries.clear();
	is_loaded = false;
}

UCInSchemaSet::UCInSchemaSet(UCSchemaEntry &schema) : UnityCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> UCInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return UnityCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
