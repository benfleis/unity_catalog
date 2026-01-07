#include "storage/uc_catalog.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_table_entry.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "uc_api.hpp"

namespace duckdb {

UCTableEntry::UCTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
}

UCTableEntry::UCTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, UCTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = false;
}

unique_ptr<BaseStatistics> UCTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

optional_ptr<Catalog> UCTableEntry::GetInternalCatalog() {
	return internal_attached_database->GetCatalog();
}

void UCTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &,
                                         ClientContext &) {
	throw NotImplementedException("BindUpdateConstraints");
}

void UCTableEntry::InternalAttach(ClientContext &context, UCCatalog &catalog) {
	if (internal_attached_database) {
		return;
	}
	auto &db_manager = DatabaseManager::Get(context);

	// Create the attach info for the table
	AttachInfo info;
	info.name = "__unity_catalog_internal_" + catalog.internal_name + "_" + schema.name + "_" + name; // TODO:
	info.options = {
		{"type", Value("Delta")}, {"child_catalog_mode", Value(true)}, {"internal_table_name", Value(name)}};
	info.path = table_data->storage_location;
	AttachOptions options(context.db->config.options);
	options.access_mode = AccessMode::READ_WRITE;
	options.db_type = "delta";

	auto &internal_db = internal_attached_database;
	internal_db = db_manager.AttachDatabase(context, info, options);

	//! Initialize the database.
	internal_db->Initialize(context);
	internal_db->FinalizeLoad(context);
	db_manager.FinalizeAttach(context, info, internal_db);
}

void UCTableEntry::RefreshCredentials(ClientContext &context, UCCatalog &uc_catalog) {
	D_ASSERT(table_data);
	if (table_data->storage_location.find("file://") == 0) {
		return;
	}
	auto &secret_manager = SecretManager::Get(context);
	// Get Credentials from UCAPI
	auto table_credentials = UCAPI::GetTableCredentials(context, table_data->table_id, uc_catalog.credentials);

	// Inject secret into secret manager scoped to this path
	CreateSecretInput input;
	input.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	input.persist_type = SecretPersistType::TEMPORARY;
	input.name = "__internal_uc_" + table_data->table_id;
	input.type = "s3";
	input.provider = "config";
	input.options = {
		{"key_id", table_credentials.key_id},
		{"secret", table_credentials.secret},
		{"session_token", table_credentials.session_token},
		{"region", uc_catalog.credentials.aws_region},
	};
	input.scope = {table_data->storage_location};

	secret_manager.CreateSecret(context, input);
}

TableFunction UCTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("UCTableEntry::GetScanFunction called without entry lookup info");
}

TableFunction UCTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data, const EntryLookupInfo &lookup_info) {
	D_ASSERT(table_data);
	if (table_data->data_source_format != "DELTA") {
		throw NotImplementedException("Table '%s' is of unsupported format '%s', ", table_data->name,
		                              table_data->data_source_format);
	}
	auto &uc_catalog = catalog.Cast<UCCatalog>();
	RefreshCredentials(context, uc_catalog);

	InternalAttach(context, uc_catalog);
	auto &delta_catalog = *GetInternalCatalog();

	auto &schema = delta_catalog.GetSchema(context, table_data->schema_name);
	auto transaction = schema.GetCatalogTransaction(context);
	auto table_entry = schema.LookupEntry(transaction, lookup_info);
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
