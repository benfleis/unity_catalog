#include <chrono>
#include <cstddef>
#include <sys/stat.h>
#include <thread>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "duckdb/common/http_util.hpp"
#include "uc_logging.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "uc_api.hpp"
#include "storage/unity_catalog.hpp"
#include "yyjson.hpp"

namespace duckdb {

// RAII wrapper for yyjson_doc* to ensure yyjson_doc_free is called even when exceptions are thrown
struct YYJsonDoc {
	explicit YYJsonDoc(const string &json) : doc(duckdb_yyjson::yyjson_read(json.c_str(), json.size(), 0)) {
	}
	~YYJsonDoc() {
		if (doc) {
			duckdb_yyjson::yyjson_doc_free(doc);
		}
	}
	duckdb_yyjson::yyjson_val *Root() const {
		return duckdb_yyjson::yyjson_doc_get_root(doc);
	}
	// non-copyable
	YYJsonDoc(const YYJsonDoc &) = delete;
	YYJsonDoc &operator=(const YYJsonDoc &) = delete;

	duckdb_yyjson::yyjson_doc *doc;
};

static void AuthenticateViaBearerToken(HTTPHeaders &hdrs, const string &token) {
	if (!token.empty()) {
		hdrs.Insert("Authorization", "Bearer " + token);
		// curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
	}
}

static void EnsureHttpfsExtension(shared_ptr<DatabaseInstance> db) {
	// autoloading/requiring HTTPFS at this ext's Load time fails, iceberg does the same deferred load
	if (!db) {
		throw InvalidConfigurationException("Context does not have database instance");
	}
	ExtensionHelper::AutoLoadExtension(*db, "httpfs");
	if (!db->ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The iceberg extension requires the httpfs extension to be loaded!");
	}
}

static string MakeRequest(ClientContext &ctx, const string &url, const string &token = "", const string &body = "",
                          bool send_as_get = false) {
	auto db = ctx.db;
	EnsureHttpfsExtension(db);
	auto &http_util = HTTPUtil::Get(*db);
	auto params = http_util.InitializeParameters(*db, url);
	params->logger = ctx.logger;

	HTTPHeaders hdrs(*ctx.db);
	AuthenticateViaBearerToken(hdrs, token);
	// TODO: conditional set if not override?
	hdrs.Insert("Content-Type", "application/json");

	unique_ptr<HTTPResponse> resp;
	if (body.empty()) {
		GetRequestInfo req(url, hdrs, *params, nullptr, nullptr);
		resp = http_util.Request(req);
	} else {
		PostRequestInfo req(url, hdrs, *params, const_data_ptr_cast(body.data()), body.size());
		req.send_post_as_get_request = send_as_get;
		resp = http_util.Request(req);
	}

	if (!resp->Success()) {
		auto status_int = static_cast<int>(resp->status);
		throw IOException("Request to '%s' failed (HTTP %d): %s\nResponse body: %s", url, status_int, resp->GetError(),
		                  resp->body.empty() ? "(empty)" : resp->body);
	}
	return std::move(resp->body);
}

template <class TYPE, uint8_t TYPE_NUM, TYPE (*get_function)(duckdb_yyjson::yyjson_val *obj)>
static TYPE TemplatedTryGetYYJson(duckdb_yyjson::yyjson_val *obj, const string &field, TYPE default_val,
                                  bool fail_on_missing = true) {
	auto val = yyjson_obj_get(obj, field.c_str());
	if (val && !yyjson_is_null(val)) {
		if (yyjson_get_type(val) == TYPE_NUM) {
			return get_function(val);
		}
		throw IOException("Invalid field found while parsing field: " + field);
	}
	// field absent or JSON null
	if (fail_on_missing && !val) {
		throw IOException("Invalid field found while parsing field: " + field);
	}
	return default_val;
}

static uint64_t TryGetNumFromObject(duckdb_yyjson::yyjson_val *obj, const string &field, bool fail_on_missing = true,
                                    uint64_t default_val = 0) {
	return TemplatedTryGetYYJson<uint64_t, YYJSON_TYPE_NUM, duckdb_yyjson::yyjson_get_uint>(obj, field, default_val,
	                                                                                        fail_on_missing);
}
static bool TryGetBoolFromObject(duckdb_yyjson::yyjson_val *obj, const string &field, bool fail_on_missing = false,
                                 bool default_val = false) {
	return TemplatedTryGetYYJson<bool, YYJSON_TYPE_BOOL, duckdb_yyjson::yyjson_get_bool>(obj, field, default_val,
	                                                                                     fail_on_missing);
}
static string TryGetStrFromObject(duckdb_yyjson::yyjson_val *obj, const string &field, bool fail_on_missing = true,
                                  const char *default_val = "") {
	return TemplatedTryGetYYJson<const char *, YYJSON_TYPE_STR, duckdb_yyjson::yyjson_get_str>(obj, field, default_val,
	                                                                                           fail_on_missing);
}

namespace {

struct UCAPIError {
public:
	UCAPIError() {
	}
	UCAPIError(const string &error_code, const string &message) : error_code(error_code), message(message) {
	}

public:
	bool HasError() {
		return !error_code.empty();
	}

public:
	void ThrowError(const string &prefix) {
		D_ASSERT(HasError());
		throw IOException("%s. error_code: %s, message: %s", prefix, error_code, message);
	}

private:
	string error_code;
	string message;
};

} // namespace

static UCAPIError CheckError(duckdb_yyjson::yyjson_val *api_result) {
	// delta.yaml v1 format: {"error": {"message": "...", "type": "CommitVersionConflictException", "code": N}}
	auto *error_obj = yyjson_obj_get(api_result, "error");
	if (error_obj && yyjson_is_obj(error_obj)) {
		auto message = TryGetStrFromObject(error_obj, "message", false);
		if (!message.empty()) {
			auto type = TryGetStrFromObject(error_obj, "type", false);
			return UCAPIError(type.empty() ? "error" : type, message);
		}
	}
	// all.yaml legacy format: {"error_code": "...", "message": "..."}
	auto error_code = TryGetStrFromObject(api_result, "error_code", false);
	if (!error_code.empty()) {
		auto message = TryGetStrFromObject(api_result, "message", false);
		if (message.empty()) {
			message = "-";
		}
		return UCAPIError(error_code, message);
	}
	return UCAPIError();
}

// # list catalogs
//     echo "List of catalogs"
//     curl --request GET
//     "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs" \
//  	--header "Authorization: Bearer ${TOKEN}" | jq .
//
// # list short version of all tables
//     echo "Table Summaries"
//     curl --request GET
//     "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/table-summaries?catalog_name=workspace"
//     \
//  	--header "Authorization: Bearer ${TOKEN}" | jq .
//
// # list tables in `default` schema
//     echo "Tables in default schema"
//     curl --request GET
//     "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/tables?catalog_name=workspace&schema_name=default"
//     \
//  	--header "Authorization: Bearer ${TOKEN}" | jq .

string UCAPI::GetDefaultSchema(ClientContext &ctx, const UCCredentials &credentials) {
	auto url = credentials.endpoint + "/api/2.0/settings/types/default_namespace_ws/names/default";
	UC_LOG_DEBUG(ctx, "uc-api.GetDefaultSchema endpoint=%s", credentials.endpoint);
	auto resp = MakeRequest(ctx, url, credentials.token);

	YYJsonDoc doc(resp);
	auto *root = doc.Root();

	auto error = CheckError(root);
	if (error.HasError()) {
		error.ThrowError("Failed to get default schema of the catalog");
	}

	auto setting_name = TryGetStrFromObject(root, "setting_name", false);
	if (setting_name.empty()) {
		throw InvalidInputException("Failed to get default schema of the catalog, "
		                            "API response is invalid!");
	}

	return setting_name;
}

UCAPICommitsResult UCAPI::LoadTable(ClientContext &ctx, const string &catalog_name, const string &schema_name,
                                    const string &table_name, const UCCredentials &credentials) {
	UCAPICommitsResult result;
	// XXX: hard-coded delta v1 protocol; protocol negotiation via GET /delta/v1/config not yet implemented
	string url = StringUtil::Format("%s/api/2.1/unity-catalog/delta/v1/catalogs/%s/schemas/%s/tables/%s",
	                                credentials.endpoint, catalog_name, schema_name, table_name);
	UC_LOG_DEBUG(ctx, "uc-api.LoadTable %s.%s.%s", catalog_name, schema_name, table_name);
	auto api_result = MakeRequest(ctx, url, credentials.token);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	auto error = CheckError(root);
	if (error.HasError()) {
		error.ThrowError(StringUtil::Format("Failed to load table %s.%s.%s", catalog_name, schema_name, table_name));
	}

	auto *metadata = yyjson_obj_get(root, "metadata");
	if (metadata && yyjson_is_obj(metadata)) {
		result.etag = TryGetStrFromObject(metadata, "etag", false);
	}

	result.latest_table_version = TryGetNumFromObject(root, "latest-table-version", false, 0);

	auto *commits = yyjson_obj_get(root, "commits");
	if (commits && yyjson_is_arr(commits)) {
		size_t idx, max;
		duckdb_yyjson::yyjson_val *commit;
		yyjson_arr_foreach(commits, idx, max, commit) {
			UCAPICommit c;
			c.version = TryGetNumFromObject(commit, "version", true);
			c.timestamp = TryGetNumFromObject(commit, "timestamp", true);
			c.file_name = TryGetStrFromObject(commit, "file-name", true);
			c.file_size = TryGetNumFromObject(commit, "file-size", true);
			c.file_modification_timestamp = TryGetNumFromObject(commit, "file-modification-timestamp", true);
			result.commits.push_back(c);
		}
	}

	UC_LOG_DEBUG(ctx, "uc-api.LoadTable %s.%s.%s -> etag=%s commits=%zu latest_version=%lld", catalog_name, schema_name,
	             table_name, result.etag.empty() ? "(none)" : result.etag, result.commits.size(),
	             (long long)result.latest_table_version);
	return result;
}

string UCAPI::UpdateTable(ClientContext &ctx, const string &catalog_name, const string &schema_name,
                          const string &table_name, const string &table_id, const string &etag,
                          const UCCredentials &credentials, idx_t version, idx_t timestamp, const string &file_name,
                          idx_t file_size, idx_t file_modification_timestamp, idx_t backfill_version) {
	string uuid_req = StringUtil::Format(R"({"type": "assert-table-uuid", "uuid": "%s"})", table_id);
	string etag_req = etag.empty() ? "" : StringUtil::Format(R"(, {"type": "assert-etag", "etag": "%s"})", etag);
	string backfill_update;
	if (backfill_version != idx_t(-1)) {
		backfill_update =
		    StringUtil::Format(R"(, {"action": "set-latest-backfilled-version", "latest-published-version": %lld})",
		                       (long long)backfill_version);
	}
	string body = StringUtil::Format(
	    R"({"requirements": [%s%s], "updates": [{"action": "add-commit", "commit": {"version": %lld, "timestamp": %lld, "file-name": "%s", "file-size": %lld, "file-modification-timestamp": %lld}}%s]})",
	    uuid_req, etag_req, (long long)version, (long long)timestamp, file_name, (long long)file_size,
	    (long long)file_modification_timestamp, backfill_update);
	// XXX: hard-coded delta v1 protocol; protocol negotiation via GET /delta/v1/config not yet implemented
	string url = StringUtil::Format("%s/api/2.1/unity-catalog/delta/v1/catalogs/%s/schemas/%s/tables/%s",
	                                credentials.endpoint, catalog_name, schema_name, table_name);
	UC_LOG_DEBUG(ctx, "uc-api.UpdateTable %s.%s.%s version=%lld etag=%s", catalog_name, schema_name, table_name,
	             (long long)version, etag.empty() ? "(none)" : etag);
	auto api_result = MakeRequest(ctx, url, credentials.token, body);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	auto error = CheckError(root);
	if (error.HasError()) {
		error.ThrowError(StringUtil::Format("Failed to commit to %s.%s.%s", catalog_name, schema_name, table_name));
	}

	string new_etag;
	auto *metadata = yyjson_obj_get(root, "metadata");
	if (metadata && yyjson_is_obj(metadata)) {
		new_etag = TryGetStrFromObject(metadata, "etag", false);
	}
	UC_LOG_DEBUG(ctx, "uc-api.UpdateTable %s.%s.%s -> new_etag=%s", catalog_name, schema_name, table_name,
	             new_etag.empty() ? "(none)" : new_etag);
	return new_etag;
}

UCAPITableCredentials UCAPI::GetTableCredentials(ClientContext &ctx, const string &catalog_name,
                                                 const string &schema_name, const string &table_name, bool write,
                                                 const UCCredentials &credentials) {
	UCAPITableCredentials result;
	const char *operation = write ? "READ_WRITE" : "READ";
	// XXX: hard-coded delta v1 protocol; protocol negotiation via GET /delta/v1/config not yet implemented
	string url = StringUtil::Format(
	    "%s/api/2.1/unity-catalog/delta/v1/catalogs/%s/schemas/%s/tables/%s/credentials?operation=%s",
	    credentials.endpoint, catalog_name, schema_name, table_name, operation);
	UC_LOG_DEBUG(ctx, "uc-api.GetTableCredentials %s.%s.%s op=%s", catalog_name, schema_name, table_name, operation);
	auto api_result = MakeRequest(ctx, url, credentials.token);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	auto error = CheckError(root);
	if (error.HasError()) {
		error.ThrowError(
		    StringUtil::Format("Failed to get table credentials for %s.%s.%s", catalog_name, schema_name, table_name));
	}

	// Parse storage-credentials array; use first entry (longest-prefix matching is a TODO)
	auto *creds_arr = yyjson_obj_get(root, "storage-credentials");
	if (creds_arr && yyjson_is_arr(creds_arr) && yyjson_arr_size(creds_arr) > 0) {
		auto *cred = yyjson_arr_get_first(creds_arr);
		auto *cfg = yyjson_obj_get(cred, "config");
		if (cfg && yyjson_is_obj(cfg)) {
			result.key_id = TryGetStrFromObject(cfg, "s3.access-key-id", false);
			result.secret = TryGetStrFromObject(cfg, "s3.secret-access-key", false);
			result.session_token = TryGetStrFromObject(cfg, "s3.session-token", false);
		}
	}

	return result;
}

vector<string> UCAPI::GetCatalogs(ClientContext &ctx, Catalog &catalog, const UCCredentials &credentials) {
	throw NotImplementedException("UCAPI::GetCatalogs");
}

static UCAPIColumnDefinition ParseColumnDefinition(duckdb_yyjson::yyjson_val *column_def) {
	UCAPIColumnDefinition result;

	result.name = TryGetStrFromObject(column_def, "name");
	result.type_text = TryGetStrFromObject(column_def, "type_text");
	result.precision = TryGetNumFromObject(column_def, "type_precision");
	result.scale = TryGetNumFromObject(column_def, "type_scale");
	result.position = TryGetNumFromObject(column_def, "position");

	return result;
}

vector<UCAPITable> UCAPI::GetTables(ClientContext &ctx, Catalog &catalog, const string &schema,
                                    const UCCredentials &credentials) {
	vector<UCAPITable> result;
	auto url = credentials.endpoint + "/api/2.1/unity-catalog/tables?catalog_name=" + catalog.GetDBPath() +
	           "&schema_name=" + schema;
	UC_LOG_DEBUG(ctx, "uc-api.GetTables catalog=%s schema=%s", catalog.GetDBPath(), schema);
	auto api_result = MakeRequest(ctx, url, credentials.token);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	// Get root["hits"], iterate over the array
	auto *tables = yyjson_obj_get(root, "tables");
	size_t idx, max;
	duckdb_yyjson::yyjson_val *table;
	yyjson_arr_foreach(tables, idx, max, table) {
		UCAPITable table_result;
		table_result.catalog_name = catalog.GetDBPath();
		table_result.schema_name = schema;

		table_result.name = TryGetStrFromObject(table, "name");
		table_result.table_type = TryGetStrFromObject(table, "table_type");
		table_result.data_source_format = TryGetStrFromObject(table, "data_source_format", false);
		table_result.storage_location = TryGetStrFromObject(table, "storage_location", false);
		table_result.table_id = TryGetStrFromObject(table, "table_id");

		auto *columns = yyjson_obj_get(table, "columns");
		duckdb_yyjson::yyjson_val *col;
		size_t col_idx, col_max;
		yyjson_arr_foreach(columns, col_idx, col_max, col) {
			auto column_definition = ParseColumnDefinition(col);
			table_result.columns.push_back(column_definition);
		}

		auto *properties = yyjson_obj_get(table, "properties");
		duckdb_yyjson::yyjson_val *key, *val;
		size_t prop_idx, prop_max;
		yyjson_obj_foreach(properties, prop_idx, prop_max, key, val) {
			auto val_cstr = duckdb_yyjson::yyjson_get_str(val);
			if (val_cstr) {
				table_result.properties[duckdb_yyjson::yyjson_get_str(key)] = val_cstr;
			}
		}

		result.push_back(table_result);
	}

	UC_LOG_DEBUG(ctx, "uc-api.GetTables catalog=%s schema=%s -> tables=%zu", catalog.GetDBPath(), schema,
	             result.size());
	return result;
}

vector<UCAPISchema> UCAPI::GetSchemas(ClientContext &ctx, Catalog &catalog, const UCCredentials &credentials) {
	vector<UCAPISchema> result;
	auto url = credentials.endpoint + "/api/2.1/unity-catalog/schemas?catalog_name=" + catalog.GetDBPath();
	UC_LOG_DEBUG(ctx, "uc-api.GetSchemas catalog=%s", catalog.GetDBPath());
	auto api_result = MakeRequest(ctx, url, credentials.token);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	// Get root["hits"], iterate over the array
	auto *schemas = yyjson_obj_get(root, "schemas");
	size_t idx, max;
	duckdb_yyjson::yyjson_val *schema;
	yyjson_arr_foreach(schemas, idx, max, schema) {
		UCAPISchema schema_result;

		auto *name = yyjson_obj_get(schema, "name");
		if (name) {
			schema_result.schema_name = yyjson_get_str(name);
		}
		schema_result.catalog_name = catalog.GetDBPath();

		result.push_back(schema_result);
	}

	UC_LOG_DEBUG(ctx, "uc-api.GetSchemas catalog=%s -> schemas=%zu", catalog.GetDBPath(), result.size());
	return result;
}

// ---------------------------------------------------------------------------
// Scan plan API helpers
// ---------------------------------------------------------------------------

static void ParseCountMap(duckdb_yyjson::yyjson_val *map_val, unordered_map<uint32_t, int64_t> &out) {
	if (!map_val) {
		return;
	}
	auto *keys = yyjson_obj_get(map_val, "keys");
	auto *vals = yyjson_obj_get(map_val, "values");
	if (!keys || !vals) {
		return;
	}
	vector<uint32_t> key_vec;
	size_t k_idx, k_max;
	duckdb_yyjson::yyjson_val *k_val;
	yyjson_arr_foreach(keys, k_idx, k_max, k_val) {
		key_vec.push_back((uint32_t)duckdb_yyjson::yyjson_get_uint(k_val));
	}
	size_t v_idx, v_max;
	duckdb_yyjson::yyjson_val *v_val;
	yyjson_arr_foreach(vals, v_idx, v_max, v_val) {
		if (v_idx < key_vec.size()) {
			out[key_vec[v_idx]] = duckdb_yyjson::yyjson_get_sint(v_val);
		}
	}
}

static UCScanPlanDataFile ParseDataFile(duckdb_yyjson::yyjson_val *df_val) {
	UCScanPlanDataFile df;
	df.content = TryGetStrFromObject(df_val, "content", false);
	df.file_path = TryGetStrFromObject(df_val, "file-path");
	df.file_format = TryGetStrFromObject(df_val, "file-format", false);
	df.spec_id = (int64_t)TryGetNumFromObject(df_val, "spec-id", false);
	df.file_size_in_bytes = (int64_t)TryGetNumFromObject(df_val, "file-size-in-bytes", false);
	df.record_count = (int64_t)TryGetNumFromObject(df_val, "record-count", false);
	auto *frid = yyjson_obj_get(df_val, "first-row-id");
	if (frid) {
		df.first_row_id = duckdb_yyjson::yyjson_get_sint(frid);
	}
	ParseCountMap(yyjson_obj_get(df_val, "column-sizes"), df.column_sizes);
	ParseCountMap(yyjson_obj_get(df_val, "value-counts"), df.value_counts);
	ParseCountMap(yyjson_obj_get(df_val, "null-value-counts"), df.null_value_counts);
	ParseCountMap(yyjson_obj_get(df_val, "nan-value-counts"), df.nan_value_counts);
	// lower/upper bounds stored as-is; not yet acted on
	return df;
}

static UCScanDeleteFile ParseDeleteFile(duckdb_yyjson::yyjson_val *del_val) {
	UCScanDeleteFile df;
	string content = TryGetStrFromObject(del_val, "content");
	df.content = (content == "position-deletes") ? UCScanDeleteFileType::POSITION_DELETES
	                                             : UCScanDeleteFileType::EQUALITY_DELETES;
	df.file_path = TryGetStrFromObject(del_val, "file-path");
	df.file_format = TryGetStrFromObject(del_val, "file-format", false);
	df.file_size_in_bytes = (int64_t)TryGetNumFromObject(del_val, "file-size-in-bytes", false);
	df.record_count = (int64_t)TryGetNumFromObject(del_val, "record-count", false);
	auto *eq_ids = yyjson_obj_get(del_val, "equality-ids");
	if (eq_ids) {
		size_t idx, max;
		duckdb_yyjson::yyjson_val *id_val;
		yyjson_arr_foreach(eq_ids, idx, max, id_val) {
			df.equality_ids.push_back((uint32_t)duckdb_yyjson::yyjson_get_uint(id_val));
		}
	}
	auto *offset = yyjson_obj_get(del_val, "content-offset");
	if (offset) {
		df.content_offset = duckdb_yyjson::yyjson_get_sint(offset);
	}
	auto *csize = yyjson_obj_get(del_val, "content-size-in-bytes");
	if (csize) {
		df.content_size_in_bytes = duckdb_yyjson::yyjson_get_sint(csize);
	}
	return df;
}

// Parses the ScanTasks payload fields (delete-files, file-scan-tasks, plan-tasks,
// storage-credentials) from a JSON root into an existing result struct.
// Used by both ParseScanPlanResponse (completed path) and FetchScanTasks.
static void ParseScanTasksPayload(duckdb_yyjson::yyjson_val *root, UCScanPlanResult &result) {
	auto *del_files = yyjson_obj_get(root, "delete-files");
	if (del_files) {
		size_t idx, max;
		duckdb_yyjson::yyjson_val *del_val;
		yyjson_arr_foreach(del_files, idx, max, del_val) {
			result.delete_files.push_back(ParseDeleteFile(del_val));
		}
	}

	auto *tasks = yyjson_obj_get(root, "file-scan-tasks");
	if (tasks) {
		size_t idx, max;
		duckdb_yyjson::yyjson_val *task_val;
		yyjson_arr_foreach(tasks, idx, max, task_val) {
			UCScanPlanFileScanTask task;
			auto *df_val = yyjson_obj_get(task_val, "data-file");
			if (df_val) {
				task.data_file = ParseDataFile(df_val);
			}
			auto *refs = yyjson_obj_get(task_val, "delete-file-references");
			if (refs) {
				size_t r_idx, r_max;
				duckdb_yyjson::yyjson_val *ref_val;
				yyjson_arr_foreach(refs, r_idx, r_max, ref_val) {
					task.delete_file_references.push_back((idx_t)duckdb_yyjson::yyjson_get_uint(ref_val));
				}
			}
			// residual-filter decoded but not yet re-applied
			result.file_scan_tasks.push_back(std::move(task));
		}
	}

	auto *plan_tasks = yyjson_obj_get(root, "plan-tasks");
	if (plan_tasks) {
		size_t idx, max;
		duckdb_yyjson::yyjson_val *pt_val;
		yyjson_arr_foreach(plan_tasks, idx, max, pt_val) {
			const char *pt_str = duckdb_yyjson::yyjson_get_str(pt_val);
			if (pt_str) {
				result.plan_tasks.emplace_back(pt_str);
			}
		}
	}

	auto *storage_creds = yyjson_obj_get(root, "storage-credentials");
	if (storage_creds) {
		size_t idx, max;
		duckdb_yyjson::yyjson_val *cred_val;
		yyjson_arr_foreach(storage_creds, idx, max, cred_val) {
			string prefix = TryGetStrFromObject(cred_val, "prefix", false);
			unordered_map<string, string> config;
			auto *cfg = yyjson_obj_get(cred_val, "config");
			if (cfg) {
				size_t cidx, cmax;
				duckdb_yyjson::yyjson_val *key, *val;
				yyjson_obj_foreach(cfg, cidx, cmax, key, val) {
					const char *k = duckdb_yyjson::yyjson_get_str(key);
					const char *v = duckdb_yyjson::yyjson_get_str(val);
					if (k && v) {
						config.emplace(k, v);
					}
				}
			}
			result.storage_credentials.emplace_back(std::move(prefix), std::move(config));
		}
	}
}

static UCScanPlanResult ParseScanPlanResponse(const string &json_str) {
	YYJsonDoc doc(json_str);
	auto *root = doc.Root();
	if (!root) {
		throw IOException("Failed to parse scan plan response");
	}

	UCScanPlanResult result;
	// NOTE: checkin w/ DB -- should be uppers according to spec
	string status_str = StringUtil::Lower(TryGetStrFromObject(root, "status"));
	if (status_str == "completed") {
		result.status = UCScanPlanStatus::COMPLETED;
	} else if (status_str == "submitted") {
		result.status = UCScanPlanStatus::SUBMITTED;
	} else if (status_str == "failed") {
		result.status = UCScanPlanStatus::FAILED;
	} else if (status_str == "cancelled") {
		result.status = UCScanPlanStatus::CANCELLED;
	}
	result.plan_id = TryGetStrFromObject(root, "plan-id", false);

	if (result.status == UCScanPlanStatus::FAILED) {
		auto *error_obj = yyjson_obj_get(root, "error");
		if (error_obj) {
			result.error_message = TryGetStrFromObject(error_obj, "message", false);
			result.error_type = TryGetStrFromObject(error_obj, "type", false);
		}
		return result;
	}

	if (result.status != UCScanPlanStatus::COMPLETED) {
		return result;
	}

	ParseScanTasksPayload(root, result);
	return result;
}

// ---------------------------------------------------------------------------
// Scan plan API methods
// ---------------------------------------------------------------------------

static const char *UCScanPlanStatusToString(UCScanPlanStatus s) {
	switch (s) {
	case UCScanPlanStatus::COMPLETED:
		return "completed";
	case UCScanPlanStatus::SUBMITTED:
		return "submitted";
	case UCScanPlanStatus::FAILED:
		return "failed";
	case UCScanPlanStatus::CANCELLED:
		return "cancelled";
	default:
		return "unknown";
	}
}

UCScanPlanResult UCAPI::FetchPlanningResult(ClientContext &ctx, const string &catalog_name, const string &schema_name,
                                            const string &table_name, const string &plan_id,
                                            const UCCredentials &credentials, const string &scan_plan_endpoint) {
	string url = scan_plan_endpoint + "/v1/catalogs/" + catalog_name + "/namespaces/" + schema_name + "/tables/" +
	             table_name + "/plan/" + plan_id;
	UC_LOG_DEBUG(ctx, "scan-plan.FetchPlanningResult plan_id=%s", plan_id);
	auto resp = MakeRequest(ctx, url, credentials.token);
	auto result = ParseScanPlanResponse(resp);
	UC_LOG_DEBUG(ctx, "scan-plan.FetchPlanningResult plan_id=%s -> status=%s inline=%zu plan_tasks=%zu delete=%zu%s%s",
	             plan_id, UCScanPlanStatusToString(result.status), result.file_scan_tasks.size(),
	             result.plan_tasks.size(), result.delete_files.size(),
	             result.status == UCScanPlanStatus::FAILED ? " error=" : "",
	             result.status == UCScanPlanStatus::FAILED ? result.error_message.c_str() : "");
	return result;
}

UCScanPlanResult UCAPI::PlanTableScan(ClientContext &ctx, const string &catalog_name, const string &schema_name,
                                      const string &table_name, const UCCredentials &credentials,
                                      const string &scan_plan_endpoint, const string &filter_json) {
	string url = scan_plan_endpoint + "/v1/catalogs/" + catalog_name + "/namespaces/" + schema_name + "/tables/" +
	             table_name + "/plan";
	// TODO: remove hard coded case-sensitive?
	string body = filter_json.empty() ? "{\"case-sensitive\":false}"
	                                  : "{\"case-sensitive\":false,\"filter\":" + filter_json + "}";
	UC_LOG_DEBUG(ctx, "scan-plan.PlanTableScan %s.%s.%s filter=%s", catalog_name, schema_name, table_name,
	             filter_json.empty() ? "(none)" : filter_json);
	auto resp = MakeRequest(ctx, url, credentials.token, body);
	auto result = ParseScanPlanResponse(resp);
	UC_LOG_DEBUG(ctx,
	             "scan-plan.PlanTableScan %s.%s.%s -> status=%s plan_id=%s inline=%zu plan_tasks=%zu delete=%zu%s%s",
	             catalog_name, schema_name, table_name, UCScanPlanStatusToString(result.status), result.plan_id.c_str(),
	             result.file_scan_tasks.size(), result.plan_tasks.size(), result.delete_files.size(),
	             result.status == UCScanPlanStatus::FAILED ? " error=" : "",
	             result.status == UCScanPlanStatus::FAILED ? result.error_message.c_str() : "");

	constexpr int POLL_COUNT_MAX = 20;
	constexpr int POLL_SLEEP_MS = 500;
	for (int i = 0; i < POLL_COUNT_MAX && result.status == UCScanPlanStatus::SUBMITTED; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(POLL_SLEEP_MS));
		result = FetchPlanningResult(ctx, catalog_name, schema_name, table_name, result.plan_id, credentials,
		                             scan_plan_endpoint);
	}

	return result;
}

UCScanPlanResult UCAPI::FetchScanTasks(ClientContext &ctx, const string &catalog_name, const string &schema_name,
                                       const string &table_name, const string &plan_task,
                                       const UCCredentials &credentials, const string &scan_plan_endpoint) {
	string url = scan_plan_endpoint + "/v1/catalogs/" + catalog_name + "/namespaces/" + schema_name + "/tables/" +
	             table_name + "/tasks";
	// Body is {"plan-task":"<token>"}; token is opaque so we escape it as a JSON string manually.
	string escaped;
	for (char c : plan_task) {
		if (c == '"' || c == '\\') {
			escaped += '\\';
		}
		escaped += c;
	}
	string body = "{\"plan-task\":\"" + escaped + "\"}";
	UC_LOG_DEBUG(ctx, "scan-plan.FetchScanTasks %s.%s.%s token=%s", catalog_name, schema_name, table_name, plan_task);
	auto resp = MakeRequest(ctx, url, credentials.token, body);

	// FetchScanTasksResult is a bare ScanTasks object — no status field.
	YYJsonDoc doc(resp);
	auto *root = doc.Root();
	if (!root) {
		throw IOException("Failed to parse fetchScanTasks response from '%s'", url);
	}
	UCScanPlanResult result;
	result.status = UCScanPlanStatus::COMPLETED;
	ParseScanTasksPayload(root, result);
	UC_LOG_DEBUG(ctx, "scan-plan.FetchScanTasks %s.%s.%s token=%s -> inline=%zu plan_tasks=%zu delete=%zu",
	             catalog_name, schema_name, table_name, plan_task, result.file_scan_tasks.size(),
	             result.plan_tasks.size(), result.delete_files.size());
	return result;
}

} // namespace duckdb
