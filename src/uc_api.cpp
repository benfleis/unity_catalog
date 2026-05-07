#include <chrono>
#include <cstddef>
#include <sys/stat.h>
#include <thread>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "duckdb/common/http_util.hpp"
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
		throw IOException("Request to '%s' failed: '%s'", url, resp->GetError());
	}
	return std::move(resp->body);
}

template <class TYPE, uint8_t TYPE_NUM, TYPE (*get_function)(duckdb_yyjson::yyjson_val *obj)>
static TYPE TemplatedTryGetYYJson(duckdb_yyjson::yyjson_val *obj, const string &field, TYPE default_val,
                                  bool fail_on_missing = true) {
	auto val = yyjson_obj_get(obj, field.c_str());
	if (val && yyjson_get_type(val) == TYPE_NUM) {
		return get_function(val);
	} else if (!fail_on_missing) {
		return default_val;
	}
	throw IOException("Invalid field found while parsing field: " + field);
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

static string GetCredentialsRequest(ClientContext &ctx, const string &url, const string &table_id, bool write = false,
                                    const string &token = "") {
	auto db = ctx.db;
	auto &http_util = HTTPUtil::Get(*db);
	auto params = http_util.InitializeParameters(*db, url);

	string access_type = write ? "READ_WRITE" : "READ";
	string body = StringUtil::Format(R"({"table_id" : "%s", "operation" : "%s"})", table_id, access_type);
	HTTPHeaders hdrs(*db);
	hdrs.Insert("Content-Type", "application/json");
	AuthenticateViaBearerToken(hdrs, token);

	params->logger = ctx.logger;
	PostRequestInfo req(url, hdrs, *params, reinterpret_cast<const_data_ptr_t>(body.c_str()), body.length());
	auto resp = http_util.Request(req);

	if (!resp->Success()) {
		throw IOException("POST Request to '%s' failed: '%s'", url, resp->GetError());
	}
	// Ugh. actual response body not in resp->body, but in req.buffer_out
	return std::move(req.buffer_out);
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

UCAPICommitsResult UCAPI::GetCommits(ClientContext &ctx, const string &table_id, const string &table_uri,
                                     const UCCredentials &credentials) {
	UCAPICommitsResult result;
	string body = StringUtil::Format("{\"start_version\": 0, \"table_id\": \"%s\", \"table_uri\": \"%s\"}",
	                                 table_id.c_str(), table_uri.c_str());
	string url = credentials.endpoint + "/api/2.1/unity-catalog/delta/preview/commits";
	auto api_result = MakeRequest(ctx, url, credentials.token, body, true);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	auto error = CheckError(root);
	if (error.HasError()) {
		error.ThrowError(StringUtil::Format("Failed to get commits for %s", table_id));
	}

	result.latest_table_version = TryGetNumFromObject(root, "latest_table_version", true);

	auto *commits = yyjson_obj_get(root, "commits");
	size_t idx, max;
	duckdb_yyjson::yyjson_val *commit;
	yyjson_arr_foreach(commits, idx, max, commit) {
		UCAPICommit commit_result;
		commit_result.version = TryGetNumFromObject(commit, "version", true);
		commit_result.timestamp = TryGetNumFromObject(commit, "timestamp", true);
		commit_result.file_name = TryGetStrFromObject(commit, "file_name", true);
		commit_result.file_size = TryGetNumFromObject(commit, "file_size", true);
		commit_result.file_modification_timestamp = TryGetNumFromObject(commit, "file_modification_timestamp", true);
		result.commits.push_back(commit_result);
	}

	return result;
}

bool UCAPI::PostCommit(ClientContext &ctx, const string &table_id, const string &table_uri,
                       const UCCredentials &credentials, idx_t version, idx_t timestamp, const string &file_name,
                       idx_t file_size, idx_t file_modification_timestamp) {
	string body = StringUtil::Format(
	    R"({"table_id": "%s", "table_uri": "%s/", "commit_info": {"version": %ld, "timestamp": %ld, "file_name": "%s", "file_size": %ld, "file_modification_timestamp": %ld}})",
	    table_id.c_str(), table_uri.c_str(), version, timestamp, file_name.c_str(), file_size,
	    file_modification_timestamp);
	string url = credentials.endpoint + "/api/2.1/unity-catalog/delta/preview/commits";
	auto api_result = MakeRequest(ctx, url, credentials.token, body);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	auto error = CheckError(root);
	if (error.HasError()) {
		error.ThrowError(StringUtil::Format("Failed to commit to %s", table_id));
	}

	return true;
}

UCAPITableCredentials UCAPI::GetTableCredentials(ClientContext &ctx, const string &table_id, bool write,
                                                 const UCCredentials &credentials) {
	UCAPITableCredentials result;

	auto url = credentials.endpoint + "/api/2.1/unity-catalog/temporary-table-credentials";
	auto api_result = GetCredentialsRequest(ctx, url, table_id, write, credentials.token);

	YYJsonDoc doc(api_result);
	auto *root = doc.Root();

	auto error = CheckError(root);
	if (error.HasError()) {
		error.ThrowError(StringUtil::Format("Failed to get table credentials for table_id: %s", table_id));
	}

	auto *aws_temp_credentials = yyjson_obj_get(root, "aws_temp_credentials");
	if (aws_temp_credentials) {
		result.key_id = TryGetStrFromObject(aws_temp_credentials, "access_key_id");
		result.secret = TryGetStrFromObject(aws_temp_credentials, "secret_access_key");
		result.session_token = TryGetStrFromObject(aws_temp_credentials, "session_token");
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

	return result;
}

vector<UCAPISchema> UCAPI::GetSchemas(ClientContext &ctx, Catalog &catalog, const UCCredentials &credentials) {
	vector<UCAPISchema> result;
	auto url = credentials.endpoint + "/api/2.1/unity-catalog/schemas?catalog_name=" + catalog.GetDBPath();
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

static UCScanPlanResult ParseScanPlanResponse(const string &json_str) {
	YYJsonDoc doc(json_str);
	auto *root = doc.Root();
	if (!root) {
		throw IOException("Failed to parse scan plan response");
	}

	UCScanPlanResult result;
	string status_str = TryGetStrFromObject(root, "status");
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
			// config is an object; store as placeholder for now
			result.storage_credentials.emplace_back(prefix, string());
		}
	}

	return result;
}

// ---------------------------------------------------------------------------
// Scan plan API methods
// ---------------------------------------------------------------------------

UCScanPlanResult UCAPI::FetchPlanningResult(ClientContext &ctx, const string &catalog_name, const string &schema_name,
                                            const string &table_name, const string &plan_id,
                                            const UCCredentials &credentials, const string &scan_plan_endpoint) {
	string url = scan_plan_endpoint + "/v1/catalogs/" + catalog_name + "/namespaces/" + schema_name + "/tables/" +
	             table_name + "/plan/" + plan_id;
	auto resp = MakeRequest(ctx, url, credentials.token);
	return ParseScanPlanResponse(resp);
}

UCScanPlanResult UCAPI::PlanTableScan(ClientContext &ctx, const string &catalog_name, const string &schema_name,
                                      const string &table_name, const UCCredentials &credentials,
                                      const string &scan_plan_endpoint, const string &filter_json) {
	string url = scan_plan_endpoint + "/v1/catalogs/" + catalog_name + "/namespaces/" + schema_name + "/tables/" +
	             table_name + "/plan";
	string body = filter_json.empty() ? "{}" : "{\"filter\":" + filter_json + "}";
	auto resp = MakeRequest(ctx, url, credentials.token, body);
	auto result = ParseScanPlanResponse(resp);

	constexpr int POLL_COUNT_MAX = 20;
	constexpr int POLL_SLEEP_MS = 500;
	for (int i = 0; i < POLL_COUNT_MAX && result.status == UCScanPlanStatus::SUBMITTED; i++) {
		std::this_thread::sleep_for(std::chrono::milliseconds(POLL_SLEEP_MS));
		result = FetchPlanningResult(ctx, catalog_name, schema_name, table_name, result.plan_id, credentials,
		                             scan_plan_endpoint);
	}

	return result;
}

} // namespace duckdb
