#include "uc_irc_expression.hpp"

#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

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

// Returns an IRC Expression JSON string, or "" if the expression cannot be
// serialized (caller treats "" as "omit this term").
static string ExprToIRCJson(const Expression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COMPARISON: {
		auto &cmp = reinterpret_cast<const BoundComparisonExpression &>(expr);

		const BoundColumnRefExpression *col_ref = nullptr;
		const BoundConstantExpression *const_ = nullptr;
		bool flipped = false;

		if (cmp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    cmp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col_ref = reinterpret_cast<const BoundColumnRefExpression *>(cmp.left.get());
			const_ = reinterpret_cast<const BoundConstantExpression *>(cmp.right.get());
		} else if (cmp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		           cmp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col_ref = reinterpret_cast<const BoundColumnRefExpression *>(cmp.right.get());
			const_ = reinterpret_cast<const BoundConstantExpression *>(cmp.left.get());
			flipped = true;
		} else {
			return "";
		}

		ExpressionType effective_type = flipped ? FlipComparisonExpression(expr.type) : expr.type;
		const char *irc_type = nullptr;
		switch (effective_type) {
		case ExpressionType::COMPARE_EQUAL:
			irc_type = "eq";
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			irc_type = "not-eq";
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			irc_type = "lt";
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			irc_type = "gt";
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			irc_type = "lt-eq";
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			irc_type = "gt-eq";
			break;
		default:
			return "";
		}

		const string &col_name = col_ref->GetName();
		if (col_name.empty()) {
			return "";
		}
		string val_json = ValueToIRCJson(const_->value);
		if (val_json.empty()) {
			return "";
		}
		return string("{\"type\":\"") + irc_type + "\",\"term\":\"" + col_name + "\",\"value\":" + val_json + "}";
	}

	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = reinterpret_cast<const BoundConjunctionExpression &>(expr);
		bool is_and = (expr.type == ExpressionType::CONJUNCTION_AND);
		const char *irc_type = is_and ? "and" : "or";

		vector<string> parts;
		for (auto &child : conj.children) {
			string s = ExprToIRCJson(*child);
			if (s.empty()) {
				if (!is_and) {
					return ""; // OR with an unsupported child = vacuously true; drop whole OR
				}
				// AND with an unsupported child: skip it
			} else {
				parts.push_back(std::move(s));
			}
		}
		if (parts.empty()) {
			return "";
		}
		if (parts.size() == 1) {
			return parts[0];
		}
		string result = parts[0];
		for (idx_t i = 1; i < parts.size(); i++) {
			result = string("{\"type\":\"") + irc_type + "\",\"left\":" + result + ",\"right\":" + parts[i] + "}";
		}
		return result;
	}

	case ExpressionClass::BOUND_OPERATOR: {
		if (expr.type != ExpressionType::OPERATOR_IS_NULL && expr.type != ExpressionType::OPERATOR_IS_NOT_NULL) {
			return "";
		}
		auto &op = reinterpret_cast<const BoundOperatorExpression &>(expr);
		if (op.children.size() != 1 || op.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return "";
		}
		auto &col = reinterpret_cast<const BoundColumnRefExpression &>(*op.children[0]);
		const string &col_name = col.GetName();
		if (col_name.empty()) {
			return "";
		}
		const char *irc_type = (expr.type == ExpressionType::OPERATOR_IS_NULL) ? "is-null" : "not-null";
		return string("{\"type\":\"") + irc_type + "\",\"term\":\"" + col_name + "\"}";
	}

	default:
		return "";
	}
}

string SerializeFiltersToIRC(const vector<unique_ptr<Expression>> &filters) {
	if (filters.empty()) {
		return "";
	}
	vector<string> parts(filters.size());
	for (auto &f : filters) {
		string s = ExprToIRCJson(*f);
		if (!s.empty()) {
			parts.push_back(std::move(s));
		}
	}
	if (parts.empty()) {
		return "";
	}
	if (parts.size() == 1) {
		return parts[0];
	}
	string result = parts[0];
	for (idx_t i = 1; i < parts.size(); i++) {
		result = "{\"type\":\"and\",\"left\":" + result + ",\"right\":" + parts[i] + "}";
	}
	return result;
}

} // namespace duckdb
