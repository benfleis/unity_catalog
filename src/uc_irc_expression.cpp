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

static string ExprToIRCJson(const Expression &expr, const LogicalGet &get);

static string ExprToIRCJson(const Expression &expr, const LogicalGet &get) {
	switch (expr.GetExpressionClass()) {

	case ExpressionClass::BOUND_COMPARISON: {
		auto &cmp = reinterpret_cast<const BoundComparisonExpression &>(expr);

		const BoundColumnRefExpression *col = nullptr;
		const BoundConstantExpression  *con = nullptr;
		bool flipped = false;

		if (cmp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    cmp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col = reinterpret_cast<const BoundColumnRefExpression *>(cmp.left.get());
			con = reinterpret_cast<const BoundConstantExpression *>(cmp.right.get());
		} else if (cmp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		           cmp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
			col     = reinterpret_cast<const BoundColumnRefExpression *>(cmp.right.get());
			con     = reinterpret_cast<const BoundConstantExpression *>(cmp.left.get());
			flipped = true;
		} else {
			return "{\"type\":\"true\"}";
		}

		ExpressionType effective_type = flipped ? FlipComparisonExpression(expr.type) : expr.type;
		const char *irc_type = nullptr;
		switch (effective_type) {
		case ExpressionType::COMPARE_EQUAL:             irc_type = "eq";     break;
		case ExpressionType::COMPARE_NOTEQUAL:          irc_type = "not-eq"; break;
		case ExpressionType::COMPARE_LESSTHAN:          irc_type = "lt";     break;
		case ExpressionType::COMPARE_GREATERTHAN:       irc_type = "gt";     break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO: irc_type = "lt-eq";  break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO: irc_type = "gt-eq"; break;
		default: return "{\"type\":\"true\"}";
		}

		idx_t col_idx = col->binding.column_index;
		if (col_idx >= get.names.size()) {
			return "{\"type\":\"true\"}";
		}
		string val_json = ValueToIRCJson(con->value);
		if (val_json.empty()) {
			return "{\"type\":\"true\"}";
		}
		return string("{\"type\":\"") + irc_type + "\",\"term\":\"" + get.names[col_idx] +
		       "\",\"value\":" + val_json + "}";
	}

	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = reinterpret_cast<const BoundConjunctionExpression &>(expr);
		const char *irc_type = (expr.type == ExpressionType::CONJUNCTION_AND) ? "and" : "or";

		if (conj.children.empty()) {
			return "{\"type\":\"true\"}";
		}
		if (conj.children.size() == 1) {
			return ExprToIRCJson(*conj.children[0], get);
		}
		string result = ExprToIRCJson(*conj.children[0], get);
		for (idx_t i = 1; i < conj.children.size(); i++) {
			result = string("{\"type\":\"") + irc_type + "\",\"left\":" + result +
			         ",\"right\":" + ExprToIRCJson(*conj.children[i], get) + "}";
		}
		return result;
	}

	case ExpressionClass::BOUND_OPERATOR: {
		if (expr.type != ExpressionType::OPERATOR_IS_NULL &&
		    expr.type != ExpressionType::OPERATOR_IS_NOT_NULL) {
			return "{\"type\":\"true\"}";
		}
		auto &op = reinterpret_cast<const BoundOperatorExpression &>(expr);
		if (op.children.size() != 1 ||
		    op.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return "{\"type\":\"true\"}";
		}
		auto &col = reinterpret_cast<const BoundColumnRefExpression &>(*op.children[0]);
		if (col.binding.column_index >= get.names.size()) {
			return "{\"type\":\"true\"}";
		}
		const char *irc_type = (expr.type == ExpressionType::OPERATOR_IS_NULL) ? "is-null" : "not-null";
		return string("{\"type\":\"") + irc_type + "\",\"term\":\"" +
		       get.names[col.binding.column_index] + "\"}";
	}

	default:
		return "{\"type\":\"true\"}";
	}
}

string SerializeFiltersToIRC(const vector<unique_ptr<Expression>> &filters, const LogicalGet &get) {
	if (filters.empty()) {
		return "";
	}
	if (filters.size() == 1) {
		return ExprToIRCJson(*filters[0], get);
	}
	string result = ExprToIRCJson(*filters[0], get);
	for (idx_t i = 1; i < filters.size(); i++) {
		result = "{\"type\":\"and\",\"left\":" + result +
		         ",\"right\":" + ExprToIRCJson(*filters[i], get) + "}";
	}
	return result;
}

} // namespace duckdb
