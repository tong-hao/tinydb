#include "executor.h"
#include "common/logger.h"
#include <algorithm>
#include <cctype>

namespace tinydb {
namespace engine {

// Evaluate WHERE condition
bool Executor::evaluateWhereCondition(const storage::Tuple& tuple, const sql::Expression* condition, const storage::Schema* schema) {
    if (!condition) return true;

    // 处理比较表达式
    if (auto comp_expr = dynamic_cast<const sql::ComparisonExpr*>(condition)) {
        auto left = comp_expr->left();
        auto right = comp_expr->right();

        // 获取左值（列引用）
        std::string left_val;
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            // 使用 toString() 获取字段的字符串表示（包含整数等）
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号（如果是字符串类型）
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        } else if (auto literal = dynamic_cast<const sql::LiteralExpr*>(left)) {
            left_val = literal->value();
            // 去除引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 获取右值（字面量）
        std::string right_val;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(right)) {
            right_val = literal->value();
            // 去除引号
            if (right_val.size() >= 2 && right_val.front() == '\'' && right_val.back() == '\'') {
                right_val = right_val.substr(1, right_val.size() - 2);
            }
        } else if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(right)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            right_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (right_val.size() >= 2 && right_val.front() == '\'' && right_val.back() == '\'') {
                right_val = right_val.substr(1, right_val.size() - 2);
            }
        }

        // 比较操作
        switch (comp_expr->op()) {
            case sql::OpType::EQ:
                return left_val == right_val;
            case sql::OpType::NE:
                return left_val != right_val;
            case sql::OpType::LT:
                try {
                    return std::stod(left_val) < std::stod(right_val);
                } catch (...) {
                    return left_val < right_val;
                }
            case sql::OpType::LE:
                try {
                    return std::stod(left_val) <= std::stod(right_val);
                } catch (...) {
                    return left_val <= right_val;
                }
            case sql::OpType::GT:
                try {
                    return std::stod(left_val) > std::stod(right_val);
                } catch (...) {
                    return left_val > right_val;
                }
            case sql::OpType::GE:
                try {
                    return std::stod(left_val) >= std::stod(right_val);
                } catch (...) {
                    return left_val >= right_val;
                }
            default:
                return false;
        }
    }

    // 处理逻辑表达式
    if (auto logic_expr = dynamic_cast<const sql::LogicalExpr*>(condition)) {
        auto left_result = Executor::evaluateWhereCondition(tuple, logic_expr->left(), schema);

        if (logic_expr->op() == sql::OpType::AND) {
            if (!left_result) return false;
            return Executor::evaluateWhereCondition(tuple, logic_expr->right(), schema);
        } else if (logic_expr->op() == sql::OpType::OR) {
            if (left_result) return true;
            return Executor::evaluateWhereCondition(tuple, logic_expr->right(), schema);
        } else if (logic_expr->op() == sql::OpType::NOT) {
            return !left_result;
        }
    }

    // 处理 IN 表达式
    if (auto in_expr = dynamic_cast<const sql::InExpr*>(condition)) {
        // 获取左值（列引用）
        std::string left_val;
        auto left = in_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 检查是否在值列表中
        bool found = false;
        for (const auto& val_expr : in_expr->values()) {
            std::string val;
            if (auto literal = dynamic_cast<const sql::LiteralExpr*>(val_expr.get())) {
                val = literal->value();
                // 去除引号
                if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                    val = val.substr(1, val.size() - 2);
                }
            }
            if (left_val == val) {
                found = true;
                break;
            }
        }

        return in_expr->isNot() ? !found : found;
    }

    // 处理 BETWEEN 表达式
    if (auto between_expr = dynamic_cast<const sql::BetweenExpr*>(condition)) {
        // 获取左值（列引用）
        std::string left_val;
        auto left = between_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 获取下界值
        std::string lower_val;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(between_expr->lower())) {
            lower_val = literal->value();
            if (lower_val.size() >= 2 && lower_val.front() == '\'' && lower_val.back() == '\'') {
                lower_val = lower_val.substr(1, lower_val.size() - 2);
            }
        }

        // 获取上界值
        std::string upper_val;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(between_expr->upper())) {
            upper_val = literal->value();
            if (upper_val.size() >= 2 && upper_val.front() == '\'' && upper_val.back() == '\'') {
                upper_val = upper_val.substr(1, upper_val.size() - 2);
            }
        }

        // 比较：left >= lower AND left <= upper
        bool in_range = false;
        try {
            double left_num = std::stod(left_val);
            double lower_num = std::stod(lower_val);
            double upper_num = std::stod(upper_val);
            in_range = (left_num >= lower_num && left_num <= upper_num);
        } catch (...) {
            // 字符串比较
            in_range = (left_val >= lower_val && left_val <= upper_val);
        }

        return between_expr->isNot() ? !in_range : in_range;
    }

    // 处理 LIKE 表达式
    if (auto like_expr = dynamic_cast<const sql::LikeExpr*>(condition)) {
        // 获取左值（列引用）
        std::string left_val;
        auto left = like_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            left_val = tuple.getField(col_idx).toString();
            // 去除字符串的引号
            if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
                left_val = left_val.substr(1, left_val.size() - 2);
            }
        }

        // 获取模式值
        std::string pattern;
        if (auto literal = dynamic_cast<const sql::LiteralExpr*>(like_expr->pattern())) {
            pattern = literal->value();
            if (pattern.size() >= 2 && pattern.front() == '\'' && pattern.back() == '\'') {
                pattern = pattern.substr(1, pattern.size() - 2);
            }
        }

        // 简单的 LIKE 模式匹配（支持 % 和 _）
        bool matches = Executor::matchLikePattern(left_val, pattern);
        return like_expr->isNot() ? !matches : matches;
    }

    // 处理 IS NULL 表达式
    if (auto is_null_expr = dynamic_cast<const sql::IsNullExpr*>(condition)) {
        // 获取左值（列引用）
        bool is_null = false;
        auto left = is_null_expr->left();
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(left)) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx < 0) return false;
            // 获取字段并检查是否为空
            const auto& field = tuple.getField(col_idx);
            is_null = field.isNull();
        }

        return is_null_expr->isNot() ? !is_null : is_null;
    }

    return true;
}

// 格式化元组为字符串，支持选择特定列
std::string Executor::formatTuple(const storage::Tuple& tuple, const std::vector<std::unique_ptr<sql::Expression>>& select_list, const storage::Schema* schema) {
    // 如果没有指定列或者 select_list 为空，返回所有列
    if (select_list.empty()) {
        return tuple.toString();
    }

    // 检查是否是 * (所有列)
    bool is_select_all = false;
    for (const auto& expr : select_list) {
        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(expr.get())) {
            if (col_ref->columnName() == "*") {
                is_select_all = true;
                break;
            }
        }
    }

    if (is_select_all) {
        return tuple.toString();
    }

    // 构建指定列的结果
    std::string result = "(";
    for (size_t i = 0; i < select_list.size(); ++i) {
        if (i > 0) result += ", ";

        if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(select_list[i].get())) {
            int col_idx = schema->findColumnIndex(col_ref->columnName());
            if (col_idx >= 0 && col_idx < static_cast<int>(tuple.getFieldCount())) {
                result += tuple.getField(col_idx).toString();
            } else {
                result += "NULL";
            }
        } else if (auto literal = dynamic_cast<const sql::LiteralExpr*>(select_list[i].get())) {
            result += literal->value();
        } else if (auto alias_expr = dynamic_cast<const sql::AliasExpr*>(select_list[i].get())) {
            // 处理别名表达式 (expr AS alias)
            storage::Field field = evaluateExpression(alias_expr->expression(), tuple, schema);
            result += field.toString();
        } else {
            // 处理其他表达式类型（算术运算、比较等）
            storage::Field field = evaluateExpression(select_list[i].get(), tuple, schema);
            result += field.toString();
        }
    }
    result += ")";
    return result;
}

storage::DataType Executor::parseDataType(const std::string& type_str) {
    std::string upper_type = type_str;
    for (auto& c : upper_type) {
        c = std::toupper(c);
    }

    // 提取基本类型（去掉括号部分）
    size_t paren_pos = upper_type.find('(');
    std::string base_type = (paren_pos != std::string::npos) ?
                            upper_type.substr(0, paren_pos) : upper_type;

    if (base_type == "INT" || base_type == "INTEGER") {
        return storage::DataType::INT32;
    } else if (base_type == "BIGINT") {
        return storage::DataType::INT64;
    } else if (base_type == "FLOAT" || base_type == "REAL") {
        return storage::DataType::FLOAT;
    } else if (base_type == "DOUBLE") {
        return storage::DataType::DOUBLE;
    } else if (base_type == "DECIMAL" || base_type == "NUMERIC") {
        return storage::DataType::DECIMAL;
    } else if (base_type == "BOOL" || base_type == "BOOLEAN") {
        return storage::DataType::BOOL;
    } else if (base_type == "CHAR") {
        return storage::DataType::CHAR;
    } else if (base_type == "VARCHAR") {
        return storage::DataType::VARCHAR;
    }

    // 默认类型
    return storage::DataType::VARCHAR;
}

// 解析类型长度（如 VARCHAR(32) 返回 32）
uint32_t Executor::parseTypeLength(const std::string& type_str) {
    size_t paren_pos = type_str.find('(');
    if (paren_pos != std::string::npos) {
        size_t close_pos = type_str.find(')', paren_pos);
        if (close_pos != std::string::npos) {
            std::string len_str = type_str.substr(paren_pos + 1, close_pos - paren_pos - 1);
            try {
                return static_cast<uint32_t>(std::stoul(len_str));
            } catch (...) {
                return 0;
            }
        }
    }
    // 对于没有长度指定的类型，返回默认值
    return 0;
}

// 表达式求值（阶段三新增：支持算术运算）
storage::Field Executor::evaluateExpression(const sql::Expression* expr, const storage::Tuple& tuple, const storage::Schema* schema) {
    if (!expr) {
        return storage::Field();
    }

    // 处理字面量
    if (auto literal = dynamic_cast<const sql::LiteralExpr*>(expr)) {
        std::string val = literal->value();
        // 去除引号
        if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
            val = val.substr(1, val.size() - 2);
        }
        // 尝试解析为数字
        try {
            // 先尝试整数
            return storage::Field(static_cast<int64_t>(std::stoll(val)));
        } catch (...) {
            // 再尝试浮点数
            try {
                return storage::Field(std::stod(val));
            } catch (...) {
                return storage::Field(val, storage::DataType::VARCHAR);
            }
        }
    }

    // 处理列引用
    if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(expr)) {
        int col_idx = schema->findColumnIndex(col_ref->columnName());
        if (col_idx >= 0) {
            return tuple.getField(col_idx);
        }
        return storage::Field();
    }

    // 处理二元操作表达式
    if (auto binary_expr = dynamic_cast<const sql::BinaryOpExpr*>(expr)) {
        auto left_val = evaluateExpression(binary_expr->left(), tuple, schema);
        auto right_val = evaluateExpression(binary_expr->right(), tuple, schema);

        // 检查是否有浮点类型
        bool has_float = (left_val.getType() == storage::DataType::FLOAT ||
                          left_val.getType() == storage::DataType::DOUBLE ||
                          left_val.getType() == storage::DataType::DECIMAL ||
                          right_val.getType() == storage::DataType::FLOAT ||
                          right_val.getType() == storage::DataType::DOUBLE ||
                          right_val.getType() == storage::DataType::DECIMAL);

        if (has_float) {
            // 使用浮点数运算
            double left_num = 0.0, right_num = 0.0;
            if (!left_val.isNull()) {
                if (left_val.getType() == storage::DataType::FLOAT ||
                    left_val.getType() == storage::DataType::DOUBLE ||
                    left_val.getType() == storage::DataType::DECIMAL) {
                    left_num = left_val.getDouble();
                } else {
                    try {
                        left_num = std::stod(left_val.toString());
                    } catch (...) {
                        left_num = static_cast<double>(left_val.getInt64());
                    }
                }
            }
            if (!right_val.isNull()) {
                if (right_val.getType() == storage::DataType::FLOAT ||
                    right_val.getType() == storage::DataType::DOUBLE ||
                    right_val.getType() == storage::DataType::DECIMAL) {
                    right_num = right_val.getDouble();
                } else {
                    try {
                        right_num = std::stod(right_val.toString());
                    } catch (...) {
                        right_num = static_cast<double>(right_val.getInt64());
                    }
                }
            }

            switch (binary_expr->op()) {
                case sql::OpType::ADD:
                    return storage::Field(left_num + right_num);
                case sql::OpType::SUB:
                    return storage::Field(left_num - right_num);
                case sql::OpType::MUL:
                    return storage::Field(left_num * right_num);
                case sql::OpType::DIV:
                    if (right_num != 0.0) {
                        return storage::Field(left_num / right_num);
                    }
                    return storage::Field(0.0);
                default:
                    return storage::Field();
            }
        } else {
            // 使用整数运算
            int64_t left_num = 0, right_num = 0;
            try {
                left_num = std::stoll(left_val.toString());
            } catch (...) {
                left_num = left_val.getInt64();
            }
            try {
                right_num = std::stoll(right_val.toString());
            } catch (...) {
                right_num = right_val.getInt64();
            }

            switch (binary_expr->op()) {
                case sql::OpType::ADD:
                    return storage::Field(left_num + right_num);
                case sql::OpType::SUB:
                    return storage::Field(left_num - right_num);
                case sql::OpType::MUL:
                    return storage::Field(left_num * right_num);
                case sql::OpType::DIV:
                    if (right_num != 0) {
                        return storage::Field(left_num / right_num);
                    }
                    return storage::Field(static_cast<int64_t>(0));
                default:
                    return storage::Field();
            }
        }
    }

    return storage::Field();
}

// Lock management helper functions
bool Executor::acquireTableLock(storage::StorageEngine* storage_engine,
                                storage::Transaction* txn,
                                const std::string& table_name, 
                                storage::LockType lock_type) {
    if (!storage_engine) {
        return false;
    }

    if (!txn) {
        LOG_ERROR("Cannot acquire lock without active transaction");
        return false;
    }

    return storage_engine->lockTable(txn, table_name, lock_type);
}

bool Executor::releaseTableLock(storage::StorageEngine* storage_engine,
                                storage::Transaction* txn,
                                const std::string& table_name) {
    if (!storage_engine || !txn) {
        return false;
    }

    return storage_engine->unlockTable(txn, table_name);
}

// LIKE 模式匹配辅助函数
bool Executor::matchLikePattern(const std::string& value, const std::string& pattern) {
    // 将 SQL LIKE 模式转换为类似 glob 的匹配
    // % 匹配任意字符序列，_ 匹配单个字符

    size_t val_idx = 0;
    size_t pat_idx = 0;
    size_t val_len = value.size();
    size_t pat_len = pattern.size();

    // 用于处理 % 的回溯位置
    size_t val_backup = 0;
    size_t pat_backup = 0;
    bool has_backup = false;

    while (val_idx < val_len) {
        if (pat_idx < pat_len) {
            char pat_char = pattern[pat_idx];

            if (pat_char == '%') {
                // % 匹配零个或多个字符
                pat_idx++;
                // 记录回溯位置
                val_backup = val_idx;
                pat_backup = pat_idx;
                has_backup = true;
                continue;
            } else if (pat_char == '_') {
                // _ 匹配单个字符
                pat_idx++;
                val_idx++;
                continue;
            } else if (pat_char == value[val_idx]) {
                // 精确匹配
                pat_idx++;
                val_idx++;
                continue;
            }
        }

        // 当前不匹配，尝试回溯
        if (has_backup && val_backup < val_len) {
            val_backup++;
            val_idx = val_backup;
            pat_idx = pat_backup;
            continue;
        }

        // 无法匹配
        return false;
    }

    // 跳过模式末尾的 %
    while (pat_idx < pat_len && pattern[pat_idx] == '%') {
        pat_idx++;
    }

    // 如果模式也处理完毕，则匹配成功
    return pat_idx == pat_len;
}

// Phase 4: 多表 WHERE 条件评估
bool Executor::evaluateJoinWhereCondition(const std::vector<storage::Tuple>& tuples,
                                          const std::vector<storage::Schema*>& schemas,
                                          const sql::Expression* condition) {
    if (!condition) return true;

    // 处理比较表达式
    if (auto comp_expr = dynamic_cast<const sql::ComparisonExpr*>(condition)) {
        auto left = comp_expr->left();
        auto right = comp_expr->right();

        // 获取左值
        storage::Field left_field = evaluateJoinExpression(left, tuples, schemas);
        storage::Field right_field = evaluateJoinExpression(right, tuples, schemas);

        std::string left_val = left_field.toString();
        std::string right_val = right_field.toString();

        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }
        if (right_val.size() >= 2 && right_val.front() == '\'' && right_val.back() == '\'') {
            right_val = right_val.substr(1, right_val.size() - 2);
        }

        // 比较操作
        switch (comp_expr->op()) {
            case sql::OpType::EQ:
                return left_val == right_val;
            case sql::OpType::NE:
                return left_val != right_val;
            case sql::OpType::LT:
                try {
                    return std::stod(left_val) < std::stod(right_val);
                } catch (...) {
                    return left_val < right_val;
                }
            case sql::OpType::LE:
                try {
                    return std::stod(left_val) <= std::stod(right_val);
                } catch (...) {
                    return left_val <= right_val;
                }
            case sql::OpType::GT:
                try {
                    return std::stod(left_val) > std::stod(right_val);
                } catch (...) {
                    return left_val > right_val;
                }
            case sql::OpType::GE:
                try {
                    return std::stod(left_val) >= std::stod(right_val);
                } catch (...) {
                    return left_val >= right_val;
                }
            default:
                return false;
        }
    }

    // 处理逻辑表达式
    if (auto logic_expr = dynamic_cast<const sql::LogicalExpr*>(condition)) {
        auto left_result = evaluateJoinWhereCondition(tuples, schemas, logic_expr->left());

        if (logic_expr->op() == sql::OpType::AND) {
            if (!left_result) return false;
            return evaluateJoinWhereCondition(tuples, schemas, logic_expr->right());
        } else if (logic_expr->op() == sql::OpType::OR) {
            if (left_result) return true;
            return evaluateJoinWhereCondition(tuples, schemas, logic_expr->right());
        } else if (logic_expr->op() == sql::OpType::NOT) {
            return !left_result;
        }
    }

    // 处理 IN 表达式
    if (auto in_expr = dynamic_cast<const sql::InExpr*>(condition)) {
        // 获取左值
        storage::Field left_field = evaluateJoinExpression(in_expr->left(), tuples, schemas);
        std::string left_val = left_field.toString();
        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }

        // 检查是否在值列表中
        bool found = false;
        for (const auto& val_expr : in_expr->values()) {
            storage::Field val_field = evaluateJoinExpression(val_expr.get(), tuples, schemas);
            std::string val = val_field.toString();
            // 去除引号
            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                val = val.substr(1, val.size() - 2);
            }
            if (left_val == val) {
                found = true;
                break;
            }
        }

        return in_expr->isNot() ? !found : found;
    }

    // 处理 BETWEEN 表达式
    if (auto between_expr = dynamic_cast<const sql::BetweenExpr*>(condition)) {
        // 获取左值
        storage::Field left_field = evaluateJoinExpression(between_expr->left(), tuples, schemas);
        std::string left_val = left_field.toString();
        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }

        // 获取下界和上界值
        storage::Field lower_field = evaluateJoinExpression(between_expr->lower(), tuples, schemas);
        storage::Field upper_field = evaluateJoinExpression(between_expr->upper(), tuples, schemas);
        std::string lower_val = lower_field.toString();
        std::string upper_val = upper_field.toString();
        // 去除引号
        if (lower_val.size() >= 2 && lower_val.front() == '\'' && lower_val.back() == '\'') {
            lower_val = lower_val.substr(1, lower_val.size() - 2);
        }
        if (upper_val.size() >= 2 && upper_val.front() == '\'' && upper_val.back() == '\'') {
            upper_val = upper_val.substr(1, upper_val.size() - 2);
        }

        // 比较
        bool in_range = false;
        try {
            double left_num = std::stod(left_val);
            double lower_num = std::stod(lower_val);
            double upper_num = std::stod(upper_val);
            in_range = (left_num >= lower_num && left_num <= upper_num);
        } catch (...) {
            in_range = (left_val >= lower_val && left_val <= upper_val);
        }

        return between_expr->isNot() ? !in_range : in_range;
    }

    // 处理 LIKE 表达式
    if (auto like_expr = dynamic_cast<const sql::LikeExpr*>(condition)) {
        // 获取左值
        storage::Field left_field = evaluateJoinExpression(like_expr->left(), tuples, schemas);
        std::string left_val = left_field.toString();
        // 去除引号
        if (left_val.size() >= 2 && left_val.front() == '\'' && left_val.back() == '\'') {
            left_val = left_val.substr(1, left_val.size() - 2);
        }

        // 获取模式值
        storage::Field pattern_field = evaluateJoinExpression(like_expr->pattern(), tuples, schemas);
        std::string pattern = pattern_field.toString();
        // 去除引号
        if (pattern.size() >= 2 && pattern.front() == '\'' && pattern.back() == '\'') {
            pattern = pattern.substr(1, pattern.size() - 2);
        }

        // LIKE 匹配
        bool matches = matchLikePattern(left_val, pattern);
        return like_expr->isNot() ? !matches : matches;
    }

    // 处理 IS NULL 表达式
    if (auto is_null_expr = dynamic_cast<const sql::IsNullExpr*>(condition)) {
        storage::Field left_field = evaluateJoinExpression(is_null_expr->left(), tuples, schemas);
        bool is_null = left_field.isNull();
        return is_null_expr->isNot() ? !is_null : is_null;
    }

    return true;
}

// Phase 4: JOIN表达式求值
storage::Field Executor::evaluateJoinExpression(const sql::Expression* expr,
                                                const std::vector<storage::Tuple>& tuples,
                                                const std::vector<storage::Schema*>& schemas) {
    if (!expr) {
        return storage::Field();
    }

    // 处理字面量
    if (auto literal = dynamic_cast<const sql::LiteralExpr*>(expr)) {
        std::string val = literal->value();
        if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
            val = val.substr(1, val.size() - 2);
        }
        try {
            return storage::Field(static_cast<int64_t>(std::stoll(val)));
        } catch (...) {
            return storage::Field(val, storage::DataType::VARCHAR);
        }
    }

    // 处理列引用（需要在多个表中查找）
    if (auto col_ref = dynamic_cast<const sql::ColumnRefExpr*>(expr)) {
        std::string col_name = col_ref->columnName();

        // 检查是否是 qualified name (table.column)
        size_t dot_pos = col_name.find('.');
        if (dot_pos != std::string::npos) {
            std::string table_hint = col_name.substr(0, dot_pos);
            std::string actual_col = col_name.substr(dot_pos + 1);

            // 在指定表中查找
            for (size_t i = 0; i < schemas.size(); ++i) {
                // TODO: 简化处理：假设表名在schema中（需要扩展）
                int col_idx = schemas[i]->findColumnIndex(actual_col);
                if (col_idx >= 0 && i < tuples.size()) {
                    return tuples[i].getField(col_idx);
                }
            }
        } else {
            // 在所有表中查找（返回第一个匹配的）
            for (size_t i = 0; i < schemas.size(); ++i) {
                int col_idx = schemas[i]->findColumnIndex(col_name);
                if (col_idx >= 0 && i < tuples.size()) {
                    return tuples[i].getField(col_idx);
                }
            }
        }
        return storage::Field();
    }

    return storage::Field();
}

// Phase 4: JOIN结果格式化
std::string Executor::formatJoinTuple(const std::vector<storage::Tuple>& tuples,
                                      const std::vector<storage::Schema*>& schemas,
                                      const std::vector<std::unique_ptr<sql::Expression>>& select_list) {
    if (select_list.empty()) {
        // 返回所有列
        std::string result = "(";
        for (size_t t = 0; t < tuples.size(); ++t) {
            if (t > 0) result += ", ";
            for (size_t i = 0; i < tuples[t].getFieldCount(); ++i) {
                if (i > 0) result += ", ";
                result += tuples[t].getField(i).toString();
            }
        }
        result += ")";
        return result;
    }

    std::string result = "(";
    for (size_t i = 0; i < select_list.size(); ++i) {
        if (i > 0) result += ", ";

        storage::Field field = evaluateJoinExpression(select_list[i].get(), tuples, schemas);
        result += field.toString();
    }
    result += ")";
    return result;
}

} // namespace engine
} // namespace tinydb
