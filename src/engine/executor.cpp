#include "executor.h"
#include "common/logger.h"
#include <sstream>
#include <algorithm>
#include <cctype>
#include "sql/lexer.h"
#include "sql/parser.h"

namespace tinydb {
namespace engine {

Executor::Executor() {
    LOG_INFO("Executor initialized");
}

Executor::~Executor() {
}

void Executor::initialize(storage::StorageEngine* storage_engine) {
    storage_engine_ = storage_engine;
    LOG_INFO("Executor bound to storage engine");
}

ExecutionResult Executor::execute(const sql::AST& ast) {
    if (!ast.statement()) {
        return ExecutionResult::error("Empty statement");
    }

    const sql::Statement* stmt = ast.statement();

    // 根据语句类型分发执行
    if (auto select_stmt = dynamic_cast<const sql::SelectStmt*>(stmt)) {
        return executeSelect(select_stmt);
    } else if (auto insert_stmt = dynamic_cast<const sql::InsertStmt*>(stmt)) {
        return executeInsert(insert_stmt);
    } else if (auto create_stmt = dynamic_cast<const sql::CreateTableStmt*>(stmt)) {
        return executeCreateTable(create_stmt);
    } else if (auto drop_stmt = dynamic_cast<const sql::DropTableStmt*>(stmt)) {
        return executeDropTable(drop_stmt);
    } else if (auto update_stmt = dynamic_cast<const sql::UpdateStmt*>(stmt)) {
        return executeUpdate(update_stmt);
    } else if (auto delete_stmt = dynamic_cast<const sql::DeleteStmt*>(stmt)) {
        return executeDelete(delete_stmt);
    } else if (auto alter_stmt = dynamic_cast<const sql::AlterTableStmt*>(stmt)) {
        return executeAlterTable(alter_stmt);
    }

    return ExecutionResult::error("Unsupported statement type");
}

ExecutionResult Executor::execute(const std::string &sql)
{
    LOG_INFO("Executing SQL: " << sql);

    // 使用 Lexer + Parser 解析 SQL
    ExecutionResult result;
    try
    {
        sql::Lexer lexer(sql);
        auto tokens = lexer.tokenize();

        sql::Parser parser(tokens);
        auto ast = parser.parse();

        if (ast && ast->statement())
        {
            // 通过 AST 执行
            LOG_INFO("SQL parsed successfully, executing via AST");
            result = execute(*ast);
        }
        else
        {
            // 解析失败，直接报错
            LOG_ERROR("SQL parse failed: " + parser.error());
            result = ExecutionResult::error("SQL parse error: " + parser.error());
        }
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("SQL execution error: " << e.what());
        result = ExecutionResult::error("SQL execution error: " + std::string(e.what()));
    }

    return result;
}

// 评估 WHERE 条件
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
        auto left_result = evaluateWhereCondition(tuple, logic_expr->left(), schema);

        if (logic_expr->op() == sql::OpType::AND) {
            if (!left_result) return false;
            return evaluateWhereCondition(tuple, logic_expr->right(), schema);
        } else if (logic_expr->op() == sql::OpType::OR) {
            if (left_result) return true;
            return evaluateWhereCondition(tuple, logic_expr->right(), schema);
        } else if (logic_expr->op() == sql::OpType::NOT) {
            return !left_result;
        }
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
        } else {
            result += "?";
        }
    }
    result += ")";
    return result;
}

ExecutionResult Executor::executeSelect(const sql::SelectStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("SELECT executed (simulated - no storage engine)");
    }

    std::string table_name = stmt ? stmt->fromTable() : "";

    if (table_name == "pg_class" || table_name == "pg_attribute") {
        // 查询系统表
        std::string result = "Table: " + table_name + "\n";
        result += "--------------------\n";

        auto iter = storage_engine_->scan(table_name);
        int count = 0;
        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            result += tuple.toString() + "\n";
            count++;
        }

        result += "--------------------\n";
        result += "Total rows: " + std::to_string(count);

        return ExecutionResult::ok(result);
    }

    // 查询用户表
    if (!table_name.empty()) {
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        std::string result = "Table: " + table_name + "\n";
        result += "--------------------\n";

        auto iter = storage_engine_->scan(table_name);
        int count = 0;

        // 获取 WHERE 条件
        const sql::Expression* where_condition = stmt ? stmt->whereCondition() : nullptr;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();

            // 应用 WHERE 条件过滤
            if (where_condition && !evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 格式化输出，支持选择特定列
            if (stmt) {
                result += formatTuple(tuple, stmt->selectList(), &table_meta->schema) + "\n";
            } else {
                result += tuple.toString() + "\n";
            }
            count++;
        }

        result += "--------------------\n";
        result += "Total rows: " + std::to_string(count);

        return ExecutionResult::ok(result);
    }

    return ExecutionResult::ok("SELECT executed");
}

ExecutionResult Executor::executeInsert(const sql::InsertStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("INSERT executed (simulated - no storage engine)");
    }

    // 阶段二：实际插入数据
    if (stmt) {
        std::string table_name = stmt->table();

        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 构建元组
        storage::Tuple tuple(&table_meta->schema);
        const auto& values = stmt->values();
        const auto& columns = stmt->columns();

        for (size_t i = 0; i < values.size(); ++i) {
            const auto* literal = dynamic_cast<const sql::LiteralExpr*>(values[i].get());
            if (literal) {
                std::string val = literal->value();

                // 确定列索引
                size_t col_idx = i;
                if (i < columns.size()) {
                    col_idx = table_meta->schema.findColumnIndex(columns[i]);
                    if (col_idx == static_cast<size_t>(-1)) {
                        return ExecutionResult::error("Unknown column: " + columns[i]);
                    }
                }

                // 根据列类型创建字段
                const auto& col_def = table_meta->schema.getColumn(col_idx);
                storage::Field field;

                switch (col_def.type) {
                    case storage::DataType::INT32:
                        field = storage::Field(static_cast<int32_t>(std::stoll(val)));
                        break;
                    case storage::DataType::INT64:
                        field = storage::Field(static_cast<int64_t>(std::stoll(val)));
                        break;
                    case storage::DataType::FLOAT:
                        field = storage::Field(static_cast<float>(std::stod(val)));
                        break;
                    case storage::DataType::DOUBLE:
                        field = storage::Field(static_cast<double>(std::stod(val)));
                        break;
                    case storage::DataType::BOOL:
                        field = storage::Field(val == "true" || val == "1");
                        break;
                    case storage::DataType::CHAR:
                    case storage::DataType::VARCHAR:
                        // 去除引号
                        if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                            val = val.substr(1, val.size() - 2);
                        }
                        field = storage::Field(val, col_def.type);
                        break;
                    default:
                        field = storage::Field(val, storage::DataType::VARCHAR);
                        break;
                }

                tuple.setField(col_idx, field);
            }
        }

        // 插入数据
        auto tid = storage_engine_->insert(table_name, tuple);
        if (tid.isValid()) {
            return ExecutionResult::ok("INSERT 1 row at " + tid.toString());
        } else {
            return ExecutionResult::error("Failed to insert row");
        }
    }

    return ExecutionResult::ok("INSERT executed");
}

ExecutionResult Executor::executeCreateTable(const sql::CreateTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("CREATE TABLE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否已存在
        if (storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table already exists: " + table_name);
        }

        // 构建Schema
        storage::Schema schema;
        for (const auto& col : stmt->columns()) {
            storage::DataType type = parseDataType(col.type);
            uint32_t length = 0;

            // 解析长度（如 VARCHAR(32)）
            size_t paren_pos = col.type.find('(');
            if (paren_pos != std::string::npos) {
                size_t close_pos = col.type.find(')', paren_pos);
                if (close_pos != std::string::npos) {
                    std::string len_str = col.type.substr(paren_pos + 1, close_pos - paren_pos - 1);
                    length = static_cast<uint32_t>(std::stoul(len_str));
                }
            }

            schema.addColumn(col.name, type, length);
        }

        // 创建表
        if (storage_engine_->createTable(table_name, schema)) {
            return ExecutionResult::ok("CREATE TABLE " + table_name);
        } else {
            return ExecutionResult::error("Failed to create table: " + table_name);
        }
    }

    return ExecutionResult::ok("CREATE TABLE executed");
}

ExecutionResult Executor::executeDropTable(const sql::DropTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("DROP TABLE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();
        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        // 删除表
        if (storage_engine_->dropTable(table_name)) {
            return ExecutionResult::ok("DROP TABLE " + table_name);
        } else {
            return ExecutionResult::error("Failed to drop table: " + table_name);
        }
    }

    return ExecutionResult::ok("DROP TABLE executed");
}

ExecutionResult Executor::executeUpdate(const sql::UpdateStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("UPDATE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 获取 WHERE 条件
        const sql::Expression* where_condition = stmt->whereCondition();

        // 获取赋值列表
        const auto& assignments = stmt->assignments();

        // 遍历表中的所有行
        auto iter = storage_engine_->scan(table_name);
        int update_count = 0;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            storage::TID tid = iter.getCurrentTID();

            // 应用 WHERE 条件过滤
            if (where_condition && !evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 创建更新后的元组
            storage::Tuple updated_tuple(&table_meta->schema);

            // 先复制原元组的所有字段
            for (size_t i = 0; i < tuple.getFieldCount(); ++i) {
                updated_tuple.addField(tuple.getField(i));
            }

            // 应用赋值
            for (const auto& assignment : assignments) {
                const std::string& col_name = assignment.first;
                const sql::Expression* value_expr = assignment.second.get();

                int col_idx = table_meta->schema.findColumnIndex(col_name);
                if (col_idx < 0) {
                    return ExecutionResult::error("Unknown column: " + col_name);
                }

                const auto& col_def = table_meta->schema.getColumn(col_idx);
                storage::Field field;

                // 获取值
                if (auto literal = dynamic_cast<const sql::LiteralExpr*>(value_expr)) {
                    std::string val = literal->value();

                    switch (col_def.type) {
                        case storage::DataType::INT32:
                            field = storage::Field(static_cast<int32_t>(std::stoll(val)));
                            break;
                        case storage::DataType::INT64:
                            field = storage::Field(static_cast<int64_t>(std::stoll(val)));
                            break;
                        case storage::DataType::FLOAT:
                            field = storage::Field(static_cast<float>(std::stod(val)));
                            break;
                        case storage::DataType::DOUBLE:
                            field = storage::Field(static_cast<double>(std::stod(val)));
                            break;
                        case storage::DataType::BOOL:
                            field = storage::Field(val == "true" || val == "1");
                            break;
                        case storage::DataType::CHAR:
                        case storage::DataType::VARCHAR:
                            // 去除引号
                            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                                val = val.substr(1, val.size() - 2);
                            }
                            field = storage::Field(val, col_def.type);
                            break;
                        default:
                            field = storage::Field(val, storage::DataType::VARCHAR);
                            break;
                    }
                } else {
                    return ExecutionResult::error("Unsupported value expression in UPDATE");
                }

                updated_tuple.setField(col_idx, field);
            }

            // 删除旧元组并插入新元组
            // 注意：这里简化处理，实际应该支持原地更新
            storage_engine_->remove(table_name, tid);
            storage_engine_->insert(table_name, updated_tuple);

            update_count++;
        }

        return ExecutionResult::ok("UPDATE " + std::to_string(update_count) + " row(s) in table " + table_name);
    }

    return ExecutionResult::ok("UPDATE executed");
}

ExecutionResult Executor::executeDelete(const sql::DeleteStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("DELETE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 获取 WHERE 条件
        const sql::Expression* where_condition = stmt->whereCondition();

        // 遍历表中的所有行
        auto iter = storage_engine_->scan(table_name);
        int delete_count = 0;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            storage::TID tid = iter.getCurrentTID();

            // 应用 WHERE 条件过滤
            if (where_condition && !evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 删除元组
            if (storage_engine_->remove(table_name, tid)) {
                delete_count++;
            }
        }

        return ExecutionResult::ok("DELETE " + std::to_string(delete_count) + " row(s) from table " + table_name);
    }

    return ExecutionResult::ok("DELETE executed");
}

ExecutionResult Executor::executeAlterTable(const sql::AlterTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("ALTER TABLE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        // TODO: 实现实际的 ALTER TABLE 逻辑
        // 目前返回模拟结果
        return ExecutionResult::ok("ALTER TABLE executed on table " + table_name);
    }

    return ExecutionResult::ok("ALTER TABLE executed");
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

} // namespace engine
} // namespace tinydb
