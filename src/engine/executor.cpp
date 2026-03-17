#include "executor.h"
#include "common/logger.h"
#include <sstream>
#include <algorithm>
#include <cctype>

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
    }

    return ExecutionResult::error("Unsupported statement type");
}

ExecutionResult Executor::execute(const std::string& sql) {
    LOG_INFO("Executing SQL: " << sql);

    // 简单的 SQL 类型检测
    std::string upper_sql = sql;
    for (auto& c : upper_sql) {
        c = std::toupper(c);
    }

    std::istringstream iss(upper_sql);
    std::string first_word;
    iss >> first_word;

    if (first_word == "SELECT") {
        return executeSelect(nullptr);
    } else if (first_word == "INSERT") {
        return executeInsert(nullptr);
    } else if (first_word == "CREATE") {
        return executeCreateTable(nullptr);
    } else if (first_word == "DROP") {
        return executeDropTable(nullptr);
    } else {
        return ExecutionResult::ok("Command executed (simulated)");
    }
}

ExecutionResult Executor::executeSelect(const sql::SelectStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("SELECT executed (simulated - no storage engine)");
    }

    // 阶段二：支持从pg_class查询表信息
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
