#include "alter_table_executor.h"
#include "storage/tuple.h"
#include <functional>

namespace tinydb {
namespace engine {

AlterTableExecutor::AlterTableExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

AlterTableExecutor::~AlterTableExecutor() {}

ExecutionResult AlterTableExecutor::execute(const sql::AlterTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("ALTER TABLE executed (simulated - no storage engine)");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid ALTER TABLE statement");
    }

    std::string table_name = stmt->table();

    // Check if table exists
    if (!storage_engine_->tableExists(table_name)) {
        return ExecutionResult::error("Table does not exist: " + table_name);
    }

    // Execute different operations based on action type
    switch (stmt->action()) {
        case sql::AlterTableStmt::ActionType::ADD_COLUMN: {
            // Add column
            storage::ColumnDef col_def(
                stmt->columnName(),
                parseDataType(stmt->columnType()),
                parseTypeLength(stmt->columnType())
            );

            if (storage_engine_->addColumn(table_name, col_def)) {
                return ExecutionResult::ok("ALTER TABLE ADD COLUMN " + stmt->columnName() +
                                           " " + stmt->columnType() + " ON " + table_name);
            } else {
                return ExecutionResult::error("Failed to add column " + stmt->columnName() +
                                              " to table " + table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::DROP_COLUMN: {
            // Drop column
            if (storage_engine_->dropColumn(table_name, stmt->columnName())) {
                return ExecutionResult::ok("ALTER TABLE DROP COLUMN " + stmt->columnName() +
                                           " FROM " + table_name);
            } else {
                return ExecutionResult::error("Failed to drop column " + stmt->columnName() +
                                              " from table " + table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::MODIFY_COLUMN: {
            // Modify column
            storage::ColumnDef col_def(
                stmt->columnName(),
                parseDataType(stmt->columnType()),
                parseTypeLength(stmt->columnType())
            );

            if (storage_engine_->modifyColumn(table_name, stmt->columnName(), col_def)) {
                return ExecutionResult::ok("ALTER TABLE ALTER COLUMN " + stmt->columnName() +
                                           " TYPE " + stmt->columnType() + " ON " + table_name);
            } else {
                return ExecutionResult::error("Failed to alter column type for " + stmt->columnName() +
                                              " in table " + table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::RENAME_TABLE: {
            // Rename table
            std::string new_table_name = stmt->newTableName();
            if (storage_engine_->renameTable(table_name, new_table_name)) {
                return ExecutionResult::ok("ALTER TABLE RENAME " + table_name + " TO " + new_table_name);
            } else {
                return ExecutionResult::error("Failed to rename table " + table_name + " to " + new_table_name);
            }
        }

        case sql::AlterTableStmt::ActionType::RENAME_COLUMN: {
            // Rename column
            std::string old_col_name = stmt->columnName();
            std::string new_col_name = stmt->newColumnName();
            if (storage_engine_->renameColumn(table_name, old_col_name, new_col_name)) {
                return ExecutionResult::ok("ALTER TABLE RENAME COLUMN " + old_col_name + " TO " + new_col_name);
            } else {
                return ExecutionResult::error("Failed to rename column " + old_col_name + " to " + new_col_name);
            }
        }

        default:
            return ExecutionResult::error("Unknown ALTER TABLE action");
    }
}

storage::DataType AlterTableExecutor::parseDataType(const std::string& type_str) {
    std::string type_lower = type_str;
    std::transform(type_lower.begin(), type_lower.end(), type_lower.begin(), ::tolower);
    
    if (type_lower.find("int") != std::string::npos) {
        return storage::DataType::INT64;
    } else if (type_lower.find("varchar") != std::string::npos) {
        return storage::DataType::VARCHAR;
    } else if (type_lower.find("char") != std::string::npos) {
        return storage::DataType::VARCHAR;
    } else if (type_lower.find("float") != std::string::npos) {
        return storage::DataType::FLOAT;
    } else if (type_lower.find("double") != std::string::npos) {
        return storage::DataType::DOUBLE;
    } else if (type_lower.find("bool") != std::string::npos) {
        return storage::DataType::BOOL;
    } else if (type_lower.find("date") != std::string::npos) {
        return storage::DataType::DATE;
    } else if (type_lower.find("time") != std::string::npos) {
        return storage::DataType::TIME;
    } else if (type_lower.find("datetime") != std::string::npos) {
        return storage::DataType::DATETIME;
    } else {
        return storage::DataType::VARCHAR;  // Default to VARCHAR
    }
}

uint32_t AlterTableExecutor::parseTypeLength(const std::string& type_str) {
    size_t paren_pos = type_str.find('(');
    if (paren_pos != std::string::npos) {
        size_t close_pos = type_str.find(')', paren_pos);
        if (close_pos != std::string::npos) {
            std::string len_str = type_str.substr(paren_pos + 1, close_pos - paren_pos - 1);
            return static_cast<uint32_t>(std::stoul(len_str));
        }
    }
    return 0;
}

}
}
