#include "create_table_executor.h"
#include "common/logger.h"

namespace tinydb {
namespace engine {

CreateTableExecutor::CreateTableExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

CreateTableExecutor::~CreateTableExecutor() {}

ExecutionResult CreateTableExecutor::execute(const sql::CreateTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("CREATE TABLE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // Check if table already exists
        if (storage_engine_->tableExists(table_name)) {
            if (stmt->ifNotExists()) {
                return ExecutionResult::ok("Table already exists, skipped");
            }
            return ExecutionResult::error("Table already exists: " + table_name);
        }

        // Build Schema
        storage::Schema schema;
        for (const auto& col : stmt->columns()) {
            storage::DataType type = parseDataType(col.type);
            uint32_t length = 0;

            // Parse length (e.g., VARCHAR(32))
            size_t paren_pos = col.type.find('(');
            if (paren_pos != std::string::npos) {
                size_t close_pos = col.type.find(')', paren_pos);
                if (close_pos != std::string::npos) {
                    std::string len_str = col.type.substr(paren_pos + 1, close_pos - paren_pos - 1);
                    length = static_cast<uint32_t>(std::stoul(len_str));
                }
            }

            // Build complete ColumnDef with constraints
            storage::ColumnDef col_def;
            col_def.name = col.name;
            col_def.type = type;
            col_def.length = length;
            col_def.is_nullable = !col.is_not_null;  // NOT NULL constraint
            col_def.is_primary_key = col.is_primary_key;  // PRIMARY KEY constraint

            schema.addColumn(col_def);

            // Auto-create index if PRIMARY KEY constraint
            if (col.is_primary_key) {
                LOG_INFO("Column '" << col.name << "' is PRIMARY KEY");
                // Storage engine will auto-create index for primary key after createTable
            }
        }

        // Create table
        if (storage_engine_->createTable(table_name, schema)) {
            return ExecutionResult::ok("CREATE TABLE " + table_name);
        } else {
            return ExecutionResult::error("Failed to create table: " + table_name);
        }
    }

    return ExecutionResult::ok("CREATE TABLE executed");
}

storage::DataType CreateTableExecutor::parseDataType(const std::string& type_str) {
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

uint32_t CreateTableExecutor::parseTypeLength(const std::string& type_str) {
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
