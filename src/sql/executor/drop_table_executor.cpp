#include "drop_table_executor.h"

namespace tinydb {
namespace engine {

DropTableExecutor::DropTableExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

DropTableExecutor::~DropTableExecutor() {}

ExecutionResult DropTableExecutor::execute(const sql::DropTableStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("DROP TABLE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // Check if table exists
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        // Drop table
        if (storage_engine_->dropTable(table_name)) {
            return ExecutionResult::ok("DROP TABLE " + table_name);
        } else {
            return ExecutionResult::error("Failed to drop table: " + table_name);
        }
    }

    return ExecutionResult::ok("DROP TABLE executed");
}

}
}
