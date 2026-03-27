#include "common/logger.h"
#include "delete_executor.h"
#include "executor.h"

namespace tinydb {
namespace engine {
DeleteExecutor::DeleteExecutor(storage::StorageEngine* storage_engine, storage::Transaction* current_txn)
    :storage_engine_(storage_engine),current_txn_(current_txn) {}

DeleteExecutor::~DeleteExecutor() {}

ExecutionResult DeleteExecutor::execute(const sql::DeleteStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("DELETE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // Check if table exists
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // Get or start transaction
        storage::Transaction* txn = current_txn_;
        bool auto_txn = false;
        if (!txn) {
            txn = storage_engine_->beginTransaction();
            current_txn_ = txn;
            auto_txn = true;
        }

        // Acquire exclusive lock (write lock)
        if (!Executor::acquireTableLock(storage_engine_, txn, table_name, storage::LockType::EXCLUSIVE)) {
            if (auto_txn) {
                storage_engine_->abortTransaction(txn);
                current_txn_ = nullptr;
            }
            return ExecutionResult::error("Failed to acquire lock on table: " + table_name);
        }

        // Get WHERE condition
        const sql::Expression* where_condition = stmt->whereCondition();

        // Iterate through all rows in table
        auto iter = storage_engine_->scan(table_name);
        int delete_count = 0;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            storage::TID tid = iter.getCurrentTID();

            // Apply WHERE condition filter
            if (where_condition && !Executor::evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // Record deletion (for rollback)
            if (txn) {
                txn->addDeleteRecord(table_name, tuple, tid);
            }

            // Delete tuple
            if (storage_engine_->remove(table_name, tid)) {
                delete_count++;
            }
        }

        // Release lock
        Executor::releaseTableLock(storage_engine_, txn, table_name);

        // Auto-commit if no explicit transaction
        if (auto_txn) {
            if (!storage_engine_->commitTransaction(txn)) {
                current_txn_ = nullptr;
                return ExecutionResult::error("Failed to commit delete");
            }
            current_txn_ = nullptr;
        }

        return ExecutionResult::ok("DELETE " + std::to_string(delete_count) + " row(s) from table " + table_name);
    }

    return ExecutionResult::ok("DELETE executed");
}

} // namespace engine
} // namespace tinydb
