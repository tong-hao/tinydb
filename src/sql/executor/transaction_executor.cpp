#include "transaction_executor.h"
#include "common/logger.h"

namespace tinydb {
namespace engine {

// BeginExecutor
BeginExecutor::BeginExecutor(storage::StorageEngine* storage_engine, storage::Transaction** current_txn)
    : storage_engine_(storage_engine), current_txn_(current_txn) {}

BeginExecutor::~BeginExecutor() {}

ExecutionResult BeginExecutor::execute(const sql::BeginStmt* stmt) {
    (void)stmt;  // Avoid unused parameter warning

    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (*current_txn_) {
        return ExecutionResult::error("Transaction already in progress");
    }

    *current_txn_ = storage_engine_->beginTransaction();
    if (!(*current_txn_)) {
        return ExecutionResult::error("Failed to begin transaction");
    }

    LOG_INFO("Transaction started: " << (*current_txn_)->getId());
    return ExecutionResult::ok("BEGIN");
}

// CommitExecutor
CommitExecutor::CommitExecutor(storage::StorageEngine* storage_engine, storage::Transaction** current_txn)
    : storage_engine_(storage_engine), current_txn_(current_txn) {}

CommitExecutor::~CommitExecutor() {}

ExecutionResult CommitExecutor::execute(const sql::CommitStmt* stmt) {
    (void)stmt;  // Avoid unused parameter warning

    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!(*current_txn_)) {
        return ExecutionResult::error("No active transaction");
    }

    if (storage_engine_->commitTransaction(*current_txn_)) {
        LOG_INFO("Transaction committed: " << (*current_txn_)->getId());
        *current_txn_ = nullptr;
        return ExecutionResult::ok("COMMIT");
    }

    return ExecutionResult::error("Failed to commit transaction");
}

// RollbackExecutor
RollbackExecutor::RollbackExecutor(storage::StorageEngine* storage_engine, storage::Transaction** current_txn)
    : storage_engine_(storage_engine), current_txn_(current_txn) {}

RollbackExecutor::~RollbackExecutor() {}

ExecutionResult RollbackExecutor::execute(const sql::RollbackStmt* stmt) {
    (void)stmt;  // Avoid unused parameter warning

    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!(*current_txn_)) {
        return ExecutionResult::error("No active transaction");
    }

    if (storage_engine_->abortTransaction(*current_txn_)) {
        LOG_INFO("Transaction rolled back: " << (*current_txn_)->getId());
        *current_txn_ = nullptr;
        return ExecutionResult::ok("ROLLBACK");
    }

    return ExecutionResult::error("Failed to rollback transaction");
}

} // namespace engine
} // namespace tinydb