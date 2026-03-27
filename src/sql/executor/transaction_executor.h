#pragma once

#include "execution_result.h"
#include "storage/storage_engine.h"
#include "sql/stmt/begin_stmt.h"
#include "sql/stmt/commit_stmt.h"
#include "sql/stmt/rollback_stmt.h"

namespace tinydb {
namespace engine {

class BeginExecutor {
public:
    BeginExecutor(storage::StorageEngine* storage_engine, storage::Transaction** current_txn);
    virtual ~BeginExecutor();

    ExecutionResult execute(const sql::BeginStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    storage::Transaction** current_txn_;
};

class CommitExecutor {
public:
    CommitExecutor(storage::StorageEngine* storage_engine, storage::Transaction** current_txn);
    virtual ~CommitExecutor();

    ExecutionResult execute(const sql::CommitStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    storage::Transaction** current_txn_;
};

class RollbackExecutor {
public:
    RollbackExecutor(storage::StorageEngine* storage_engine, storage::Transaction** current_txn);
    virtual ~RollbackExecutor();

    ExecutionResult execute(const sql::RollbackStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    storage::Transaction** current_txn_;
};

}
}
