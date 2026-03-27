#pragma once

#include "execution_result.h"
#include "storage/storage_engine.h"
#include "sql/stmt/delete_stmt.h"

namespace tinydb {
namespace engine {

class DeleteExecutor
{
public:
    DeleteExecutor(storage::StorageEngine* storage_engine, storage::Transaction* current_txn);
    virtual ~DeleteExecutor();

    ExecutionResult execute(const sql::DeleteStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    storage::Transaction* current_txn_;
};


}
}
