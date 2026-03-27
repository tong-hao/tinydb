#pragma once

#include "execution_result.h"
#include "storage/storage_engine.h"
#include "sql/stmt/drop_table_stmt.h"

namespace tinydb {
namespace engine {

class DropTableExecutor {
public:
    DropTableExecutor(storage::StorageEngine* storage_engine);
    virtual ~DropTableExecutor();

    ExecutionResult execute(const sql::DropTableStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
};

}
}
