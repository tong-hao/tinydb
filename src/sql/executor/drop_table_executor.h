#pragma once

#include "execution_result.h"
#include "storage/storage_engine.h"

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
