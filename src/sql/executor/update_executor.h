#pragma once

#include "execution_result.h"
#include "sql/optimizer/optimizer.h"
#include <memory>
#include "storage/storage_engine.h"

namespace tinydb {
namespace engine {

class UpdateExecutor {
public:
    UpdateExecutor(storage::StorageEngine* storage_engine):storage_engine_(storage_engine) {}

    virtual ~UpdateExecutor() {};

    ExecutionResult execute(const sql::SelectStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
};


}
}
