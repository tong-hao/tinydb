#pragma once

#include "execution_result.h"
#include "sql/optimizer/optimizer.h"
#include <memory>
#include "storage/storage_engine.h"
#include "sql/stmt/update_stmt.h"

namespace tinydb {
namespace engine {

class UpdateExecutor {
public:
    UpdateExecutor(storage::StorageEngine* storage_engine):storage_engine_(storage_engine) {}

    virtual ~UpdateExecutor() {};

    ExecutionResult execute(const sql::UpdateStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
};


}
}
