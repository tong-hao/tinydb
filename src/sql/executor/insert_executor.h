#pragma once

#include "execution_result.h"
#include "sql/optimizer/optimizer.h"
#include <memory>
#include "storage/storage_engine.h"
#include "sql/stmt/insert_stmt.h"

namespace tinydb {
namespace engine {

class InsertExecutor {
public:
    InsertExecutor(storage::StorageEngine* storage_engine):storage_engine_(storage_engine) {}

    virtual ~InsertExecutor() {};

    ExecutionResult execute(const sql::InsertStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
};


}
}
