#pragma once

#include "execution_result.h"
#include "storage/storage_engine.h"
#include "sql/stmt/create_table_stmt.h"
#include <functional>

namespace tinydb {
namespace engine {

class CreateTableExecutor {
public:
    CreateTableExecutor(storage::StorageEngine* storage_engine);
    virtual ~CreateTableExecutor();

    ExecutionResult execute(const sql::CreateTableStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    storage::DataType parseDataType(const std::string& type_str);
    uint32_t parseTypeLength(const std::string& type_str);
};

}
}
