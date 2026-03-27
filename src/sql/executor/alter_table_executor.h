#pragma once

#include "execution_result.h"
#include "sql/stmt/alter_table_stmt.h"
#include "storage/storage_engine.h"

namespace tinydb {
namespace engine {

class AlterTableExecutor {
public:
    AlterTableExecutor(storage::StorageEngine* storage_engine);
    virtual ~AlterTableExecutor();

    ExecutionResult execute(const sql::AlterTableStmt* stmt);

private:
    storage::DataType parseDataType(const std::string& type_str);
    uint32_t parseTypeLength(const std::string& type_str);

private:
    storage::StorageEngine* storage_engine_;

};

}
}
