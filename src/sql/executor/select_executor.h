#pragma once

#include "execution_result.h"
#include <memory>
#include "storage/storage_engine.h"
#include "sql/stmt/select_stmt.h"
#include "executor.h"

namespace tinydb {
namespace engine {

class SelectExecutor {
public:
    SelectExecutor(storage::StorageEngine* storage_engine);
    virtual ~SelectExecutor();

    ExecutionResult execute(const sql::SelectStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    std::unordered_map<std::string, ViewMeta> views_;

    ExecutionResult executeJoin(const sql::SelectStmt* stmt);
    ExecutionResult executeSimpleJoin(const sql::SelectStmt* stmt, const std::vector<std::string>& tables, std::string& result);
    void processJoinClause(const sql::SelectStmt* stmt, const storage::Tuple& current_tuple,
                           storage::Schema* current_schema, const std::vector<std::unique_ptr<sql::JoinTable>>& join_tables,
                           size_t join_index, std::vector<storage::Tuple> accumulated_tuples,
                           std::vector<storage::Schema*>& schemas,
                           std::vector<std::vector<storage::Tuple>>& results, int& row_count);
};


}
}
