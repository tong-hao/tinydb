#pragma once

#include "sql/parser/ast.h"
#include "common/error.h"
#include "storage/storage_engine.h"
#include "sql/optimizer/optimizer.h"
#include "execution_result.h"
#include <string>
#include <memory>
#include <unordered_map>

namespace tinydb {
namespace engine {

// View metadata structure
struct ViewMeta {
    std::string view_name;
    std::string sql_definition;
};

// SQL 执行器
class Executor {
public:
    Executor();
    ~Executor();

    // 初始化执行器（传入存储引擎）
    void initialize(storage::StorageEngine* storage_engine);

    // 执行 SQL 语句
    ExecutionResult execute(const sql::SQLParseTree& ast);
    ExecutionResult execute(const std::string& sql);

private:
    storage::StorageEngine* storage_engine_ = nullptr;
    storage::Transaction* current_txn_ = nullptr;
    std::unique_ptr<QueryOptimizer> optimizer_;  // Phase 4

    // View storage (view name -> view metadata)
    std::unordered_map<std::string, ViewMeta> views_;
};

} // namespace engine
} // namespace tinydb
