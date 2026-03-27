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

    // Utility functions for executors
    static bool evaluateWhereCondition(const storage::Tuple& tuple, 
                                        const sql::Expression* condition, 
                                        const storage::Schema* schema);
    
    static bool acquireTableLock(storage::StorageEngine* storage_engine,
                                  storage::Transaction* txn,
                                  const std::string& table_name, 
                                  storage::LockType lock_type);
    
    static bool releaseTableLock(storage::StorageEngine* storage_engine,
                                  storage::Transaction* txn,
                                  const std::string& table_name);
    
    static std::string formatTuple(const storage::Tuple& tuple, 
                                   const std::vector<std::unique_ptr<sql::Expression>>& select_list, 
                                   const storage::Schema* schema);
    
    static storage::DataType parseDataType(const std::string& type_str);
    static uint32_t parseTypeLength(const std::string& type_str);
    
    static storage::Field evaluateExpression(const sql::Expression* expr, 
                                              const storage::Tuple& tuple, 
                                              const storage::Schema* schema);
    
    static bool matchLikePattern(const std::string& value, const std::string& pattern);
    
    static bool evaluateJoinWhereCondition(const std::vector<storage::Tuple>& tuples,
                                            const std::vector<storage::Schema*>& schemas,
                                            const sql::Expression* condition);
    
    static storage::Field evaluateJoinExpression(const sql::Expression* expr,
                                                  const std::vector<storage::Tuple>& tuples,
                                                  const std::vector<storage::Schema*>& schemas);
    
    static std::string formatJoinTuple(const std::vector<storage::Tuple>& tuples,
                                        const std::vector<storage::Schema*>& schemas,
                                        const std::vector<std::unique_ptr<sql::Expression>>& select_list);

private:
    storage::StorageEngine* storage_engine_ = nullptr;
    storage::Transaction* current_txn_ = nullptr;
    std::unique_ptr<QueryOptimizer> optimizer_;  // Phase 4

    // View storage (view name -> view metadata)
    std::unordered_map<std::string, ViewMeta> views_;
};

} // namespace engine
} // namespace tinydb
