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
    ExecutionResult execute(const sql::AST& ast);
    ExecutionResult execute(const std::string& sql);

private:
    storage::StorageEngine* storage_engine_ = nullptr;
    storage::Transaction* current_txn_ = nullptr;
    std::unique_ptr<QueryOptimizer> optimizer_;  // Phase 4

    // View storage (view name -> view metadata)
    std::unordered_map<std::string, ViewMeta> views_;

    ExecutionResult executeSelect(const sql::SelectStmt* stmt);
    ExecutionResult executeInsert(const sql::InsertStmt* stmt);
    ExecutionResult executeCreateTable(const sql::CreateTableStmt* stmt);
    ExecutionResult executeDropTable(const sql::DropTableStmt* stmt);
    ExecutionResult executeUpdate(const sql::UpdateStmt* stmt);
    ExecutionResult executeDelete(const sql::DeleteStmt* stmt);
    ExecutionResult executeAlterTable(const sql::AlterTableStmt* stmt);
    ExecutionResult executeBegin(const sql::BeginStmt* stmt);
    ExecutionResult executeCommit(const sql::CommitStmt* stmt);
    ExecutionResult executeRollback(const sql::RollbackStmt* stmt);

    // Phase 4: 新增执行方法
    ExecutionResult executeCreateIndex(const sql::CreateIndexStmt* stmt);
    ExecutionResult executeDropIndex(const sql::DropIndexStmt* stmt);
    ExecutionResult executeAnalyze(const sql::AnalyzeStmt* stmt);
    ExecutionResult executeExplain(const sql::ExplainStmt* stmt);

    // View operations
    ExecutionResult executeCreateView(const sql::CreateViewStmt* stmt);
    ExecutionResult executeDropView(const sql::DropViewStmt* stmt);

    // Phase 4: 多表 JOIN 执行
    ExecutionResult executeJoin(const sql::SelectStmt* stmt);

    // LEFT JOIN 辅助函数
    void processJoinClause(const sql::SelectStmt* stmt,
                          const storage::Tuple& current_tuple,
                          storage::Schema* current_schema,
                          const std::vector<std::unique_ptr<sql::JoinTable>>& join_tables,
                          size_t join_index,
                          std::vector<storage::Tuple> accumulated_tuples,
                          std::vector<storage::Schema*>& schemas,
                          std::vector<std::vector<storage::Tuple>>& results,
                          int& row_count);
    ExecutionResult executeSimpleJoin(const sql::SelectStmt* stmt,
                                      const std::vector<std::string>& tables,
                                      std::string& result);

    // 类型转换辅助函数
    storage::DataType parseDataType(const std::string& type_str);
    uint32_t parseTypeLength(const std::string& type_str);

    // WHERE 条件评估
    bool evaluateWhereCondition(const storage::Tuple& tuple, const sql::Expression* condition, const storage::Schema* schema);

    // Phase 4: 多表 WHERE 条件评估
    bool evaluateJoinWhereCondition(const std::vector<storage::Tuple>& tuples,
                                    const std::vector<storage::Schema*>& schemas,
                                    const sql::Expression* condition);

    // 格式化元组为字符串，支持选择特定列
    std::string formatTuple(const storage::Tuple& tuple, const std::vector<std::unique_ptr<sql::Expression>>& select_list, const storage::Schema* schema);

    // Phase 4: 多表格式化
    std::string formatJoinTuple(const std::vector<storage::Tuple>& tuples,
                                const std::vector<storage::Schema*>& schemas,
                                const std::vector<std::unique_ptr<sql::Expression>>& select_list);

    // 表达式求值
    storage::Field evaluateExpression(const sql::Expression* expr, const storage::Tuple& tuple, const storage::Schema* schema);

    // Phase 4: 多表表达式求值
    storage::Field evaluateJoinExpression(const sql::Expression* expr,
                                          const std::vector<storage::Tuple>& tuples,
                                          const std::vector<storage::Schema*>& schemas);

    // 获取表锁
    bool acquireTableLock(const std::string& table_name, storage::LockType lock_type);

    // 释放表锁
    bool releaseTableLock(const std::string& table_name);

    // LIKE 模式匹配辅助函数
    bool matchLikePattern(const std::string& value, const std::string& pattern);
};

} // namespace engine
} // namespace tinydb
