#pragma once

#include "sql/ast.h"
#include "common/error.h"
#include "storage/storage_engine.h"
#include "optimizer.h"
#include <string>
#include <memory>

namespace tinydb {
namespace engine {

// 执行结果
class ExecutionResult {
public:
    ExecutionResult() : success_(true) {}
    explicit ExecutionResult(std::string message) : success_(true), message_(std::move(message)) {}

    static ExecutionResult ok(const std::string& msg = "OK") {
        return ExecutionResult(msg);
    }

    static ExecutionResult error(const std::string& msg) {
        ExecutionResult result;
        result.success_ = false;
        result.message_ = msg;
        return result;
    }

    bool success() const { return success_; }
    const std::string& message() const { return message_; }

private:
    bool success_;
    std::string message_;
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

    // Phase 4: 多表JOIN执行
    ExecutionResult executeJoin(const sql::SelectStmt* stmt);

    // 类型转换辅助函数
    storage::DataType parseDataType(const std::string& type_str);
    uint32_t parseTypeLength(const std::string& type_str);

    // WHERE 条件评估
    bool evaluateWhereCondition(const storage::Tuple& tuple, const sql::Expression* condition, const storage::Schema* schema);

    // Phase 4: 多表WHERE条件评估
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
};

} // namespace engine
} // namespace tinydb
