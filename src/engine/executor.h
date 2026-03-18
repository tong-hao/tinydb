#pragma once

#include "sql/ast.h"
#include "common/error.h"
#include "storage/storage_engine.h"
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

    ExecutionResult executeSelect(const sql::SelectStmt* stmt);
    ExecutionResult executeInsert(const sql::InsertStmt* stmt);
    ExecutionResult executeCreateTable(const sql::CreateTableStmt* stmt);
    ExecutionResult executeDropTable(const sql::DropTableStmt* stmt);
    ExecutionResult executeUpdate(const sql::UpdateStmt* stmt);
    ExecutionResult executeDelete(const sql::DeleteStmt* stmt);
    ExecutionResult executeAlterTable(const sql::AlterTableStmt* stmt);

    // 类型转换辅助函数
    storage::DataType parseDataType(const std::string& type_str);

    // WHERE 条件评估
    bool evaluateWhereCondition(const storage::Tuple& tuple, const sql::Expression* condition, const storage::Schema* schema);

    // 格式化元组为字符串，支持选择特定列
    std::string formatTuple(const storage::Tuple& tuple, const std::vector<std::unique_ptr<sql::Expression>>& select_list, const storage::Schema* schema);
};

} // namespace engine
} // namespace tinydb
