#pragma once

#include "sql/ast.h"
#include "common/error.h"
#include <string>

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

    // 执行 SQL 语句（阶段一仅返回预定义响应）
    ExecutionResult execute(const sql::AST& ast);
    ExecutionResult execute(const std::string& sql);

private:
    ExecutionResult executeSelect(const sql::SelectStmt* stmt);
    ExecutionResult executeInsert(const sql::InsertStmt* stmt);
    ExecutionResult executeCreateTable(const sql::CreateTableStmt* stmt);
    ExecutionResult executeDropTable(const sql::DropTableStmt* stmt);
};

} // namespace engine
} // namespace tinydb
