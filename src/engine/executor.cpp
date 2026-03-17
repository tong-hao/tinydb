#include "executor.h"
#include "common/logger.h"
#include <sstream>

namespace tinydb {
namespace engine {

Executor::Executor() {
    LOG_INFO("Executor initialized");
}

Executor::~Executor() {
}

ExecutionResult Executor::execute(const sql::AST& ast) {
    if (!ast.statement()) {
        return ExecutionResult::error("Empty statement");
    }

    const sql::Statement* stmt = ast.statement();

    // 根据语句类型分发执行
    if (auto select_stmt = dynamic_cast<const sql::SelectStmt*>(stmt)) {
        return executeSelect(select_stmt);
    } else if (auto insert_stmt = dynamic_cast<const sql::InsertStmt*>(stmt)) {
        return executeInsert(insert_stmt);
    } else if (auto create_stmt = dynamic_cast<const sql::CreateTableStmt*>(stmt)) {
        return executeCreateTable(create_stmt);
    } else if (auto drop_stmt = dynamic_cast<const sql::DropTableStmt*>(stmt)) {
        return executeDropTable(drop_stmt);
    }

    return ExecutionResult::error("Unsupported statement type");
}

ExecutionResult Executor::execute(const std::string& sql) {
    // 阶段一：仅返回预定义响应，不实际执行
    LOG_INFO("Executing SQL: " << sql);

    // 简单的 SQL 类型检测，返回相应的模拟响应
    std::string upper_sql = sql;
    for (auto& c : upper_sql) {
        c = std::toupper(c);
    }

    std::istringstream iss(upper_sql);
    std::string first_word;
    iss >> first_word;

    if (first_word == "SELECT") {
        return ExecutionResult::ok("SELECT executed (simulated)");
    } else if (first_word == "INSERT") {
        return ExecutionResult::ok("INSERT executed (simulated)");
    } else if (first_word == "UPDATE") {
        return ExecutionResult::ok("UPDATE executed (simulated)");
    } else if (first_word == "DELETE") {
        return ExecutionResult::ok("DELETE executed (simulated)");
    } else if (first_word == "CREATE") {
        return ExecutionResult::ok("CREATE executed (simulated)");
    } else if (first_word == "DROP") {
        return ExecutionResult::ok("DROP executed (simulated)");
    } else {
        return ExecutionResult::ok("Command executed (simulated)");
    }
}

ExecutionResult Executor::executeSelect(const sql::SelectStmt* stmt) {
    LOG_INFO("Executing SELECT on table: " << stmt->fromTable());
    return ExecutionResult::ok("SELECT executed (simulated)");
}

ExecutionResult Executor::executeInsert(const sql::InsertStmt* stmt) {
    LOG_INFO("Executing INSERT into table: " << stmt->table());
    return ExecutionResult::ok("INSERT executed (simulated)");
}

ExecutionResult Executor::executeCreateTable(const sql::CreateTableStmt* stmt) {
    LOG_INFO("Executing CREATE TABLE: " << stmt->table());
    return ExecutionResult::ok("CREATE TABLE executed (simulated)");
}

ExecutionResult Executor::executeDropTable(const sql::DropTableStmt* stmt) {
    LOG_INFO("Executing DROP TABLE: " << stmt->table());
    return ExecutionResult::ok("DROP TABLE executed (simulated)");
}

} // namespace engine
} // namespace tinydb
