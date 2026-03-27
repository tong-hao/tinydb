#include "view_executor.h"
#include "common/logger.h"

namespace tinydb {
namespace engine {

// CreateViewExecutor
CreateViewExecutor::CreateViewExecutor(storage::StorageEngine* storage_engine, std::unordered_map<std::string, ViewMeta>* views)
    : storage_engine_(storage_engine), views_(views) {}

CreateViewExecutor::~CreateViewExecutor() {}

ExecutionResult CreateViewExecutor::execute(const sql::CreateViewStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid CREATE VIEW statement");
    }

    std::string view_name = stmt->viewName();

    // Check if view or table already exists
    if (views_->count(view_name) > 0 || storage_engine_->tableExists(view_name)) {
        return ExecutionResult::error("View or table already exists: " + view_name);
    }

    // Get the underlying SELECT statement
    const sql::SelectStmt* select_stmt = stmt->selectStmt();
    if (!select_stmt) {
        return ExecutionResult::error("VIEW must have a SELECT statement");
    }

    // Validate the base table exists
    std::string base_table = select_stmt->fromTable();
    if (!base_table.empty() && !storage_engine_->tableExists(base_table)) {
        return ExecutionResult::error("Base table does not exist: " + base_table);
    }

    // Store view metadata
    ViewMeta meta;
    meta.view_name = view_name;
    meta.sql_definition = select_stmt->toString();

    (*views_)[view_name] = std::move(meta);

    LOG_INFO("Created view: " << view_name);
    return ExecutionResult::ok("CREATE VIEW " + view_name);
}

// DropViewExecutor
DropViewExecutor::DropViewExecutor(storage::StorageEngine* storage_engine, std::unordered_map<std::string, ViewMeta>* views)
    : storage_engine_(storage_engine), views_(views) {}

DropViewExecutor::~DropViewExecutor() {}

ExecutionResult DropViewExecutor::execute(const sql::DropViewStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid DROP VIEW statement");
    }

    std::string view_name = stmt->viewName();

    // Check if view exists
    auto it = views_->find(view_name);
    if (it == views_->end()) {
        if (stmt->ifExists()) {
            return ExecutionResult::ok("View does not exist: " + view_name);
        }
        return ExecutionResult::error("View does not exist: " + view_name);
    }

    // Drop the view
    views_->erase(it);
    LOG_INFO("Dropped view: " << view_name);

    return ExecutionResult::ok("DROP VIEW " + view_name);
}

} // namespace engine
} // namespace tinydb