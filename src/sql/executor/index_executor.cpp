#include "index_executor.h"
#include "common/logger.h"

namespace tinydb {
namespace engine {

// CreateIndexExecutor
CreateIndexExecutor::CreateIndexExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

CreateIndexExecutor::~CreateIndexExecutor() {}

ExecutionResult CreateIndexExecutor::execute(const sql::CreateIndexStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid CREATE INDEX statement");
    }

    std::string index_name = stmt->indexName();
    std::string table_name = stmt->tableName();
    std::string column_name = stmt->columnName();
    bool is_unique = stmt->isUnique();

    // Check if table exists
    if (!storage_engine_->tableExists(table_name)) {
        return ExecutionResult::error("Table does not exist: " + table_name);
    }

    // Get table metadata, check if column exists
    auto table_meta = storage_engine_->getTable(table_name);
    if (!table_meta) {
        return ExecutionResult::error("Failed to get table metadata: " + table_name);
    }

    int col_idx = table_meta->schema.findColumnIndex(column_name);
    if (col_idx < 0) {
        return ExecutionResult::error("Column does not exist: " + column_name);
    }

    // Create index
    if (storage_engine_->createIndex(index_name, table_name, column_name, is_unique)) {
        return ExecutionResult::ok("CREATE INDEX " + index_name + " ON " + table_name +
                                   "(" + column_name + ")");
    }

    return ExecutionResult::error("Failed to create index: " + index_name);
}

// DropIndexExecutor
DropIndexExecutor::DropIndexExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

DropIndexExecutor::~DropIndexExecutor() {}

ExecutionResult DropIndexExecutor::execute(const sql::DropIndexStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid DROP INDEX statement");
    }

    std::string index_name = stmt->indexName();

    if (storage_engine_->getIndex(index_name)) {
        if (storage_engine_->dropIndex(index_name)) {
            return ExecutionResult::ok("DROP INDEX " + index_name);
        }
        return ExecutionResult::error("Failed to drop index: " + index_name);
    }

    return ExecutionResult::error("Index does not exist: " + index_name);
}

// AnalyzeExecutor
AnalyzeExecutor::AnalyzeExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

AnalyzeExecutor::~AnalyzeExecutor() {}

ExecutionResult AnalyzeExecutor::execute(const sql::AnalyzeStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt) {
        return ExecutionResult::error("Invalid ANALYZE statement");
    }

    std::string table_name = stmt->tableName();

    if (!storage_engine_->tableExists(table_name)) {
        return ExecutionResult::error("Table does not exist: " + table_name);
    }

    if (storage_engine_->analyzeTable(table_name)) {
        return ExecutionResult::ok("ANALYZE " + table_name);
    }

    return ExecutionResult::error("Failed to analyze table: " + table_name);
}

// ExplainExecutor
ExplainExecutor::ExplainExecutor(storage::StorageEngine* storage_engine)
    : storage_engine_(storage_engine) {}

ExplainExecutor::~ExplainExecutor() {}

ExecutionResult ExplainExecutor::execute(const sql::ExplainStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::error("No storage engine");
    }

    if (!stmt || !stmt->innerStatement()) {
        return ExecutionResult::error("Invalid EXPLAIN statement");
    }

    // Check if this is a SELECT statement
    if (auto select_stmt = dynamic_cast<const sql::SelectStmt*>(stmt->innerStatement())) {
        if (!optimizer_) {
            optimizer_ = std::make_unique<QueryOptimizer>(
                storage_engine_->getStatisticsManager(),
                storage_engine_->getIndexManager());
        }

        ExecutionPlan plan = optimizer_->optimize(select_stmt);
        return ExecutionResult::ok("\n" + plan.toString());
    }

    return ExecutionResult::ok("EXPLAIN: Query plan not available for this statement type");
}

} // namespace engine
} // namespace tinydb