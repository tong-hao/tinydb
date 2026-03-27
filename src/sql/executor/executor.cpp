#include "executor.h"
#include "common/logger.h"
#include "sql/stmt/system_view_manager.h"
#include <sstream>
#include <algorithm>
#include <cctype>
#include "sql/parser/parser.h"

#include "insert_executor.h"
#include "delete_executor.h"
#include "update_executor.h"
#include "select_executor.h"
#include "create_table_executor.h"
#include "drop_table_executor.h"
#include "alter_table_executor.h"
#include "transaction_executor.h"
#include "index_executor.h"
#include "view_executor.h"

namespace tinydb {
namespace engine {

Executor::Executor() {
    LOG_INFO("Executor initialized");
}

Executor::~Executor() {
}

void Executor::initialize(storage::StorageEngine* storage_engine) {
    storage_engine_ = storage_engine;
    LOG_INFO("Executor bound to storage engine");
}

ExecutionResult Executor::execute(const sql::SQLParseTree& ast) {
    if (!ast.statement()) {
        return ExecutionResult::error("Empty statement");
    }

    const sql::Statement* stmt = ast.statement();

    // 根据语句类型分发执行
    switch (stmt->type()) {
        case sql::StatementType::SELECT: {
            const auto* select_stmt = static_cast<const sql::SelectStmt*>(stmt);
            SelectExecutor e(storage_engine_);
            return e.execute(select_stmt);
        }
        case sql::StatementType::INSERT: {
            const auto* insert_stmt = static_cast<const sql::InsertStmt*>(stmt);
            InsertExecutor e(storage_engine_);
            return e.execute(insert_stmt);
        }
        case sql::StatementType::UPDATE: {
            const auto* update_stmt = static_cast<const sql::UpdateStmt*>(stmt);
            UpdateExecutor e(storage_engine_);
            return e.execute(update_stmt);
        }
        case sql::StatementType::DELETE: {
            const auto* delete_stmt = static_cast<const sql::DeleteStmt*>(stmt);
            DeleteExecutor e(storage_engine_, current_txn_);
            return e.execute(delete_stmt);
        }
        case sql::StatementType::CREATE_TABLE: {
            const auto* create_stmt = static_cast<const sql::CreateTableStmt*>(stmt);
            CreateTableExecutor e(storage_engine_);
            return e.execute(create_stmt);
        }
        case sql::StatementType::DROP_TABLE: {
            const auto* drop_stmt = static_cast<const sql::DropTableStmt*>(stmt);
            DropTableExecutor e(storage_engine_);
            return e.execute(drop_stmt);
        }
        case sql::StatementType::ALTER_TABLE: {
            const auto* alter_stmt = static_cast<const sql::AlterTableStmt*>(stmt);
            AlterTableExecutor e(storage_engine_);
            return e.execute(alter_stmt);
        }
        case sql::StatementType::BEGIN: {
            const auto* begin_stmt = static_cast<const sql::BeginStmt*>(stmt);
            BeginExecutor e(storage_engine_, &current_txn_);
            return e.execute(begin_stmt);
        }
        case sql::StatementType::COMMIT: {
            const auto* commit_stmt = static_cast<const sql::CommitStmt*>(stmt);
            CommitExecutor e(storage_engine_, &current_txn_);
            return e.execute(commit_stmt);
        }
        case sql::StatementType::ROLLBACK: {
            const auto* rollback_stmt = static_cast<const sql::RollbackStmt*>(stmt);
            RollbackExecutor e(storage_engine_, &current_txn_);
            return e.execute(rollback_stmt);
        }
        case sql::StatementType::CREATE_INDEX: {
            const auto* create_index_stmt = static_cast<const sql::CreateIndexStmt*>(stmt);
            CreateIndexExecutor e(storage_engine_);
            return e.execute(create_index_stmt);
        }
        case sql::StatementType::DROP_INDEX: {
            const auto* drop_index_stmt = static_cast<const sql::DropIndexStmt*>(stmt);
            DropIndexExecutor e(storage_engine_);
            return e.execute(drop_index_stmt);
        }
        case sql::StatementType::ANALYZE: {
            const auto* analyze_stmt = static_cast<const sql::AnalyzeStmt*>(stmt);
            AnalyzeExecutor e(storage_engine_);
            return e.execute(analyze_stmt);
        }
        case sql::StatementType::EXPLAIN: {
            const auto* explain_stmt = static_cast<const sql::ExplainStmt*>(stmt);
            ExplainExecutor e(storage_engine_);
            return e.execute(explain_stmt);
        }
        case sql::StatementType::CREATE_VIEW: {
            const auto* create_view_stmt = static_cast<const sql::CreateViewStmt*>(stmt);
            CreateViewExecutor e(storage_engine_, &views_);
            return e.execute(create_view_stmt);
        }
        case sql::StatementType::DROP_VIEW: {
            const auto* drop_view_stmt = static_cast<const sql::DropViewStmt*>(stmt);
            DropViewExecutor e(storage_engine_, &views_);
            return e.execute(drop_view_stmt);
        }
        default:
            return ExecutionResult::error("Unsupported statement type");
    }
}

ExecutionResult Executor::execute(const std::string &sql)
{
    LOG_INFO("Executing SQL: " << sql);

    // 使用 Parser 直接解析 SQL (Bison/Flex 集成)
    ExecutionResult result;
    try
    {
        sql::Parser parser(sql);
        auto ast = parser.parse();

        if (ast && ast->statement())
        {
            // 通过 SQLParseTree 执行
            LOG_INFO("SQL parsed successfully, executing via SQLParseTree");
            result = execute(*ast);
        }
        else
        {
            // 解析失败，直接报错
            LOG_ERROR("SQL parse failed: " + parser.error());
            result = ExecutionResult::error("SQL parse error: " + parser.error());
        }
    }
    catch (const std::exception &e)
    {
        LOG_ERROR("SQL execution error: " << e.what());
        result = ExecutionResult::error("SQL execution error: " + std::string(e.what()));
    }

    return result;
}


} // namespace engine
} // namespace tinydb
