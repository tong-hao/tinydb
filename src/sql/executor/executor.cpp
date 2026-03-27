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
    if (auto select_stmt = dynamic_cast<const sql::SelectStmt*>(stmt)) {
        SelectExecutor e(storage_engine_);
        return e.execute(select_stmt);
    } else if (auto insert_stmt = dynamic_cast<const sql::InsertStmt*>(stmt)) {
        InsertExecutor e(storage_engine_);
        return e.execute(insert_stmt);
    }  else if (auto update_stmt = dynamic_cast<const sql::UpdateStmt*>(stmt)) {
        UpdateExecutor e(storage_engine_);
        return e.execute(update_stmt);
    } else if (auto delete_stmt = dynamic_cast<const sql::DeleteStmt*>(stmt)) {
        DeleteExecutor e(storage_engine_, current_txn_);
        return e.execute(delete_stmt);
    } else if (auto create_stmt = dynamic_cast<const sql::CreateTableStmt*>(stmt)) {
        CreateTableExecutor e(storage_engine_);
        return e.execute(create_stmt);
    } else if (auto drop_stmt = dynamic_cast<const sql::DropTableStmt*>(stmt)) {
        DropTableExecutor e(storage_engine_);
        return e.execute(drop_stmt);
    } else if (auto alter_stmt = dynamic_cast<const sql::AlterTableStmt*>(stmt)) {
        AlterTableExecutor e(storage_engine_);
        return e.execute(alter_stmt);
    } else if (auto begin_stmt = dynamic_cast<const sql::BeginStmt*>(stmt)) {
        BeginExecutor e(storage_engine_, &current_txn_);
        return e.execute(begin_stmt);
    } else if (auto commit_stmt = dynamic_cast<const sql::CommitStmt*>(stmt)) {
        CommitExecutor e(storage_engine_, &current_txn_);
        return e.execute(commit_stmt);
    } else if (auto rollback_stmt = dynamic_cast<const sql::RollbackStmt*>(stmt)) {
        RollbackExecutor e(storage_engine_, &current_txn_);
        return e.execute(rollback_stmt);
    } else if (auto create_index_stmt = dynamic_cast<const sql::CreateIndexStmt*>(stmt)) {
        CreateIndexExecutor e(storage_engine_);
        return e.execute(create_index_stmt);
    } else if (auto drop_index_stmt = dynamic_cast<const sql::DropIndexStmt*>(stmt)) {
        DropIndexExecutor e(storage_engine_);
        return e.execute(drop_index_stmt);
    } else if (auto analyze_stmt = dynamic_cast<const sql::AnalyzeStmt*>(stmt)) {
        AnalyzeExecutor e(storage_engine_);
        return e.execute(analyze_stmt);
    } else if (auto explain_stmt = dynamic_cast<const sql::ExplainStmt*>(stmt)) {
        ExplainExecutor e(storage_engine_);
        return e.execute(explain_stmt);
    } else if (auto create_view_stmt = dynamic_cast<const sql::CreateViewStmt*>(stmt)) {
        CreateViewExecutor e(storage_engine_, &views_);
        return e.execute(create_view_stmt);
    } else if (auto drop_view_stmt = dynamic_cast<const sql::DropViewStmt*>(stmt)) {
        DropViewExecutor e(storage_engine_, &views_);
        return e.execute(drop_view_stmt);
    }

    return ExecutionResult::error("Unsupported statement type");
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
