#pragma once

#include "execution_result.h"
#include "sql/optimizer/optimizer.h"
#include <memory>
#include "storage/storage_engine.h"

namespace tinydb {
namespace engine {

class CreateIndexExecutor {
public:
    CreateIndexExecutor(storage::StorageEngine* storage_engine);
    virtual ~CreateIndexExecutor();

    ExecutionResult execute(const sql::CreateIndexStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
};

class DropIndexExecutor {
public:
    DropIndexExecutor(storage::StorageEngine* storage_engine);
    virtual ~DropIndexExecutor();

    ExecutionResult execute(const sql::DropIndexStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
};

class AnalyzeExecutor {
public:
    AnalyzeExecutor(storage::StorageEngine* storage_engine);
    virtual ~AnalyzeExecutor();

    ExecutionResult execute(const sql::AnalyzeStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
};

class ExplainExecutor {
public:
    ExplainExecutor(storage::StorageEngine* storage_engine);
    virtual ~ExplainExecutor();

    ExecutionResult execute(const sql::ExplainStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    std::unique_ptr<QueryOptimizer> optimizer_;
};

}
}
