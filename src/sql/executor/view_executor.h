#pragma once

#include "execution_result.h"
#include "storage/storage_engine.h"

namespace tinydb {
namespace engine {

class CreateViewExecutor {
public:
    CreateViewExecutor(storage::StorageEngine* storage_engine, std::unordered_map<std::string, ViewMeta>* views);
    virtual ~CreateViewExecutor();

    ExecutionResult execute(const sql::CreateViewStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    std::unordered_map<std::string, ViewMeta>* views_;
};

class DropViewExecutor {
public:
    DropViewExecutor(storage::StorageEngine* storage_engine, std::unordered_map<std::string, ViewMeta>* views);
    virtual ~DropViewExecutor();

    ExecutionResult execute(const sql::DropViewStmt* stmt);

private:
    storage::StorageEngine* storage_engine_;
    std::unordered_map<std::string, ViewMeta>* views_;
};

}
}
