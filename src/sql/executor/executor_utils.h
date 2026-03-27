#pragma once

#include "storage/storage_engine.h"
#include "sql/parser/ast.h"
#include <string>

namespace tinydb {
namespace engine {

// Helper functions for executors

// Evaluate WHERE condition
bool evaluateWhereCondition(const storage::Tuple& tuple, 
                            const sql::Expression* condition, 
                            const storage::Schema* schema);

// Acquire table lock
bool acquireTableLock(storage::StorageEngine* storage_engine,
                      storage::Transaction* txn,
                      const std::string& table_name, 
                      storage::LockType lock_type);

// Release table lock
bool releaseTableLock(storage::StorageEngine* storage_engine,
                      storage::Transaction* txn,
                      const std::string& table_name);

} // namespace engine
} // namespace tinydb
