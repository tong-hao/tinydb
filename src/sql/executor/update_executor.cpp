#include "common/logger.h"
#include "update_executor.h"
#include "executor.h"

namespace tinydb {
namespace engine {

ExecutionResult UpdateExecutor::execute(const sql::UpdateStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("UPDATE executed (simulated - no storage engine)");
    }

    if (stmt) {
        std::string table_name = stmt->table();

        // 检查表是否存在
        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 获取或开始事务
        storage::Transaction* txn = nullptr;
        bool auto_txn = false;
        if (!txn) {
            txn = storage_engine_->beginTransaction();
            
            auto_txn = true;
        }

        // 获取排他锁（写锁）
        if (!Executor::acquireTableLock(storage_engine_, txn, table_name, storage::LockType::EXCLUSIVE)) {
            if (auto_txn) {
                storage_engine_->abortTransaction(txn);
                
            }
            return ExecutionResult::error("Failed to acquire lock on table: " + table_name);
        }

        // 获取 WHERE 条件
        const sql::Expression* where_condition = stmt->whereCondition();

        // 获取赋值列表
        const auto& assignments = stmt->assignments();

        // 遍历表中的所有行，收集需要更新的TID和更新后的元组
        auto iter = storage_engine_->scan(table_name);
        int update_count = 0;

        // 先收集需要更新的记录，避免在迭代过程中修改表导致迭代器失效
        struct UpdateRecord {
            storage::TID tid;
            storage::Tuple updated_tuple;
        };
        std::vector<UpdateRecord> records_to_update;

        while (iter.hasNext()) {
            auto tuple = iter.getNext();
            storage::TID tid = iter.getCurrentTID();

            // 应用 WHERE 条件过滤
            if (where_condition && !Executor::evaluateWhereCondition(tuple, where_condition, &table_meta->schema)) {
                continue;
            }

            // 创建更新后的元组
            storage::Tuple updated_tuple(&table_meta->schema);

            // 先复制原元组的所有字段
            for (size_t i = 0; i < tuple.getFieldCount(); ++i) {
                updated_tuple.addField(tuple.getField(i));
            }

            // 应用赋值
            for (const auto& assignment : assignments) {
                const std::string& col_name = assignment.first;
                const sql::Expression* value_expr = assignment.second.get();

                int col_idx = table_meta->schema.findColumnIndex(col_name);
                if (col_idx < 0) {
                    Executor::releaseTableLock(storage_engine_, txn, table_name);
                    if (auto_txn) {
                        storage_engine_->abortTransaction(txn);
                        
                    }
                    return ExecutionResult::error("Unknown column: " + col_name);
                }

                // 使用表达式求值来支持算术运算和其他复杂表达式
                storage::Field field = Executor::evaluateExpression(value_expr, tuple, &table_meta->schema);

                // Debug output
                LOG_INFO("Updating column " << col_name << " at index " << col_idx
                         << " with value " << field.toString() << " (type=" << static_cast<int>(field.getType()) << ")");

                updated_tuple.setField(col_idx, field);
            }

            // 收集记录
            records_to_update.push_back({tid, std::move(updated_tuple)});
        }

        // 现在执行更新操作（在迭代完成后）
        for (auto& record : records_to_update) {
            // 使用存储引擎的 update 方法
            if (!storage_engine_->update(table_name, record.tid, record.updated_tuple)) {
                Executor::releaseTableLock(storage_engine_, txn, table_name);
                if (auto_txn) {
                    storage_engine_->abortTransaction(txn);
                    
                }
                return ExecutionResult::error("Failed to update row");
            }

            update_count++;
        }

        // 释放锁
        Executor::releaseTableLock(storage_engine_, txn, table_name);

        // 如果没有显式事务，自动提交
        if (auto_txn) {
            if (!storage_engine_->commitTransaction(txn)) {
                
                return ExecutionResult::error("Failed to commit update");
            }
            
        }

        return ExecutionResult::ok("UPDATE " + std::to_string(update_count) + " row(s) in table " + table_name);
    }

    return ExecutionResult::ok("UPDATE executed");
}

} // namespace engine
} // namespace tinydb
