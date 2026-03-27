#include "insert_executor.h"
#include "common/logger.h"

namespace tinydb {
namespace engine {

ExecutionResult InsertExecutor::execute(const sql::InsertStmt* stmt) {
    if (!storage_engine_) {
        return ExecutionResult::ok("INSERT executed (simulated - no storage engine)");
    }

    // Phase 阶段：实际插入数据
    if (stmt) {
        std::string table_name = stmt->table();

        if (!storage_engine_->tableExists(table_name)) {
            return ExecutionResult::error("Table does not exist: " + table_name);
        }

        auto table_meta = storage_engine_->getTable(table_name);
        if (!table_meta) {
            return ExecutionResult::error("Failed to get table metadata: " + table_name);
        }

        // 获取或开始事务
        storage::Transaction* txn = current_txn_;
        bool auto_txn = false;
        if (!txn) {
            txn = storage_engine_->beginTransaction();
            current_txn_ = txn;  // 设置当前事务
            auto_txn = true;
        }

        // 获取排他锁（写锁）
        if (!acquireTableLock(table_name, storage::LockType::EXCLUSIVE)) {
            if (auto_txn) {
                storage_engine_->abortTransaction(txn);
                current_txn_ = nullptr;
            }
            return ExecutionResult::error("Failed to acquire lock on table: " + table_name);
        }

        const auto& columns = stmt->columns();
        int64_t inserted_count = 0;

        // 处理多行插入
        if (stmt->isMultiRow()) {
            const auto& multi_values = stmt->multiRowValues();
            for (const auto& row_values : multi_values) {
                // 构建元组
                storage::Tuple tuple(&table_meta->schema);

                for (size_t i = 0; i < row_values.size(); ++i) {
                    const auto* literal = dynamic_cast<const sql::LiteralExpr*>(row_values[i].get());
                    if (literal) {
                        std::string val = literal->value();

                        // 确定列索引
                        size_t col_idx = i;
                        if (i < columns.size()) {
                            col_idx = table_meta->schema.findColumnIndex(columns[i]);
                            if (col_idx == static_cast<size_t>(-1)) {
                                releaseTableLock(table_name);
                                if (auto_txn) {
                                    storage_engine_->abortTransaction(txn);
                                    current_txn_ = nullptr;
                                }
                                return ExecutionResult::error("Unknown column: " + columns[i]);
                            }
                        }

                        // 根据列类型创建字段
                        const auto& col_def = table_meta->schema.getColumn(col_idx);
                        storage::Field field;

                        if (val == "NULL") {
                            // NULL 值处理 - 使用默认构造函数创建 NULL 字段
                            field = storage::Field();
                        } else {
                            switch (col_def.type) {
                                case storage::DataType::INT32:
                                    field = storage::Field(static_cast<int32_t>(std::stoll(val)));
                                    break;
                                case storage::DataType::INT64:
                                    field = storage::Field(static_cast<int64_t>(std::stoll(val)));
                                    break;
                                case storage::DataType::FLOAT:
                                    field = storage::Field(static_cast<float>(std::stod(val)));
                                    break;
                                case storage::DataType::DOUBLE:
                                    field = storage::Field(static_cast<double>(std::stod(val)));
                                    break;
                                case storage::DataType::DECIMAL:
                                    field = storage::Field(std::stod(val));
                                    break;
                                case storage::DataType::BOOL:
                                    field = storage::Field(val == "true" || val == "1");
                                    break;
                                case storage::DataType::CHAR:
                                case storage::DataType::VARCHAR:
                                    // 去除引号
                                    if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                                        val = val.substr(1, val.size() - 2);
                                    }
                                    field = storage::Field(val, col_def.type);
                                    break;
                                default:
                                    field = storage::Field(val, storage::DataType::VARCHAR);
                                    break;
                            }
                        }

                        tuple.setField(col_idx, field);
                    }
                }

                // 插入数据
                auto tid = storage_engine_->insert(table_name, tuple);

                if (!tid.isValid()) {
                    releaseTableLock(table_name);
                    if (auto_txn) {
                        storage_engine_->abortTransaction(txn);
                        current_txn_ = nullptr;
                    }
                    return ExecutionResult::error("Failed to insert row");
                }

                // 记录插入（用于回滚）
                if (txn) {
                    txn->addInsertRecord(table_name, tid);
                }

                inserted_count++;
            }
        } else {
            // 单行插入（原有逻辑）
            const auto& values = stmt->values();
            storage::Tuple tuple(&table_meta->schema);

            for (size_t i = 0; i < values.size(); ++i) {
                const auto* literal = dynamic_cast<const sql::LiteralExpr*>(values[i].get());
                if (literal) {
                    std::string val = literal->value();

                    // 确定列索引
                    size_t col_idx = i;
                    if (i < columns.size()) {
                        col_idx = table_meta->schema.findColumnIndex(columns[i]);
                        if (col_idx == static_cast<size_t>(-1)) {
                            releaseTableLock(table_name);
                            if (auto_txn) {
                                storage_engine_->abortTransaction(txn);
                                current_txn_ = nullptr;
                            }
                            return ExecutionResult::error("Unknown column: " + columns[i]);
                        }
                    }

                    // 根据列类型创建字段
                    const auto& col_def = table_meta->schema.getColumn(col_idx);
                    storage::Field field;

                    if (val == "NULL") {
                        field = storage::Field();
                    } else {
                        switch (col_def.type) {
                            case storage::DataType::INT32:
                                field = storage::Field(static_cast<int32_t>(std::stoll(val)));
                                break;
                            case storage::DataType::INT64:
                                field = storage::Field(static_cast<int64_t>(std::stoll(val)));
                                break;
                            case storage::DataType::FLOAT:
                                field = storage::Field(static_cast<float>(std::stod(val)));
                                break;
                            case storage::DataType::DOUBLE:
                                field = storage::Field(static_cast<double>(std::stod(val)));
                                break;
                            case storage::DataType::DECIMAL:
                                field = storage::Field(std::stod(val));
                                break;
                            case storage::DataType::BOOL:
                                field = storage::Field(val == "true" || val == "1");
                                break;
                            case storage::DataType::CHAR:
                            case storage::DataType::VARCHAR:
                                // 去除引号
                                if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                                    val = val.substr(1, val.size() - 2);
                                }
                                field = storage::Field(val, col_def.type);
                                break;
                            default:
                                field = storage::Field(val, storage::DataType::VARCHAR);
                                break;
                        }
                    }

                    tuple.setField(col_idx, field);
                }
            }

            // 插入数据
            auto tid = storage_engine_->insert(table_name, tuple);

            // 释放锁
            releaseTableLock(table_name);

            if (!tid.isValid()) {
                if (auto_txn) {
                    storage_engine_->abortTransaction(txn);
                    current_txn_ = nullptr;
                }
                return ExecutionResult::error("Failed to insert row");
            }

            // 记录插入（用于回滚）
            if (txn) {
                txn->addInsertRecord(table_name, tid);
            }

            inserted_count = 1;
        }

        // 释放锁
        releaseTableLock(table_name);

        // 如果没有显式事务，自动提交
        if (auto_txn) {
            if (!storage_engine_->commitTransaction(txn)) {
                current_txn_ = nullptr;
                return ExecutionResult::error("Failed to commit transaction");
            }
            current_txn_ = nullptr;
        }

        return ExecutionResult::ok("INSERT " + std::to_string(inserted_count));
    }

    return ExecutionResult::ok("INSERT executed");
}

} // namespace engine
} // namespace tinydb
