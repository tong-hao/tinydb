#include "storage_engine.h"
#include "common/logger.h"

namespace tinydb {
namespace storage {

StorageEngine::StorageEngine(const StorageConfig& config)
    : config_(config), initialized_(false) {}

StorageEngine::~StorageEngine() {
    shutdown();
}

bool StorageEngine::initialize() {
    if (initialized_) {
        return true;
    }

    LOG_INFO("Initializing storage engine...");

    // 创建磁盘管理器
    disk_manager_ = std::make_unique<DiskManager>(config_.db_file_path);

    // 创建缓冲池管理器
    buffer_pool_ = std::make_unique<BufferPoolManager>(config_.buffer_pool_size, disk_manager_.get());

    // 创建WAL管理器
    wal_manager_ = std::make_unique<WALManager>(config_.wal_file_path);
    if (!wal_manager_->initialize()) {
        LOG_ERROR("Failed to initialize WAL manager");
        return false;
    }

    // 创建表管理器
    table_manager_ = std::make_unique<TableManager>(buffer_pool_.get());

    // 创建事务管理器（阶段三新增）
    transaction_manager_ = std::make_unique<TransactionManager>();
    g_transaction_manager = transaction_manager_.get();

    // 创建锁管理器（阶段三新增）
    lock_manager_ = std::make_unique<LockManager>();
    g_lock_manager = lock_manager_.get();

    // Phase 4: 创建索引管理器
    index_manager_ = std::make_unique<IndexManager>(buffer_pool_.get());

    // Phase 4: 创建统计信息管理器
    stats_manager_ = std::make_unique<StatisticsManager>(buffer_pool_.get());

    initialized_ = true;
    LOG_INFO("Storage engine initialized successfully");
    return true;
}

bool StorageEngine::createTable(const std::string& table_name, const Schema& schema) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->createTable(table_name, schema);
}

bool StorageEngine::dropTable(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->dropTable(table_name);
}

std::shared_ptr<TableMeta> StorageEngine::getTable(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return nullptr;
    }
    return table_manager_->getTable(table_name);
}

bool StorageEngine::tableExists(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->tableExists(table_name);
}

std::vector<std::string> StorageEngine::getAllTableNames() {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return {};
    }
    return table_manager_->getAllTableNames();
}

TID StorageEngine::insert(const std::string& table_name, const Tuple& tuple) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return TID();
    }
    return table_manager_->insertTuple(table_name, tuple);
}

bool StorageEngine::remove(const std::string& table_name, const TID& tid) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }

    // 记录到WAL（如果当前有活跃事务）
    Transaction* txn = transaction_manager_->getCurrentTransaction();
    if (txn && txn->getState() == TransactionState::ACTIVE) {
        // 获取旧元组用于回滚
        Tuple old_tuple = table_manager_->getTuple(table_name, tid);
        if (old_tuple.getFieldCount() > 0) {
            txn->addDeleteRecord(table_name, old_tuple, tid);
        }
    }

    return table_manager_->deleteTuple(table_name, tid);
}

bool StorageEngine::update(const std::string& table_name, const TID& tid, const Tuple& new_tuple) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }

    // 获取旧元组
    Tuple old_tuple = table_manager_->getTuple(table_name, tid);
    if (old_tuple.getFieldCount() == 0) {
        LOG_ERROR("Failed to get old tuple for update");
        return false;
    }

    // 记录到WAL（如果当前有活跃事务）
    Transaction* txn = transaction_manager_->getCurrentTransaction();
    if (txn && txn->getState() == TransactionState::ACTIVE) {
        // 记录删除操作
        txn->addDeleteRecord(table_name, old_tuple, tid);
    }

    // 删除旧元组
    if (!table_manager_->deleteTuple(table_name, tid)) {
        LOG_ERROR("Failed to delete old tuple during update");
        return false;
    }

    // 插入新元组
    TID new_tid = table_manager_->insertTuple(table_name, new_tuple);
    if (!new_tid.isValid()) {
        // 插入失败，尝试恢复旧元组
        LOG_ERROR("Failed to insert new tuple during update");
        // 注意：这里简化处理，实际应该通过WAL恢复
        return false;
    }

    // 注意：UPDATE 操作插入的新元组不应该被回滚删除
    // 因为 UPDATE 是原子操作，回滚时应该恢复到旧状态（重新插入旧元组）
    // 新插入的元组是更新后的有效数据，不应该被删除
    // 所以这里不调用 txn->addInsertRecord()

    LOG_INFO("Updated tuple " << tid.toString() << " to " << new_tid.toString() << " in table " << table_name);
    return true;
}

Tuple StorageEngine::get(const std::string& table_name, const TID& tid) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return Tuple();
    }
    return table_manager_->getTuple(table_name, tid);
}

TableIterator StorageEngine::scan(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return TableIterator(nullptr, INVALID_PAGE_ID, nullptr);
    }
    return table_manager_->makeIterator(table_name);
}

bool StorageEngine::checkpoint() {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }

    LOG_INFO("Performing checkpoint...");

    // 刷新所有脏页
    buffer_pool_->flushAllPages();

    // 刷新WAL
    wal_manager_->flush();

    LOG_INFO("Checkpoint completed");
    return true;
}

void StorageEngine::shutdown() {
    if (!initialized_) {
        return;
    }

    LOG_INFO("Shutting down storage engine...");

    // 执行检查点
    checkpoint();

    // 清理全局指针
    g_lock_manager = nullptr;
    g_transaction_manager = nullptr;

    // 关闭组件（按照依赖顺序反向关闭）
    LOG_INFO("Resetting lock_manager...");
    lock_manager_.reset();
    LOG_INFO("lock_manager reset done");

    LOG_INFO("Resetting transaction_manager...");
    transaction_manager_.reset();
    LOG_INFO("transaction_manager reset done");

    LOG_INFO("Resetting table_manager...");
    table_manager_.reset();
    LOG_INFO("table_manager reset done");

    LOG_INFO("Resetting wal_manager...");
    wal_manager_.reset();
    LOG_INFO("wal_manager reset done");

    LOG_INFO("Resetting buffer_pool...");
    buffer_pool_.reset();
    LOG_INFO("buffer_pool reset done");

    LOG_INFO("Resetting disk_manager...");
    disk_manager_.reset();
    LOG_INFO("disk_manager reset done");

    initialized_ = false;
    LOG_INFO("Storage engine shutdown complete");
}

// 事务相关方法（阶段三新增）
Transaction* StorageEngine::beginTransaction() {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return nullptr;
    }
    return transaction_manager_->beginTransaction();
}

bool StorageEngine::commitTransaction(Transaction* txn) {
    if (!initialized_ || !txn) {
        LOG_ERROR("Cannot commit transaction: storage not initialized or null transaction");
        return false;
    }

    // 1. 释放所有锁
    lock_manager_->releaseAllLocks(txn);

    // 2. 提交事务
    bool result = transaction_manager_->commitTransaction(txn);

    // 3. 清理已完成的事务
    transaction_manager_->cleanupCompletedTransactions();

    return result;
}

bool StorageEngine::abortTransaction(Transaction* txn) {
    if (!initialized_ || !txn) {
        LOG_ERROR("Cannot abort transaction: storage not initialized or null transaction");
        return false;
    }

    // 1. 释放所有锁
    lock_manager_->releaseAllLocks(txn);

    // 2. 回滚事务
    bool result = transaction_manager_->abortTransaction(txn);

    // 3. 清理已完成的事务
    transaction_manager_->cleanupCompletedTransactions();

    return result;
}

// 锁相关方法（阶段三新增）
bool StorageEngine::lockTable(Transaction* txn, const std::string& table_name, LockType lock_type) {
    if (!initialized_ || !txn) {
        LOG_ERROR("Cannot lock table: storage not initialized or null transaction");
        return false;
    }
    return lock_manager_->lockTable(txn, table_name, lock_type);
}

bool StorageEngine::unlockTable(Transaction* txn, const std::string& table_name) {
    if (!initialized_ || !txn) {
        LOG_ERROR("Cannot unlock table: storage not initialized or null transaction");
        return false;
    }
    return lock_manager_->unlockTable(txn, table_name);
}

// Phase 4: 索引操作
bool StorageEngine::createIndex(const std::string& index_name, const std::string& table_name,
                                const std::string& column_name, bool is_unique) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }

    // 检查表是否存在
    if (!table_manager_->tableExists(table_name)) {
        LOG_ERROR("Table " << table_name << " does not exist");
        return false;
    }

    // 创建索引
    if (!index_manager_->createIndex(index_name, table_name, column_name, is_unique)) {
        LOG_ERROR("Failed to create index " << index_name);
        return false;
    }

    // 构建索引：扫描表并插入所有数据
    auto index = index_manager_->getIndex(index_name);
    if (!index) {
        LOG_ERROR("Failed to get created index " << index_name);
        return false;
    }

    auto table = table_manager_->getTable(table_name);
    if (!table) {
        LOG_ERROR("Failed to get table " << table_name);
        return false;
    }

    // 找到列索引
    int col_idx = -1;
    for (size_t i = 0; i < table->schema.getColumnCount(); ++i) {
        if (table->schema.getColumn(i).name == column_name) {
            col_idx = static_cast<int>(i);
            break;
        }
    }

    if (col_idx < 0) {
        LOG_ERROR("Column " << column_name << " not found in table " << table_name);
        return false;
    }

    // TODO: 这里太耗时了，应该通过后台线程进行
    // 扫描表数据并构建索引
    auto it = table_manager_->makeIterator(table_name);
    while (it.hasNext()) {
        Tuple tuple = it.getNext();
        TID tid = it.getCurrentTID();

        if (col_idx < static_cast<int>(tuple.getFieldCount())) {
            IndexKey key = IndexKey::fromField(tuple.getField(col_idx));
            if (!index->insert(key, tid)) {
                LOG_WARN("Failed to insert index entry for TID " << tid.toString());
            }
        }
    }

    LOG_INFO("Created index " << index_name << " on " << table_name << "." << column_name);
    return true;
}

bool StorageEngine::dropIndex(const std::string& index_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return index_manager_->dropIndex(index_name);
}

std::shared_ptr<BTreeIndex> StorageEngine::getIndex(const std::string& index_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return nullptr;
    }
    return index_manager_->getIndex(index_name);
}

std::vector<TID> StorageEngine::indexLookup(const std::string& table_name,
                                            const std::string& column_name,
                                            const IndexKey& key) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return {};
    }

    auto index = index_manager_->getColumnIndex(table_name, column_name);
    if (!index) {
        return {};
    }

    return index->lookup(key);
}

std::vector<TID> StorageEngine::indexRangeQuery(const std::string& table_name,
                                                const std::string& column_name,
                                                const IndexKey& start_key,
                                                const IndexKey& end_key) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return {};
    }

    auto index = index_manager_->getColumnIndex(table_name, column_name);
    if (!index) {
        return {};
    }

    return index->rangeQuery(start_key, end_key);
}

// Phase 4: 统计信息
bool StorageEngine::analyzeTable(const std::string& table_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return stats_manager_->analyzeTable(table_name, table_manager_.get());
}

// ALTER TABLE: 添加列
bool StorageEngine::addColumn(const std::string& table_name, const ColumnDef& column_def) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->addColumn(table_name, column_def);
}

// ALTER TABLE: 删除列
bool StorageEngine::dropColumn(const std::string& table_name, const std::string& column_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->dropColumn(table_name, column_name);
}

// ALTER TABLE: 修改列
bool StorageEngine::modifyColumn(const std::string& table_name, const std::string& column_name, const ColumnDef& new_def) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->modifyColumn(table_name, column_name, new_def);
}

// ALTER TABLE: 重命名表
bool StorageEngine::renameTable(const std::string& old_name, const std::string& new_name) {
    if (!initialized_) {
        LOG_ERROR("Storage engine not initialized");
        return false;
    }
    return table_manager_->renameTable(old_name, new_name);
}

} // namespace storage
} // namespace tinydb
