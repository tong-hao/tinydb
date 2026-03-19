#pragma once

// 存储引擎主头文件
#include "page.h"
#include "disk_manager.h"
#include "buffer_pool.h"
#include "tuple.h"
#include "table.h"
#include "wal.h"
#include "transaction.h"
#include "lock_manager.h"

namespace tinydb {
namespace storage {

// 存储引擎配置
struct StorageConfig {
    std::string db_file_path = "tinydb.db";
    std::string wal_file_path = "tinydb.wal";
    size_t buffer_pool_size = 1024;  // 页数
};

// 存储引擎类
class StorageEngine {
public:
    explicit StorageEngine(const StorageConfig& config);
    ~StorageEngine();

    // 禁止拷贝
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;

    // 初始化存储引擎
    bool initialize();

    // 获取磁盘管理器
    DiskManager* getDiskManager() { return disk_manager_.get(); }

    // 获取缓冲池管理器
    BufferPoolManager* getBufferPool() { return buffer_pool_.get(); }

    // 获取表管理器
    TableManager* getTableManager() { return table_manager_.get(); }

    // 获取WAL管理器
    WALManager* getWALManager() { return wal_manager_.get(); }

    // 获取事务管理器
    TransactionManager* getTransactionManager() { return transaction_manager_.get(); }

    // 获取锁管理器
    LockManager* getLockManager() { return lock_manager_.get(); }

    // 创建表
    bool createTable(const std::string& table_name, const Schema& schema);

    // 删除表
    bool dropTable(const std::string& table_name);

    // 获取表
    std::shared_ptr<TableMeta> getTable(const std::string& table_name);

    // 检查表是否存在
    bool tableExists(const std::string& table_name);

    // 获取所有表名
    std::vector<std::string> getAllTableNames();

    // 插入数据
    TID insert(const std::string& table_name, const Tuple& tuple);

    // 删除数据
    bool remove(const std::string& table_name, const TID& tid);

    // 更新数据（阶段三新增）
    bool update(const std::string& table_name, const TID& tid, const Tuple& new_tuple);

    // 获取数据
    Tuple get(const std::string& table_name, const TID& tid);

    // 创建表扫描迭代器
    TableIterator scan(const std::string& table_name);

    // 强制刷盘（检查点）
    bool checkpoint();

    // 关闭存储引擎
    void shutdown();

    // 事务相关方法（阶段三新增）
    Transaction* beginTransaction();
    bool commitTransaction(Transaction* txn);
    bool abortTransaction(Transaction* txn);

    // 锁相关方法（阶段三新增）
    bool lockTable(Transaction* txn, const std::string& table_name, LockType lock_type);
    bool unlockTable(Transaction* txn, const std::string& table_name);

private:
    StorageConfig config_;
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_;
    std::unique_ptr<TableManager> table_manager_;
    std::unique_ptr<WALManager> wal_manager_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<LockManager> lock_manager_;
    bool initialized_ = false;
};

} // namespace storage
} // namespace tinydb
