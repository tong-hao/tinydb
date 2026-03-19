#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include "wal.h"
#include "tuple.h"

namespace tinydb {
namespace storage {

// 事务状态
enum class TransactionState {
    ACTIVE,      // 活跃状态
    COMMITTING,  // 正在提交
    COMMITTED,   // 已提交
    ABORTING,    // 正在回滚
    ABORTED      // 已回滚
};

// 事务上下文
class Transaction {
public:
    explicit Transaction(TransactionId id);
    ~Transaction();

    // 获取事务ID
    TransactionId getId() const { return txn_id_; }

    // 获取事务状态
    TransactionState getState() const { return state_; }
    void setState(TransactionState state) { state_ = state; }

    // 获取开始时间
    uint64_t getStartTime() const { return start_time_; }

    // 记录修改过的页（用于回滚和刷盘）
    void addModifiedPage(uint32_t page_id);
    const std::vector<uint32_t>& getModifiedPages() const { return modified_pages_; }
    void clearModifiedPages() { modified_pages_.clear(); }

    // 记录插入/删除的元组（用于回滚）
    struct InsertRecord {
        std::string table_name;
        TID tid;
    };
    struct DeleteRecord {
        std::string table_name;
        Tuple old_tuple;
        TID tid;
    };

    void addInsertRecord(const std::string& table_name, const TID& tid);
    void addDeleteRecord(const std::string& table_name, const Tuple& old_tuple, const TID& tid);

    const std::vector<InsertRecord>& getInsertRecords() const { return insert_records_; }
    const std::vector<DeleteRecord>& getDeleteRecords() const { return delete_records_; }

    void clearRecords() {
        insert_records_.clear();
        delete_records_.clear();
    }

private:
    TransactionId txn_id_;
    TransactionState state_;
    uint64_t start_time_;

    std::vector<uint32_t> modified_pages_;
    std::vector<InsertRecord> insert_records_;
    std::vector<DeleteRecord> delete_records_;
};

// 事务管理器
class TransactionManager {
public:
    TransactionManager();
    ~TransactionManager();

    // 禁止拷贝
    TransactionManager(const TransactionManager&) = delete;
    TransactionManager& operator=(const TransactionManager&) = delete;

    // 开始新事务
    Transaction* beginTransaction();

    // 提交事务
    bool commitTransaction(Transaction* txn);

    // 回滚事务
    bool abortTransaction(Transaction* txn);

    // 获取当前活跃事务
    Transaction* getTransaction(TransactionId txn_id);

    // 获取当前事务（单线程环境下使用）
    Transaction* getCurrentTransaction() const { return current_txn_; }
    void setCurrentTransaction(Transaction* txn) { current_txn_ = txn; }

    // 清理已完成的事务
    void cleanupCompletedTransactions();

    // 获取下一个事务ID
    TransactionId getNextTransactionId() { return next_txn_id_++; }

    // 检查是否有活跃事务
    bool hasActiveTransactions() const;

    // 获取活跃事务数量
    size_t getActiveTransactionCount() const;

private:
    std::atomic<TransactionId> next_txn_id_{1};
    std::unordered_map<TransactionId, std::unique_ptr<Transaction>> transactions_;
    mutable std::mutex mutex_;

    // 当前线程的事务（简化实现，假设单线程）
    Transaction* current_txn_ = nullptr;

    // 内部提交实现
    bool doCommit(Transaction* txn);

    // 内部回滚实现
    bool doAbort(Transaction* txn);
};

// 全局事务管理器（简化实现）
extern TransactionManager* g_transaction_manager;

} // namespace storage
} // namespace tinydb
