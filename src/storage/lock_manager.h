#pragma once

#include "tuple.h"
#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <vector>
#include <memory>
#include <chrono>

namespace tinydb {
namespace storage {

// 前向声明
class Transaction;
using TransactionId = uint32_t;

// 锁类型
enum class LockType {
    SHARED,     // 共享锁（读锁）
    EXCLUSIVE   // 排他锁（写锁）
};

// 锁模式
enum class LockMode {
    TABLE,      // 表级锁
    ROW,        // 行级锁（阶段五实现）
    PREDICATE   // 谓词锁（用于Serializable隔离级别）
};

// 行级锁资源标识
struct RowLockResource {
    std::string table_name;
    TID tid;

    RowLockResource(const std::string& table, const TID& row_tid)
        : table_name(table), tid(row_tid) {}

    bool operator==(const RowLockResource& other) const {
        return table_name == other.table_name && tid == other.tid;
    }
};

// 为RowLockResource提供hash支持
struct RowLockResourceHash {
    std::size_t operator()(const RowLockResource& res) const {
        return std::hash<std::string>{}(res.table_name) ^
               (std::hash<uint64_t>{}(res.tid.page_id) << 1) ^
               (std::hash<uint32_t>{}(res.tid.slot_id) << 2);
    }
};

// 谓词锁资源标识（用于Serializable隔离级别）
struct PredicateLockResource {
    std::string table_name;
    std::string predicate;  // 查询条件（如 "age > 18"）
    std::string index_name; // 可选：用于索引范围锁

    PredicateLockResource(const std::string& table, const std::string& pred,
                          const std::string& idx = "")
        : table_name(table), predicate(pred), index_name(idx) {}

    bool operator==(const PredicateLockResource& other) const {
        return table_name == other.table_name &&
               predicate == other.predicate &&
               index_name == other.index_name;
    }
};

// 为PredicateLockResource提供hash支持
struct PredicateLockResourceHash {
    std::size_t operator()(const PredicateLockResource& res) const {
        return std::hash<std::string>{}(res.table_name) ^
               (std::hash<std::string>{}(res.predicate) << 1) ^
               (std::hash<std::string>{}(res.index_name) << 2);
    }
};

// 锁请求
struct LockRequest {
    TransactionId txn_id;
    LockType lock_type;
    std::chrono::steady_clock::time_point request_time;

    LockRequest(TransactionId tid, LockType type)
        : txn_id(tid)
        , lock_type(type)
        , request_time(std::chrono::steady_clock::now()) {}
};

// 锁对象（表级锁）
class Lock {
public:
    explicit Lock(const std::string& resource_name);

    // 尝试获取锁
    bool tryLock(TransactionId txn_id, LockType lock_type);

    // 释放锁
    void unlock(TransactionId txn_id);

    // 检查事务是否持有锁
    bool isHeldBy(TransactionId txn_id) const;

    // 获取锁类型
    LockType getLockType() const { return current_lock_type_; }

    // 获取持有者数量
    size_t getHolderCount() const { return holders_.size(); }

    // 获取等待者数量
    size_t getWaiterCount() const { return waiters_.size(); }

    // 检查是否有等待者
    bool hasWaiters() const { return !waiters_.empty(); }

    // 获取资源名
    const std::string& getResourceName() const { return resource_name_; }

private:
    std::string resource_name_;
    LockType current_lock_type_;
    std::unordered_map<TransactionId, size_t> holders_;  // 事务ID -> 锁计数
    std::vector<LockRequest> waiters_;

    // 兼容锁类型检查
    bool isCompatible(LockType existing, LockType requested) const;

    // 授予锁给等待者
    void grantLocksToWaiters();
};

// 锁管理器
class LockManager {
public:
    LockManager();
    ~LockManager();

    // 禁止拷贝
    LockManager(const LockManager&) = delete;
    LockManager& operator=(const LockManager&) = delete;

    // 获取表级锁（阻塞直到成功或超时）
    // timeout_ms: 超时时间（毫秒），0表示无限等待
    bool lockTable(Transaction* txn, const std::string& table_name, LockType lock_type, uint32_t timeout_ms = 5000);

    // 释放表级锁
    bool unlockTable(Transaction* txn, const std::string& table_name);

    // 释放事务持有的所有锁
    void releaseAllLocks(Transaction* txn);

    // 检查死锁（简单实现：检测锁等待超时）
    bool detectDeadlock();

    // 获取锁信息（用于调试）
    std::string getLockInfo() const;

    // 获取行级锁（阻塞直到成功或超时）
    bool lockRow(Transaction* txn, const std::string& table_name, const TID& tid,
                 LockType lock_type, uint32_t timeout_ms = 5000);

    // 释放行级锁
    bool unlockRow(Transaction* txn, const std::string& table_name, const TID& tid);

    // 释放事务持有的所有行锁
    void releaseAllRowLocks(Transaction* txn);

    // 检查事务是否持有行锁
    bool isRowLockedBy(TransactionId txn_id, const std::string& table_name,
                        const TID& tid, LockType* out_type = nullptr) const;

    // 检查行是否被其他事务锁定
    bool isRowLocked(const std::string& table_name, const TID& tid,
                     LockType* out_type = nullptr) const;

    // 获取行锁等待队列长度（用于死锁检测）
    size_t getRowLockWaiterCount(const std::string& table_name, const TID& tid) const;

    // 检查事务是否持有表的锁
    bool isTableLockedBy(TransactionId txn_id, const std::string& table_name, LockType* out_type = nullptr) const;

    // ========== 谓词锁（用于Serializable隔离级别） ==========
    // 获取谓词锁
    bool lockPredicate(Transaction* txn, const std::string& table_name,
                       const std::string& predicate,
                       const std::string& index_name = "",
                       uint32_t timeout_ms = 5000);

    // 释放谓词锁
    bool unlockPredicate(Transaction* txn, const std::string& table_name,
                         const std::string& predicate,
                         const std::string& index_name = "");

    // 释放事务持有的所有谓词锁
    void releaseAllPredicateLocks(Transaction* txn);

    // 检查谓词是否被锁定（用于检测幻读冲突）
    bool isPredicateLocked(const std::string& table_name,
                           const std::string& predicate,
                           const std::string& index_name = "") const;

    // 检查谓词是否与现有谓词锁冲突
    // 例如：范围 [10, 20] 和 [15, 25] 冲突
    bool checkPredicateConflict(const std::string& table_name,
                                const std::string& predicate1,
                                const std::string& predicate2) const;

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;

    // 资源名 -> 锁对象（表级锁）
    std::unordered_map<std::string, std::unique_ptr<Lock>> locks_;

    // 行锁资源 -> 锁对象
    std::unordered_map<RowLockResource, std::unique_ptr<Lock>, RowLockResourceHash> row_locks_;

    // 事务ID -> 持有的锁列表（表锁）
    std::unordered_map<TransactionId, std::vector<std::string>> txn_locks_;

    // 事务ID -> 持有的行锁列表
    std::unordered_map<TransactionId, std::vector<RowLockResource>> txn_row_locks_;

    // 谓词锁资源 -> 锁对象
    std::unordered_map<PredicateLockResource, std::unique_ptr<Lock>, PredicateLockResourceHash> predicate_locks_;

    // 事务ID -> 持有的谓词锁列表
    std::unordered_map<TransactionId, std::vector<PredicateLockResource>> txn_predicate_locks_;

    // 获取或创建锁对象
    Lock* getOrCreateLock(const std::string& resource_name);
    Lock* getOrCreateRowLock(const RowLockResource& resource);
    Lock* getOrCreatePredicateLock(const PredicateLockResource& resource);

    // 移除空锁对象
    void cleanupEmptyLocks();
    void cleanupEmptyRowLocks();
    void cleanupEmptyPredicateLocks();
};

// 全局锁管理器
extern LockManager* g_lock_manager;

// 锁守卫（RAII）
class TableLockGuard {
public:
    TableLockGuard(LockManager* lock_mgr, Transaction* txn, const std::string& table_name, LockType lock_type);
    ~TableLockGuard();

    // 禁止拷贝
    TableLockGuard(const TableLockGuard&) = delete;
    TableLockGuard& operator=(const TableLockGuard&) = delete;

    // 允许移动
    TableLockGuard(TableLockGuard&& other) noexcept;
    TableLockGuard& operator=(TableLockGuard&& other) noexcept;

    // 检查是否成功获取锁
    bool isLocked() const { return locked_; }

    // 手动释放锁
    void unlock();

private:
    LockManager* lock_mgr_;
    Transaction* txn_;
    std::string table_name_;
    bool locked_;
};

// 行锁守卫（RAII）
class RowLockGuard {
public:
    RowLockGuard(LockManager* lock_mgr, Transaction* txn,
                 const std::string& table_name, const TID& tid, LockType lock_type);
    ~RowLockGuard();

    // 禁止拷贝
    RowLockGuard(const RowLockGuard&) = delete;
    RowLockGuard& operator=(const RowLockGuard&) = delete;

    // 允许移动
    RowLockGuard(RowLockGuard&& other) noexcept;
    RowLockGuard& operator=(RowLockGuard&& other) noexcept;

    // 检查是否成功获取锁
    bool isLocked() const { return locked_; }

    // 手动释放锁
    void unlock();

private:
    LockManager* lock_mgr_;
    Transaction* txn_;
    std::string table_name_;
    TID tid_;
    bool locked_;
};

// 谓词锁守卫（RAII）
class PredicateLockGuard {
public:
    PredicateLockGuard(LockManager* lock_mgr, Transaction* txn,
                       const std::string& table_name,
                       const std::string& predicate,
                       const std::string& index_name = "");
    ~PredicateLockGuard();

    // 禁止拷贝
    PredicateLockGuard(const PredicateLockGuard&) = delete;
    PredicateLockGuard& operator=(const PredicateLockGuard&) = delete;

    // 允许移动
    PredicateLockGuard(PredicateLockGuard&& other) noexcept;
    PredicateLockGuard& operator=(PredicateLockGuard&& other) noexcept;

    // 检查是否成功获取锁
    bool isLocked() const { return locked_; }

    // 手动释放锁
    void unlock();

private:
    LockManager* lock_mgr_;
    Transaction* txn_;
    std::string table_name_;
    std::string predicate_;
    std::string index_name_;
    bool locked_;
};

} // namespace storage
} // namespace tinydb
