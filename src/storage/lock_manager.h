#pragma once

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
    ROW         // 行级锁（阶段四实现）
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

    // 检查事务是否持有表的锁
    bool isTableLockedBy(TransactionId txn_id, const std::string& table_name, LockType* out_type = nullptr) const;

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;

    // 资源名 -> 锁对象
    std::unordered_map<std::string, std::unique_ptr<Lock>> locks_;

    // 事务ID -> 持有的锁列表
    std::unordered_map<TransactionId, std::vector<std::string>> txn_locks_;

    // 获取或创建锁对象
    Lock* getOrCreateLock(const std::string& resource_name);

    // 移除空锁对象
    void cleanupEmptyLocks();
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

} // namespace storage
} // namespace tinydb
