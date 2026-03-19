#include "lock_manager.h"
#include "transaction.h"
#include "common/logger.h"
#include <algorithm>
#include <thread>

namespace tinydb {
namespace storage {

// 全局锁管理器实例
LockManager* g_lock_manager = nullptr;

// Lock 实现
Lock::Lock(const std::string& resource_name)
    : resource_name_(resource_name)
    , current_lock_type_(LockType::SHARED) {}

bool Lock::tryLock(TransactionId txn_id, LockType lock_type) {
    // 检查事务是否已经持有锁
    auto it = holders_.find(txn_id);
    if (it != holders_.end()) {
        // 已经持有锁，检查是否需要升级
        if (current_lock_type_ == LockType::SHARED && lock_type == LockType::EXCLUSIVE) {
            // 锁升级：只有当前只有一个持有者时才允许
            if (holders_.size() == 1) {
                current_lock_type_ = LockType::EXCLUSIVE;
                it->second++;
                LOG_DEBUG("Lock upgraded to EXCLUSIVE for txn " << txn_id << " on " << resource_name_);
                return true;
            } else {
                // 有其他共享锁持有者，不能升级
                return false;
            }
        }
        // 已经持有兼容的锁，增加计数
        it->second++;
        return true;
    }

    // 检查兼容性
    if (!holders_.empty() && !isCompatible(current_lock_type_, lock_type)) {
        return false;
    }

    // 授予锁
    holders_[txn_id] = 1;
    if (holders_.size() == 1) {
        current_lock_type_ = lock_type;
    }

    LOG_DEBUG("Lock granted to txn " << txn_id << " on " << resource_name_
              << " type=" << (lock_type == LockType::SHARED ? "S" : "X"));
    return true;
}

void Lock::unlock(TransactionId txn_id) {
    auto it = holders_.find(txn_id);
    if (it == holders_.end()) {
        return;  // 事务不持有此锁
    }

    // 减少计数
    it->second--;
    if (it->second == 0) {
        holders_.erase(it);
        LOG_DEBUG("Lock released by txn " << txn_id << " on " << resource_name_);

        // 如果没有持有者了，重置锁类型
        if (holders_.empty()) {
            current_lock_type_ = LockType::SHARED;

            // 尝试授予等待者
            grantLocksToWaiters();
        }
    }
}

bool Lock::isHeldBy(TransactionId txn_id) const {
    return holders_.find(txn_id) != holders_.end();
}

bool Lock::isCompatible(LockType existing, LockType requested) const {
    if (existing == LockType::EXCLUSIVE) {
        // 排他锁与任何锁都不兼容
        return false;
    }
    if (requested == LockType::EXCLUSIVE) {
        // 请求排他锁，但已有共享锁
        return false;
    }
    // 都是共享锁，兼容
    return true;
}

void Lock::grantLocksToWaiters() {
    // 简化实现：只授予第一个兼容的等待者
    // 实际实现可能需要更复杂的队列管理

    if (holders_.empty() && !waiters_.empty()) {
        // 没有持有者，可以授予第一个等待者
        auto& first_waiter = waiters_.front();
        holders_[first_waiter.txn_id] = 1;
        current_lock_type_ = first_waiter.lock_type;
        waiters_.erase(waiters_.begin());

        LOG_DEBUG("Lock granted to waiting txn " << first_waiter.txn_id
                  << " on " << resource_name_);
    }
}

// LockManager 实现
LockManager::LockManager() {
    LOG_INFO("LockManager initialized");
}

LockManager::~LockManager() {
    std::lock_guard<std::mutex> lock(mutex_);
    locks_.clear();
    txn_locks_.clear();
}

bool LockManager::lockTable(Transaction* txn, const std::string& table_name, LockType lock_type, uint32_t timeout_ms) {
    if (!txn) {
        LOG_ERROR("Cannot lock table for null transaction");
        return false;
    }

    TransactionId txn_id = txn->getId();
    std::string resource_name = "table:" + table_name;

    std::unique_lock<std::mutex> lock(mutex_);

    // 获取或创建锁对象
    Lock* lock_obj = getOrCreateLock(resource_name);

    // 尝试获取锁
    auto start_time = std::chrono::steady_clock::now();
    while (!lock_obj->tryLock(txn_id, lock_type)) {
        // 无法立即获取锁，需要等待
        LOG_DEBUG("Txn " << txn_id << " waiting for lock on " << table_name);

        // 检查超时
        if (timeout_ms > 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            if (elapsed >= timeout_ms) {
                LOG_WARN("Txn " << txn_id << " timeout waiting for lock on " << table_name);
                return false;
            }
        }

        // 等待一段时间再重试
        // 实际实现应该使用条件变量来唤醒
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        lock.lock();
    }

    // 记录事务持有的锁
    txn_locks_[txn_id].push_back(resource_name);

    LOG_INFO("Txn " << txn_id << " acquired "
             << (lock_type == LockType::SHARED ? "SHARED" : "EXCLUSIVE")
             << " lock on table " << table_name);
    return true;
}

bool LockManager::unlockTable(Transaction* txn, const std::string& table_name) {
    if (!txn) {
        LOG_ERROR("Cannot unlock table for null transaction");
        return false;
    }

    TransactionId txn_id = txn->getId();
    std::string resource_name = "table:" + table_name;

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = locks_.find(resource_name);
    if (it == locks_.end()) {
        LOG_WARN("Trying to unlock non-existent lock on " << table_name);
        return false;
    }

    // 释放锁
    it->second->unlock(txn_id);

    // 从事务的锁列表中移除
    auto txn_it = txn_locks_.find(txn_id);
    if (txn_it != txn_locks_.end()) {
        auto& locks = txn_it->second;
        locks.erase(std::remove(locks.begin(), locks.end(), resource_name), locks.end());
        if (locks.empty()) {
            txn_locks_.erase(txn_it);
        }
    }

    LOG_INFO("Txn " << txn_id << " released lock on table " << table_name);
    return true;
}

void LockManager::releaseAllLocks(Transaction* txn) {
    if (!txn) return;

    TransactionId txn_id = txn->getId();

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = txn_locks_.find(txn_id);
    if (it == txn_locks_.end()) {
        return;  // 事务没有持有任何锁
    }

    // 释放所有锁
    for (const auto& resource_name : it->second) {
        auto lock_it = locks_.find(resource_name);
        if (lock_it != locks_.end()) {
            lock_it->second->unlock(txn_id);
            LOG_DEBUG("Released lock " << resource_name << " for txn " << txn_id);
        }
    }

    txn_locks_.erase(it);
    LOG_INFO("Released all locks for txn " << txn_id);
}

bool LockManager::detectDeadlock() {
    // 简化实现：检测长时间等待的锁请求
    // 实际实现应该构建等待图并检测环

    std::lock_guard<std::mutex> lock(mutex_);

    // 检查是否有等待时间过长的请求
    // 简化：只检查锁对象是否有等待者
    // 实际应该检查等待时间
    (void)std::chrono::steady_clock::now();  // 避免未使用变量警告

    return false;
}

std::string LockManager::getLockInfo() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string info = "Lock Manager Status:\n";
    info += "==================\n";

    for (const auto& pair : locks_) {
        const auto& lock = pair.second;
        info += "Resource: " + pair.first + "\n";
        info += "  Type: " + std::string(lock->getLockType() == LockType::SHARED ? "SHARED" : "EXCLUSIVE") + "\n";
        info += "  Holders: " + std::to_string(lock->getHolderCount()) + "\n";
        info += "  Waiters: " + std::to_string(lock->getWaiterCount()) + "\n";
    }

    info += "\nTransaction Locks:\n";
    for (const auto& pair : txn_locks_) {
        info += "Txn " + std::to_string(pair.first) + ": ";
        for (const auto& res : pair.second) {
            info += res + " ";
        }
        info += "\n";
    }

    return info;
}

bool LockManager::isTableLockedBy(TransactionId txn_id, const std::string& table_name, LockType* out_type) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::string resource_name = "table:" + table_name;

    auto it = locks_.find(resource_name);
    if (it == locks_.end()) {
        return false;
    }

    if (!it->second->isHeldBy(txn_id)) {
        return false;
    }

    if (out_type) {
        *out_type = it->second->getLockType();
    }

    return true;
}

Lock* LockManager::getOrCreateLock(const std::string& resource_name) {
    auto it = locks_.find(resource_name);
    if (it != locks_.end()) {
        return it->second.get();
    }

    // 创建新锁
    auto new_lock = std::make_unique<Lock>(resource_name);
    Lock* lock_ptr = new_lock.get();
    locks_[resource_name] = std::move(new_lock);
    return lock_ptr;
}

void LockManager::cleanupEmptyLocks() {
    for (auto it = locks_.begin(); it != locks_.end();) {
        if (it->second->getHolderCount() == 0 && it->second->getWaiterCount() == 0) {
            it = locks_.erase(it);
        } else {
            ++it;
        }
    }
}

// TableLockGuard 实现
TableLockGuard::TableLockGuard(LockManager* lock_mgr, Transaction* txn,
                               const std::string& table_name, LockType lock_type)
    : lock_mgr_(lock_mgr)
    , txn_(txn)
    , table_name_(table_name)
    , locked_(false) {
    if (lock_mgr_ && txn_) {
        locked_ = lock_mgr_->lockTable(txn_, table_name_, lock_type);
    }
}

TableLockGuard::~TableLockGuard() {
    unlock();
}

TableLockGuard::TableLockGuard(TableLockGuard&& other) noexcept
    : lock_mgr_(other.lock_mgr_)
    , txn_(other.txn_)
    , table_name_(std::move(other.table_name_))
    , locked_(other.locked_) {
    other.lock_mgr_ = nullptr;
    other.txn_ = nullptr;
    other.locked_ = false;
}

TableLockGuard& TableLockGuard::operator=(TableLockGuard&& other) noexcept {
    if (this != &other) {
        unlock();
        lock_mgr_ = other.lock_mgr_;
        txn_ = other.txn_;
        table_name_ = std::move(other.table_name_);
        locked_ = other.locked_;
        other.lock_mgr_ = nullptr;
        other.txn_ = nullptr;
        other.locked_ = false;
    }
    return *this;
}

void TableLockGuard::unlock() {
    if (locked_ && lock_mgr_ && txn_) {
        lock_mgr_->unlockTable(txn_, table_name_);
        locked_ = false;
    }
}

} // namespace storage
} // namespace tinydb
