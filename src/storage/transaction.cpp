#include "transaction.h"
#include "common/logger.h"
#include <chrono>

namespace tinydb {
namespace storage {

// 全局事务管理器实例
TransactionManager* g_transaction_manager = nullptr;

// 事务实现
Transaction::Transaction(TransactionId id)
    : txn_id_(id)
    , state_(TransactionState::ACTIVE)
    , start_time_(std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now().time_since_epoch()).count()) {
    LOG_INFO("Transaction started: " << txn_id_);
}

Transaction::~Transaction() {
    // 如果事务仍然是活跃状态，需要回滚
    if (state_ == TransactionState::ACTIVE) {
        LOG_WARN("Transaction " << txn_id_ << " destroyed while still active");
    }
}

void Transaction::addModifiedPage(uint32_t page_id) {
    // 检查是否已存在
    for (auto id : modified_pages_) {
        if (id == page_id) return;
    }
    modified_pages_.push_back(page_id);
}

void Transaction::addInsertRecord(const std::string& table_name, const TID& tid) {
    insert_records_.push_back({table_name, tid});
}

void Transaction::addDeleteRecord(const std::string& table_name, const Tuple& old_tuple, const TID& tid) {
    delete_records_.push_back({table_name, old_tuple, tid});
}

// 事务管理器实现
TransactionManager::TransactionManager() {
    LOG_INFO("TransactionManager initialized");
}

TransactionManager::~TransactionManager() {
    // 清理所有未完成的事务
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& pair : transactions_) {
        auto& txn = pair.second;
        if (txn->getState() == TransactionState::ACTIVE) {
            LOG_WARN("Aborting active transaction " << txn->getId() << " during shutdown");
            doAbort(txn.get());
        }
    }
    transactions_.clear();
}

Transaction* TransactionManager::beginTransaction() {
    std::lock_guard<std::mutex> lock(mutex_);

    TransactionId id = next_txn_id_++;
    auto txn = std::make_unique<Transaction>(id);
    Transaction* txn_ptr = txn.get();

    transactions_[id] = std::move(txn);

    // 设置为当前事务
    current_txn_ = txn_ptr;

    LOG_INFO("Begin transaction: " << id);
    return txn_ptr;
}

bool TransactionManager::commitTransaction(Transaction* txn) {
    if (!txn) {
        LOG_ERROR("Cannot commit null transaction");
        return false;
    }

    if (txn->getState() != TransactionState::ACTIVE) {
        LOG_ERROR("Cannot commit transaction " << txn->getId() << " in state "
                 << static_cast<int>(txn->getState()));
        return false;
    }

    LOG_INFO("Committing transaction: " << txn->getId());

    bool result = doCommit(txn);

    if (result) {
        txn->setState(TransactionState::COMMITTED);
        LOG_INFO("Transaction committed: " << txn->getId());
    } else {
        LOG_ERROR("Failed to commit transaction " << txn->getId() << ", aborting");
        doAbort(txn);
    }

    // 清除当前事务
    if (current_txn_ == txn) {
        current_txn_ = nullptr;
    }

    return result;
}

bool TransactionManager::abortTransaction(Transaction* txn) {
    if (!txn) {
        LOG_ERROR("Cannot abort null transaction");
        return false;
    }

    if (txn->getState() != TransactionState::ACTIVE) {
        LOG_ERROR("Cannot abort transaction " << txn->getId() << " in state "
                 << static_cast<int>(txn->getState()));
        return false;
    }

    LOG_INFO("Aborting transaction: " << txn->getId());

    bool result = doAbort(txn);

    txn->setState(TransactionState::ABORTED);
    LOG_INFO("Transaction aborted: " << txn->getId());

    // 清除当前事务
    if (current_txn_ == txn) {
        current_txn_ = nullptr;
    }

    return result;
}

Transaction* TransactionManager::getTransaction(TransactionId txn_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = transactions_.find(txn_id);
    if (it != transactions_.end()) {
        return it->second.get();
    }
    return nullptr;
}

void TransactionManager::cleanupCompletedTransactions() {
    std::lock_guard<std::mutex> lock(mutex_);

    for (auto it = transactions_.begin(); it != transactions_.end();) {
        auto state = it->second->getState();
        if (state == TransactionState::COMMITTED || state == TransactionState::ABORTED) {
            it = transactions_.erase(it);
        } else {
            ++it;
        }
    }
}

bool TransactionManager::hasActiveTransactions() const {
    std::lock_guard<std::mutex> lock(mutex_);

    for (const auto& pair : transactions_) {
        if (pair.second->getState() == TransactionState::ACTIVE) {
            return true;
        }
    }
    return false;
}

size_t TransactionManager::getActiveTransactionCount() const {
    std::lock_guard<std::mutex> lock(mutex_);

    size_t count = 0;
    for (const auto& pair : transactions_) {
        if (pair.second->getState() == TransactionState::ACTIVE) {
            count++;
        }
    }
    return count;
}

bool TransactionManager::doCommit(Transaction* txn) {
    // 设置事务状态为提交中
    txn->setState(TransactionState::COMMITTING);

    // 1. 写入 COMMIT 日志记录（WAL）
    // 注意：实际WAL写入在存储引擎层完成，这里只是状态管理

    // 2. 刷盘所有修改过的页
    // 注意：实际刷盘在存储引擎层完成

    // 3. 清除事务的修改记录
    txn->clearRecords();
    txn->clearModifiedPages();

    return true;
}

bool TransactionManager::doAbort(Transaction* txn) {
    // 设置事务状态为回滚中
    txn->setState(TransactionState::ABORTING);

    // 1. 写入 ROLLBACK 日志记录

    // 2. 回滚所有修改（通过存储引擎）
    // 注意：实际回滚在存储引擎层完成

    // 3. 清除事务的修改记录
    txn->clearRecords();
    txn->clearModifiedPages();

    return true;
}

} // namespace storage
} // namespace tinydb
