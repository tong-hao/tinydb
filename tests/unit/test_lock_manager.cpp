#include <gtest/gtest.h>
#include "storage/lock_manager.h"
#include "storage/transaction.h"
#include <thread>
#include <chrono>

using namespace tinydb;
using namespace tinydb::storage;

class LockManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        lock_manager_ = std::make_unique<LockManager>();
        txn_manager_ = std::make_unique<TransactionManager>();
    }

    void TearDown() override {
        txn_manager_.reset();
        lock_manager_.reset();
    }

    std::unique_ptr<LockManager> lock_manager_;
    std::unique_ptr<TransactionManager> txn_manager_;
};

// 测试共享锁获取和释放
TEST_F(LockManagerTest, SharedLockAcquireAndRelease) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 获取共享锁
    EXPECT_TRUE(lock_manager_->lockTable(txn, "users", LockType::SHARED));

    // 验证锁被持有
    LockType lock_type;
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "users", &lock_type));
    EXPECT_EQ(lock_type, LockType::SHARED);

    // 释放锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn, "users"));

    // 验证锁已释放
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));

    txn_manager_->commitTransaction(txn);
}

// 测试排他锁获取和释放
TEST_F(LockManagerTest, ExclusiveLockAcquireAndRelease) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 获取排他锁
    EXPECT_TRUE(lock_manager_->lockTable(txn, "users", LockType::EXCLUSIVE));

    // 验证锁被持有
    LockType lock_type;
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "users", &lock_type));
    EXPECT_EQ(lock_type, LockType::EXCLUSIVE);

    // 释放锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn, "users"));

    // 验证锁已释放
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));

    txn_manager_->commitTransaction(txn);
}

// 测试多个事务的共享锁兼容性
TEST_F(LockManagerTest, MultipleSharedLocks) {
    Transaction* txn1 = txn_manager_->beginTransaction();
    Transaction* txn2 = txn_manager_->beginTransaction();
    ASSERT_NE(txn1, nullptr);
    ASSERT_NE(txn2, nullptr);

    // 事务1获取共享锁
    EXPECT_TRUE(lock_manager_->lockTable(txn1, "users", LockType::SHARED));

    // 事务2也可以获取共享锁（共享锁与共享锁兼容）
    EXPECT_TRUE(lock_manager_->lockTable(txn2, "users", LockType::SHARED));

    // 验证两个事务都持有锁
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn1->getId(), "users", nullptr));
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn2->getId(), "users", nullptr));

    // 释放锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn1, "users"));
    EXPECT_TRUE(lock_manager_->unlockTable(txn2, "users"));

    txn_manager_->commitTransaction(txn1);
    txn_manager_->commitTransaction(txn2);
}

// 测试排他锁与共享锁的不兼容性
TEST_F(LockManagerTest, ExclusiveLockBlocksShared) {
    Transaction* txn1 = txn_manager_->beginTransaction();
    Transaction* txn2 = txn_manager_->beginTransaction();
    ASSERT_NE(txn1, nullptr);
    ASSERT_NE(txn2, nullptr);

    // 事务1获取排他锁
    EXPECT_TRUE(lock_manager_->lockTable(txn1, "users", LockType::EXCLUSIVE));

    // 事务2尝试获取共享锁（应该超时）
    EXPECT_FALSE(lock_manager_->lockTable(txn2, "users", LockType::SHARED, 100));

    // 事务1释放锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn1, "users"));

    // 现在事务2可以获取锁
    EXPECT_TRUE(lock_manager_->lockTable(txn2, "users", LockType::SHARED));
    EXPECT_TRUE(lock_manager_->unlockTable(txn2, "users"));

    txn_manager_->commitTransaction(txn1);
    txn_manager_->commitTransaction(txn2);
}

// 测试共享锁与排他锁的不兼容性
TEST_F(LockManagerTest, SharedLockBlocksExclusive) {
    Transaction* txn1 = txn_manager_->beginTransaction();
    Transaction* txn2 = txn_manager_->beginTransaction();
    ASSERT_NE(txn1, nullptr);
    ASSERT_NE(txn2, nullptr);

    // 事务1获取共享锁
    EXPECT_TRUE(lock_manager_->lockTable(txn1, "users", LockType::SHARED));

    // 事务2尝试获取排他锁（应该超时）
    EXPECT_FALSE(lock_manager_->lockTable(txn2, "users", LockType::EXCLUSIVE, 100));

    // 事务1释放锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn1, "users"));

    // 现在事务2可以获取锁
    EXPECT_TRUE(lock_manager_->lockTable(txn2, "users", LockType::EXCLUSIVE));
    EXPECT_TRUE(lock_manager_->unlockTable(txn2, "users"));

    txn_manager_->commitTransaction(txn1);
    txn_manager_->commitTransaction(txn2);
}

// 测试同一事务多次获取锁（锁升级）
TEST_F(LockManagerTest, LockUpgrade) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 先获取共享锁
    EXPECT_TRUE(lock_manager_->lockTable(txn, "users", LockType::SHARED));

    // 同一事务升级到排他锁
    EXPECT_TRUE(lock_manager_->lockTable(txn, "users", LockType::EXCLUSIVE));

    // 验证锁类型为排他锁
    LockType lock_type;
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "users", &lock_type));
    EXPECT_EQ(lock_type, LockType::EXCLUSIVE);

    // 释放锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn, "users"));

    txn_manager_->commitTransaction(txn);
}

// 测试锁升级失败（有其他共享锁持有者）
TEST_F(LockManagerTest, LockUpgradeFailsWithMultipleHolders) {
    Transaction* txn1 = txn_manager_->beginTransaction();
    Transaction* txn2 = txn_manager_->beginTransaction();
    ASSERT_NE(txn1, nullptr);
    ASSERT_NE(txn2, nullptr);

    // 两个事务都获取共享锁
    EXPECT_TRUE(lock_manager_->lockTable(txn1, "users", LockType::SHARED));
    EXPECT_TRUE(lock_manager_->lockTable(txn2, "users", LockType::SHARED));

    // 事务1尝试升级到排他锁（应该失败，因为事务2还持有共享锁）
    EXPECT_FALSE(lock_manager_->lockTable(txn1, "users", LockType::EXCLUSIVE, 100));

    // 释放两个事务的锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn1, "users"));
    EXPECT_TRUE(lock_manager_->unlockTable(txn2, "users"));

    txn_manager_->commitTransaction(txn1);
    txn_manager_->commitTransaction(txn2);
}

// 测试释放所有锁
TEST_F(LockManagerTest, ReleaseAllLocks) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 获取多个锁
    EXPECT_TRUE(lock_manager_->lockTable(txn, "users", LockType::SHARED));
    EXPECT_TRUE(lock_manager_->lockTable(txn, "orders", LockType::EXCLUSIVE));
    EXPECT_TRUE(lock_manager_->lockTable(txn, "products", LockType::SHARED));

    // 验证都持有
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "orders", nullptr));
    EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "products", nullptr));

    // 释放所有锁
    lock_manager_->releaseAllLocks(txn);

    // 验证都释放了
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "orders", nullptr));
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "products", nullptr));

    txn_manager_->commitTransaction(txn);
}

// 测试解锁不存在的锁
TEST_F(LockManagerTest, UnlockNonExistentLock) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 尝试释放不存在的锁
    EXPECT_FALSE(lock_manager_->unlockTable(txn, "nonexistent"));

    txn_manager_->commitTransaction(txn);
}

// 测试空事务获取锁
TEST_F(LockManagerTest, LockWithNullTransaction) {
    EXPECT_FALSE(lock_manager_->lockTable(nullptr, "users", LockType::SHARED));
}

// 测试TableLockGuard RAII行为
TEST_F(LockManagerTest, TableLockGuardRAII) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    {
        // 使用RAII方式获取锁
        TableLockGuard guard(lock_manager_.get(), txn, "users", LockType::EXCLUSIVE);
        EXPECT_TRUE(guard.isLocked());

        // 验证锁被持有
        EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));
    }

    // guard销毁后锁应该被释放
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));

    txn_manager_->commitTransaction(txn);
}

// 测试TableLockGuard手动释放
TEST_F(LockManagerTest, TableLockGuardManualUnlock) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    TableLockGuard guard(lock_manager_.get(), txn, "users", LockType::EXCLUSIVE);
    EXPECT_TRUE(guard.isLocked());

    // 手动释放
    guard.unlock();
    EXPECT_FALSE(guard.isLocked());

    // 验证锁已释放
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));

    // 再次释放（应该安全）
    guard.unlock();

    txn_manager_->commitTransaction(txn);
}

// 测试TableLockGuard移动语义
TEST_F(LockManagerTest, TableLockGuardMove) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    {
        TableLockGuard guard1(lock_manager_.get(), txn, "users", LockType::EXCLUSIVE);
        EXPECT_TRUE(guard1.isLocked());

        // 移动到guard2
        TableLockGuard guard2(std::move(guard1));
        EXPECT_TRUE(guard2.isLocked());
        EXPECT_FALSE(guard1.isLocked());

        // 验证锁仍然被持有
        EXPECT_TRUE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));
    }

    // 验证锁已释放
    EXPECT_FALSE(lock_manager_->isTableLockedBy(txn->getId(), "users", nullptr));

    txn_manager_->commitTransaction(txn);
}

// 测试获取锁信息
TEST_F(LockManagerTest, GetLockInfo) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 获取锁
    EXPECT_TRUE(lock_manager_->lockTable(txn, "users", LockType::EXCLUSIVE));

    // 获取锁信息
    std::string info = lock_manager_->getLockInfo();
    EXPECT_NE(info.find("users"), std::string::npos);
    EXPECT_NE(info.find("EXCLUSIVE"), std::string::npos);

    // 释放锁
    EXPECT_TRUE(lock_manager_->unlockTable(txn, "users"));

    txn_manager_->commitTransaction(txn);
}

// 测试并发锁获取（多线程）
TEST_F(LockManagerTest, ConcurrentLockAcquisition) {
    const int num_threads = 5;
    std::vector<std::thread> threads;
    std::vector<Transaction*> transactions;
    std::atomic<int> success_count{0};

    // 创建事务
    for (int i = 0; i < num_threads; i++) {
        transactions.push_back(txn_manager_->beginTransaction());
    }

    // 每个线程尝试获取共享锁
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([this, i, &transactions, &success_count]() {
            if (lock_manager_->lockTable(transactions[i], "users", LockType::SHARED)) {
                success_count++;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                lock_manager_->unlockTable(transactions[i], "users");
            }
        });
    }

    // 等待所有线程完成
    for (auto& t : threads) {
        t.join();
    }

    // 所有线程都应该成功获取共享锁
    EXPECT_EQ(success_count, num_threads);

    // 提交所有事务
    for (auto* txn : transactions) {
        txn_manager_->commitTransaction(txn);
    }
}

// 测试死锁检测（简化实现）
TEST_F(LockManagerTest, DetectDeadlock) {
    // 简化实现只检查是否有等待者，实际死锁检测更复杂
    EXPECT_FALSE(lock_manager_->detectDeadlock());
}

// 测试Lock对象的基本功能
TEST_F(LockManagerTest, LockObjectBasics) {
    Lock lock("table:test");

    EXPECT_EQ(lock.getResourceName(), "table:test");
    EXPECT_EQ(lock.getHolderCount(), 0);
    EXPECT_EQ(lock.getWaiterCount(), 0);
    EXPECT_FALSE(lock.hasWaiters());

    // 事务1获取共享锁
    EXPECT_TRUE(lock.tryLock(1, LockType::SHARED));
    EXPECT_EQ(lock.getHolderCount(), 1);
    EXPECT_TRUE(lock.isHeldBy(1));

    // 事务2获取共享锁
    EXPECT_TRUE(lock.tryLock(2, LockType::SHARED));
    EXPECT_EQ(lock.getHolderCount(), 2);
    EXPECT_TRUE(lock.isHeldBy(2));

    // 释放事务1的锁
    lock.unlock(1);
    EXPECT_EQ(lock.getHolderCount(), 1);
    EXPECT_FALSE(lock.isHeldBy(1));
    EXPECT_TRUE(lock.isHeldBy(2));

    // 释放事务2的锁
    lock.unlock(2);
    EXPECT_EQ(lock.getHolderCount(), 0);
    EXPECT_FALSE(lock.isHeldBy(2));
}

// 测试重复加锁计数
TEST_F(LockManagerTest, LockReferenceCounting) {
    Lock lock("table:test");

    // 同一事务多次获取共享锁
    EXPECT_TRUE(lock.tryLock(1, LockType::SHARED));
    EXPECT_TRUE(lock.tryLock(1, LockType::SHARED));
    EXPECT_TRUE(lock.tryLock(1, LockType::SHARED));

    // 释放一次
    lock.unlock(1);
    EXPECT_TRUE(lock.isHeldBy(1));  // 仍然持有

    // 释放两次
    lock.unlock(1);
    EXPECT_TRUE(lock.isHeldBy(1));  // 仍然持有

    // 释放三次
    lock.unlock(1);
    EXPECT_FALSE(lock.isHeldBy(1));  // 完全释放
}

// 测试不兼容锁类型
TEST_F(LockManagerTest, LockIncompatibility) {
    Lock lock("table:test");

    // 事务1获取排他锁
    EXPECT_TRUE(lock.tryLock(1, LockType::EXCLUSIVE));

    // 事务2尝试获取共享锁（应该失败）
    EXPECT_FALSE(lock.tryLock(2, LockType::SHARED));

    // 事务2尝试获取排他锁（应该失败）
    EXPECT_FALSE(lock.tryLock(2, LockType::EXCLUSIVE));

    // 释放事务1的锁
    lock.unlock(1);

    // 现在事务2可以获取锁
    EXPECT_TRUE(lock.tryLock(2, LockType::SHARED));
    lock.unlock(2);
}
