#include <gtest/gtest.h>
#include "storage/transaction.h"
#include "storage/wal.h"
#include "storage/tuple.h"

using namespace tinydb;
using namespace tinydb::storage;

class TransactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 每个测试前重置事务管理器
        txn_manager_ = std::make_unique<TransactionManager>();
    }

    void TearDown() override {
        txn_manager_.reset();
    }

    std::unique_ptr<TransactionManager> txn_manager_;
};

// 测试事务创建和基本属性
TEST_F(TransactionTest, CreateTransaction) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 验证事务ID有效
    EXPECT_GT(txn->getId(), 0);

    // 验证初始状态为 ACTIVE
    EXPECT_EQ(txn->getState(), TransactionState::ACTIVE);

    // 验证开始时间已设置
    EXPECT_GT(txn->getStartTime(), 0);
}

// 测试事务提交
TEST_F(TransactionTest, CommitTransaction) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 提交事务
    EXPECT_TRUE(txn_manager_->commitTransaction(txn));

    // 验证事务状态变为 COMMITTED
    EXPECT_EQ(txn->getState(), TransactionState::COMMITTED);

    // 清理已完成的事务
    txn_manager_->cleanupCompletedTransactions();

    // 现在应该无法获取该事务
    EXPECT_EQ(txn_manager_->getTransaction(txn->getId()), nullptr);
}

// 测试事务回滚
TEST_F(TransactionTest, AbortTransaction) {
    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 回滚事务
    EXPECT_TRUE(txn_manager_->abortTransaction(txn));

    // 验证事务状态变为 ABORTED
    EXPECT_EQ(txn->getState(), TransactionState::ABORTED);

    // 清理已完成的事务
    txn_manager_->cleanupCompletedTransactions();

    // 现在应该无法获取该事务
    EXPECT_EQ(txn_manager_->getTransaction(txn->getId()), nullptr);
}

// 测试获取当前活跃事务
TEST_F(TransactionTest, GetCurrentTransaction) {
    EXPECT_EQ(txn_manager_->getCurrentTransaction(), nullptr);

    Transaction* txn = txn_manager_->beginTransaction();
    ASSERT_NE(txn, nullptr);

    // 验证当前事务被正确设置
    EXPECT_EQ(txn_manager_->getCurrentTransaction(), txn);

    // 提交后当前事务应被清空
    txn_manager_->commitTransaction(txn);
    EXPECT_EQ(txn_manager_->getCurrentTransaction(), nullptr);
}

// 测试记录修改过的页
TEST_F(TransactionTest, ModifiedPages) {
    Transaction txn(1);

    // 初始为空
    EXPECT_TRUE(txn.getModifiedPages().empty());

    // 添加修改页
    txn.addModifiedPage(10);
    txn.addModifiedPage(20);
    txn.addModifiedPage(30);

    // 验证修改页记录
    const auto& pages = txn.getModifiedPages();
    EXPECT_EQ(pages.size(), 3);
    EXPECT_EQ(pages[0], 10);
    EXPECT_EQ(pages[1], 20);
    EXPECT_EQ(pages[2], 30);

    // 清空记录
    txn.clearModifiedPages();
    EXPECT_TRUE(txn.getModifiedPages().empty());
}

// 测试插入记录追踪
TEST_F(TransactionTest, InsertRecords) {
    Transaction txn(1);

    // 初始为空
    EXPECT_TRUE(txn.getInsertRecords().empty());

    // 添加插入记录 - 使用正确的 TID 构造函数
    TID tid1(1, 0);
    TID tid2(2, 1);
    txn.addInsertRecord("users", tid1);
    txn.addInsertRecord("orders", tid2);

    // 验证记录
    const auto& records = txn.getInsertRecords();
    EXPECT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].table_name, "users");
    EXPECT_EQ(records[0].tid.page_id, 1);
    EXPECT_EQ(records[0].tid.slot_id, 0);
    EXPECT_EQ(records[1].table_name, "orders");
    EXPECT_EQ(records[1].tid.page_id, 2);
    EXPECT_EQ(records[1].tid.slot_id, 1);

    // 清空记录
    txn.clearRecords();
    EXPECT_TRUE(txn.getInsertRecords().empty());
}

// 测试删除记录追踪
TEST_F(TransactionTest, DeleteRecords) {
    Transaction txn(1);
    Schema schema;
    schema.addColumn("id", DataType::INT32);
    schema.addColumn("name", DataType::VARCHAR, 32);

    // 初始为空
    EXPECT_TRUE(txn.getDeleteRecords().empty());

    // 添加删除记录
    Tuple tuple(&schema);
    tuple.addField(Field(static_cast<int32_t>(1)));
    tuple.addField(Field("Alice", DataType::VARCHAR));

    TID tid(1, 0);
    txn.addDeleteRecord("users", tuple, tid);

    // 验证记录
    const auto& records = txn.getDeleteRecords();
    EXPECT_EQ(records.size(), 1);
    EXPECT_EQ(records[0].table_name, "users");
    EXPECT_EQ(records[0].tid.page_id, 1);
    EXPECT_EQ(records[0].tid.slot_id, 0);

    // 清空记录
    txn.clearRecords();
    EXPECT_TRUE(txn.getDeleteRecords().empty());
}

// 测试多事务管理
TEST_F(TransactionTest, MultipleTransactions) {
    Transaction* txn1 = txn_manager_->beginTransaction();
    Transaction* txn2 = txn_manager_->beginTransaction();
    Transaction* txn3 = txn_manager_->beginTransaction();

    ASSERT_NE(txn1, nullptr);
    ASSERT_NE(txn2, nullptr);
    ASSERT_NE(txn3, nullptr);

    // 验证事务ID递增
    EXPECT_LT(txn1->getId(), txn2->getId());
    EXPECT_LT(txn2->getId(), txn3->getId());

    // 验证活跃事务数量（所有3个事务都是活跃的）
    EXPECT_EQ(txn_manager_->getActiveTransactionCount(), 3);

    // 提交事务2
    txn_manager_->commitTransaction(txn2);

    // 验证事务2状态
    EXPECT_EQ(txn2->getState(), TransactionState::COMMITTED);

    // 清理后验证
    txn_manager_->cleanupCompletedTransactions();
    EXPECT_EQ(txn_manager_->getTransaction(txn2->getId()), nullptr);  // 已清理
    EXPECT_EQ(txn_manager_->getTransaction(txn1->getId()), txn1);
    EXPECT_EQ(txn_manager_->getTransaction(txn3->getId()), txn3);

    // 现在活跃事务数量应该是2
    EXPECT_EQ(txn_manager_->getActiveTransactionCount(), 2);
}

// 测试事务ID生成
TEST_F(TransactionTest, TransactionIdGeneration) {
    TransactionId id1 = txn_manager_->getNextTransactionId();
    TransactionId id2 = txn_manager_->getNextTransactionId();
    TransactionId id3 = txn_manager_->getNextTransactionId();

    // 验证ID递增
    EXPECT_LT(id1, id2);
    EXPECT_LT(id2, id3);

    // 验证ID从1开始
    EXPECT_EQ(id1, 1);
}

// 测试无效事务操作
TEST_F(TransactionTest, InvalidTransactionOperations) {
    // 提交空事务应该失败
    EXPECT_FALSE(txn_manager_->commitTransaction(nullptr));

    // 回滚空事务应该失败
    EXPECT_FALSE(txn_manager_->abortTransaction(nullptr));

    // 获取不存在的事务
    EXPECT_EQ(txn_manager_->getTransaction(999), nullptr);
}

// 测试是否有活跃事务
TEST_F(TransactionTest, HasActiveTransactions) {
    EXPECT_FALSE(txn_manager_->hasActiveTransactions());

    Transaction* txn = txn_manager_->beginTransaction();
    EXPECT_TRUE(txn_manager_->hasActiveTransactions());

    txn_manager_->commitTransaction(txn);
    EXPECT_FALSE(txn_manager_->hasActiveTransactions());
}
