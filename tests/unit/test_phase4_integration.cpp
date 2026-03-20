#include <gtest/gtest.h>
#include "storage/storage_engine.h"
#include "engine/executor.h"
#include <filesystem>
#include <memory>

using namespace tinydb;
using namespace tinydb::storage;
using namespace tinydb::engine;

// Phase 4 集成测试
class Phase4IntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/tinydb_phase4_test_" + std::to_string(getpid());
        std::filesystem::create_directories(test_dir_);

        StorageConfig config;
        config.db_file_path = test_dir_ + "/test.db";
        config.wal_file_path = test_dir_ + "/test.wal";

        storage_ = std::make_unique<StorageEngine>(config);
        ASSERT_TRUE(storage_->initialize());

        executor_ = std::make_unique<Executor>();
        executor_->initialize(storage_.get());

        // 创建测试表和数据
        setupTestData();
    }

    void TearDown() override {
        executor_.reset();
        storage_->shutdown();
        storage_.reset();

        std::filesystem::remove_all(test_dir_);
    }

    void setupTestData() {
        // 创建订单表
        Schema schema;
        schema.addColumn("id", DataType::INT32, sizeof(int32_t));
        schema.addColumn("user_id", DataType::INT32, sizeof(int32_t));
        schema.addColumn("product", DataType::VARCHAR, 64);
        schema.addColumn("amount", DataType::INT32, sizeof(int32_t));

        ASSERT_TRUE(storage_->createTable("orders", schema));

        // 插入测试数据
        auto table = storage_->getTable("orders");
        for (int i = 1; i <= 100; ++i) {
            Tuple tuple(&table->schema);
            tuple.addField(Field(static_cast<int32_t>(i)));
            tuple.addField(Field(static_cast<int32_t>(i % 10 + 1)));  // user_id 1-10
            tuple.addField(Field("product_" + std::to_string(i % 20), DataType::VARCHAR));
            tuple.addField(Field(static_cast<int32_t>(100 * i)));

            auto tid = storage_->insert("orders", tuple);
            ASSERT_TRUE(tid.isValid());
        }
    }

    std::string test_dir_;
    std::unique_ptr<StorageEngine> storage_;
    std::unique_ptr<Executor> executor_;
};

// 测试完整流程：创建表->插入数据->创建索引->分析->查询
TEST_F(Phase4IntegrationTest, FullWorkflow) {
    // 1. 创建索引
    auto result = executor_->execute("CREATE INDEX idx_orders_id ON orders(id)");
    EXPECT_TRUE(result.success()) << result.message();

    // 2. 分析表
    result = executor_->execute("ANALYZE orders");
    EXPECT_TRUE(result.success()) << result.message();

    // 3. 使用索引查询
    result = executor_->execute("SELECT * FROM orders WHERE id = 50");
    EXPECT_TRUE(result.success()) << result.message();
    EXPECT_NE(result.message().find("Index Scan"), std::string::npos);

    // 4. 全表扫描查询
    result = executor_->execute("SELECT * FROM orders");
    EXPECT_TRUE(result.success()) << result.message();
    EXPECT_NE(result.message().find("100"), std::string::npos);  // 100行
}

// 测试索引加速效果
TEST_F(Phase4IntegrationTest, IndexPerformance) {
    // 创建索引
    auto result = executor_->execute("CREATE INDEX idx_orders_user_id ON orders(user_id)");
    EXPECT_TRUE(result.success());

    // 使用索引的点查
    result = executor_->execute("SELECT * FROM orders WHERE user_id = 5");
    EXPECT_TRUE(result.success());
    // 应该返回约10行（100行/10个user）
    EXPECT_NE(result.message().find("10"), std::string::npos);
}

// 测试EXPLAIN功能
TEST_F(Phase4IntegrationTest, ExplainQuery) {
    // 创建索引
    executor_->execute("CREATE INDEX idx_orders_id ON orders(id)");

    // EXPLAIN查询
    auto result = executor_->execute("EXPLAIN SELECT * FROM orders WHERE id = 50");
    EXPECT_TRUE(result.success());

    // 验证执行计划包含关键信息
    std::string plan = result.message();
    EXPECT_NE(plan.find("Execution Plan"), std::string::npos);
    EXPECT_NE(plan.find("cost"), std::string::npos);
}

// 测试范围查询使用索引
TEST_F(Phase4IntegrationTest, RangeQueryWithIndex) {
    // 创建索引
    executor_->execute("CREATE INDEX idx_orders_id ON orders(id)");

    // 范围查询
    auto result = executor_->execute("SELECT * FROM orders WHERE id >= 10 AND id <= 20");
    EXPECT_TRUE(result.success());
}

// 测试删除索引
TEST_F(Phase4IntegrationTest, DropIndex) {
    // 创建并删除索引
    executor_->execute("CREATE INDEX idx_test ON orders(user_id)");

    auto result = executor_->execute("DROP INDEX idx_test");
    EXPECT_TRUE(result.success());

    // 验证索引已删除
    result = executor_->execute("DROP INDEX idx_test");
    EXPECT_FALSE(result.success());  // 再次删除应该失败
}

// 测试唯一索引约束
TEST_F(Phase4IntegrationTest, UniqueIndexConstraint) {
    // 创建唯一索引（id已经是唯一的，所以应该成功）
    auto result = executor_->execute("CREATE UNIQUE INDEX idx_orders_id_unique ON orders(id)");
    EXPECT_TRUE(result.success());
}

// 测试多列索引场景
TEST_F(Phase4IntegrationTest, MultipleIndexes) {
    // 创建多个索引
    executor_->execute("CREATE INDEX idx_orders_id ON orders(id)");
    executor_->execute("CREATE INDEX idx_orders_user_id ON orders(user_id)");
    executor_->execute("CREATE INDEX idx_orders_product ON orders(product)");

    // 使用不同索引查询
    auto result = executor_->execute("SELECT * FROM orders WHERE user_id = 3");
    EXPECT_TRUE(result.success());

    result = executor_->execute("SELECT * FROM orders WHERE product = 'product_5'");
    EXPECT_TRUE(result.success());
}

// 主函数
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
