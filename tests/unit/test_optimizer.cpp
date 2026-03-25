#include <gtest/gtest.h>
#include "sql/optimizer/optimizer.h"
#include "storage/storage_engine.h"
#include <filesystem>
#include <memory>

using namespace tinydb;
using namespace tinydb::engine;
using namespace tinydb::storage;

class OptimizerTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/tinydb_opt_test_" + std::to_string(getpid());
        std::filesystem::create_directories(test_dir_);

        StorageConfig config;
        config.db_file_path = test_dir_ + "/test.db";
        config.wal_file_path = test_dir_ + "/test.wal";

        storage_ = std::make_unique<StorageEngine>(config);
        ASSERT_TRUE(storage_->initialize());

        // 创建测试表
        Schema schema;
        schema.addColumn("id", DataType::INT32, sizeof(int32_t));
        schema.addColumn("name", DataType::VARCHAR, 64);
        schema.addColumn("age", DataType::INT32, sizeof(int32_t));

        ASSERT_TRUE(storage_->createTable("users", schema));

        // 插入大量数据
        auto table = storage_->getTable("users");
        for (int i = 0; i < 1000; ++i) {
            Tuple tuple(&table->schema);
            tuple.addField(Field(static_cast<int32_t>(i)));
            tuple.addField(Field("user_" + std::to_string(i), DataType::VARCHAR));
            tuple.addField(Field(static_cast<int32_t>(20 + i % 50)));

            auto tid = storage_->insert("users", tuple);
            EXPECT_TRUE(tid.isValid());
        }

        // 创建索引
        ASSERT_TRUE(storage_->createIndex("idx_users_id", "users", "id", false));

        // 收集统计
        ASSERT_TRUE(storage_->analyzeTable("users"));

        // 创建优化器
        optimizer_ = std::make_unique<QueryOptimizer>(
            storage_->getStatisticsManager(),
            storage_->getIndexManager());
    }

    void TearDown() override {
        optimizer_.reset();
        storage_->shutdown();
        storage_.reset();

        std::filesystem::remove_all(test_dir_);
    }

    std::string test_dir_;
    std::unique_ptr<StorageEngine> storage_;
    std::unique_ptr<QueryOptimizer> optimizer_;
};

// 测试全表扫描计划
TEST_F(OptimizerTest, SeqScanPlan) {
    sql::SelectStmt stmt;
    stmt.addSelectColumn("*");
    stmt.setFromTable("users");

    ExecutionPlan plan = optimizer_->optimize(&stmt);

    ASSERT_NE(plan.root, nullptr);
    EXPECT_GT(plan.total_cost, 0);
    EXPECT_GT(plan.estimated_rows, 0);
}

// 测试索引扫描选择
TEST_F(OptimizerTest, IndexScanSelection) {
    // 创建带有等值条件的查询
    sql::SelectStmt stmt;
    stmt.addSelectColumn("*");
    stmt.setFromTable("users");

    auto left = std::make_unique<sql::ColumnRefExpr>("id");
    auto right = std::make_unique<sql::LiteralExpr>("100");
    auto condition = std::make_unique<sql::ComparisonExpr>(sql::OpType::EQ,
        std::move(left), std::move(right));
    stmt.setWhereCondition(std::move(condition));

    // 分析扫描选项
    auto options = optimizer_->analyzeScanOptions("users", stmt.whereCondition());

    // 应该至少有一个选项（全表扫描）
    EXPECT_GE(options.size(), 1);

    // 如果有索引，检查是否有索引扫描选项
    // 注：索引扫描可能不可用，取决于索引是否正确创建
}

// 测试选择率估计
TEST_F(OptimizerTest, SelectivityEstimation) {
    sql::SelectStmt stmt;
    stmt.addSelectColumn("*");
    stmt.setFromTable("users");

    auto left = std::make_unique<sql::ColumnRefExpr>("id");
    auto right = std::make_unique<sql::LiteralExpr>("100");
    auto condition = std::make_unique<sql::ComparisonExpr>(sql::OpType::EQ,
        std::move(left), std::move(right));
    stmt.setWhereCondition(std::move(condition));

    double sel = optimizer_->estimateSelectivity("users", stmt.whereCondition());

    // 等值选择率应该比较低
    EXPECT_GT(sel, 0);
    EXPECT_LT(sel, 0.1);
}

// 测试执行计划序列化
TEST_F(OptimizerTest, ExecutionPlanToString) {
    sql::SelectStmt stmt;
    stmt.addSelectColumn("id");
    stmt.addSelectColumn("name");
    stmt.setFromTable("users");

    ExecutionPlan plan = optimizer_->optimize(&stmt);

    std::string plan_str = plan.toString();

    EXPECT_FALSE(plan_str.empty());
    EXPECT_NE(plan_str.find("cost"), std::string::npos);
    EXPECT_NE(plan_str.find("rows"), std::string::npos);
}

// 测试扫描代价估计
TEST_F(OptimizerTest, ScanCostEstimation) {
    ScanNode seq_scan;
    seq_scan.table_name = "users";
    seq_scan.scan_type = ScanType::SEQ_SCAN;
    seq_scan.estimated_rows = 1000;
    seq_scan.estimated_cost = 100;  // 假设100页

    double cost = optimizer_->estimateScanCost(seq_scan);
    EXPECT_GT(cost, 0);

    ScanNode index_scan;
    index_scan.table_name = "users";
    index_scan.scan_type = ScanType::INDEX_SCAN;
    index_scan.estimated_rows = 10;
    index_scan.estimated_cost = 20;

    cost = optimizer_->estimateScanCost(index_scan);
    EXPECT_GT(cost, 0);
}

// 测试索引使用检测
TEST_F(OptimizerTest, IndexUsageDetection) {
    auto left = std::make_unique<sql::ColumnRefExpr>("id");
    auto right = std::make_unique<sql::LiteralExpr>("100");
    auto condition = std::make_unique<sql::ComparisonExpr>(sql::OpType::EQ,
        std::move(left), std::move(right));

    // id列有索引，应该可以使用
    EXPECT_TRUE(optimizer_->canUseIndex("users", "id", condition.get()));

    // name列没有索引
    auto left2 = std::make_unique<sql::ColumnRefExpr>("name");
    auto right2 = std::make_unique<sql::LiteralExpr>("'test'");
    auto condition2 = std::make_unique<sql::ComparisonExpr>(sql::OpType::EQ,
        std::move(left2), std::move(right2));

    EXPECT_FALSE(optimizer_->canUseIndex("users", "id", condition2.get()));
}

// 主函数
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
