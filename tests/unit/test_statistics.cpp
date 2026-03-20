#include <gtest/gtest.h>
#include "storage/storage_engine.h"
#include "storage/statistics.h"
#include <filesystem>
#include <memory>

using namespace tinydb::storage;

class StatisticsTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/tinydb_stats_test_" + std::to_string(getpid());
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

        // 插入测试数据
        auto table = storage_->getTable("users");
        for (int i = 0; i < 100; ++i) {
            Tuple tuple(&table->schema);
            tuple.addField(Field(static_cast<int32_t>(i)));
            tuple.addField(Field("user_" + std::to_string(i), DataType::VARCHAR));
            tuple.addField(Field(static_cast<int32_t>(20 + i % 50)));

            auto tid = storage_->insert("users", tuple);
            EXPECT_TRUE(tid.isValid());
        }
    }

    void TearDown() override {
        storage_->shutdown();
        storage_.reset();

        std::filesystem::remove_all(test_dir_);
    }

    std::string test_dir_;
    std::unique_ptr<StorageEngine> storage_;
};

// 测试表统计收集
TEST_F(StatisticsTest, AnalyzeTable) {
    // 收集统计
    EXPECT_TRUE(storage_->analyzeTable("users"));

    // 获取统计
    auto stats_opt = storage_->getStatisticsManager()->getTableStats("users");
    ASSERT_TRUE(stats_opt.has_value());

    const auto& stats = stats_opt.value();
    EXPECT_EQ(stats.table_name, "users");
    EXPECT_EQ(stats.row_count, 100);
    EXPECT_GT(stats.page_count, 0);
    EXPECT_GT(stats.avg_row_length, 0);
}

// 测试列统计
TEST_F(StatisticsTest, ColumnStatistics) {
    EXPECT_TRUE(storage_->analyzeTable("users"));

    // id列统计
    auto col_stats_opt = storage_->getStatisticsManager()->getColumnStats("users", "id");
    ASSERT_TRUE(col_stats_opt.has_value());

    const auto& col_stats = col_stats_opt.value();
    EXPECT_EQ(col_stats.column_name, "id");
    EXPECT_EQ(col_stats.data_type, DataType::INT32);
    EXPECT_EQ(col_stats.total_count, 100);
    EXPECT_EQ(col_stats.null_count, 0);
    EXPECT_EQ(col_stats.distinct_count, 100);  // 所有id都不同

    // 验证min/max
    EXPECT_EQ(col_stats.min_value.getInt32(), 0);
    EXPECT_EQ(col_stats.max_value.getInt32(), 99);

    // age列统计（有重复值）
    col_stats_opt = storage_->getStatisticsManager()->getColumnStats("users", "age");
    ASSERT_TRUE(col_stats_opt.has_value());

    const auto& age_stats = col_stats_opt.value();
    EXPECT_LT(age_stats.distinct_count, 100);  // age有重复
}

// 测试选择率估计
TEST_F(StatisticsTest, EstimateSelectivity) {
    EXPECT_TRUE(storage_->analyzeTable("users"));

    // 等值选择率
    Field value(static_cast<int32_t>(50));

    double sel = storage_->getStatisticsManager()->estimateSelectivity(
        "users", "id", "=", value);

    // id是唯一的，选择率应该约等于 1/100 = 0.01
    EXPECT_GT(sel, 0);
    EXPECT_LT(sel, 0.1);
}

// 测试对不存在的表的分析
TEST_F(StatisticsTest, AnalyzeNonExistentTable) {
    EXPECT_FALSE(storage_->analyzeTable("non_existent_table"));
}

// 测试重复分析（覆盖旧统计）
TEST_F(StatisticsTest, ReanalyzeTable) {
    // 第一次分析
    EXPECT_TRUE(storage_->analyzeTable("users"));
    auto stats1 = storage_->getStatisticsManager()->getTableStats("users");

    // 插入更多数据
    auto table = storage_->getTable("users");
    for (int i = 100; i < 200; ++i) {
        Tuple tuple(&table->schema);
        tuple.addField(Field(static_cast<int32_t>(i)));
        tuple.addField(Field("user_" + std::to_string(i), DataType::VARCHAR));
        tuple.addField(Field(static_cast<int32_t>(25)));

        auto tid = storage_->insert("users", tuple);
        EXPECT_TRUE(tid.isValid());
    }

    // 第二次分析
    EXPECT_TRUE(storage_->analyzeTable("users"));
    auto stats2 = storage_->getStatisticsManager()->getTableStats("users");

    // 行数应该增加
    EXPECT_GT(stats2->row_count, stats1->row_count);
}

// 测试ColumnStats序列化
TEST_F(StatisticsTest, ColumnStatsSerialization) {
    ColumnStats stats;
    stats.column_name = "test_col";
    stats.data_type = DataType::INT32;
    stats.total_count = 100;
    stats.null_count = 5;
    stats.distinct_count = 95;
    stats.min_value = Field(static_cast<int32_t>(1));
    stats.max_value = Field(static_cast<int32_t>(100));

    auto data = stats.serialize();

    ColumnStats stats2;
    EXPECT_TRUE(stats2.deserialize(data.data(), data.size()));

    EXPECT_EQ(stats2.column_name, stats.column_name);
    EXPECT_EQ(stats2.data_type, stats.data_type);
    EXPECT_EQ(stats2.total_count, stats.total_count);
    EXPECT_EQ(stats2.null_count, stats.null_count);
    EXPECT_EQ(stats2.distinct_count, stats.distinct_count);
    EXPECT_EQ(stats2.min_value.getInt32(), 1);
    EXPECT_EQ(stats2.max_value.getInt32(), 100);
}

// 测试TableStats序列化
TEST_F(StatisticsTest, TableStatsSerialization) {
    TableStats stats;
    stats.table_name = "test_table";
    stats.row_count = 1000;
    stats.page_count = 10;

    ColumnStats col_stats;
    col_stats.column_name = "id";
    col_stats.data_type = DataType::INT32;
    col_stats.total_count = 1000;
    stats.column_stats["id"] = col_stats;

    auto data = stats.serialize();

    TableStats stats2;
    EXPECT_TRUE(stats2.deserialize(data.data(), data.size()));

    EXPECT_EQ(stats2.table_name, stats.table_name);
    EXPECT_EQ(stats2.row_count, stats.row_count);
    EXPECT_EQ(stats2.page_count, stats.page_count);
    EXPECT_EQ(stats2.column_stats.size(), 1);
    EXPECT_EQ(stats2.column_stats["id"].column_name, "id");
}

// 主函数
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
