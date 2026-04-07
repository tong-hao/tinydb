#include <gtest/gtest.h>
#include "storage/storage_engine.h"
#include "storage/index_manager.h"
#include <filesystem>
#include <memory>

using namespace tinydb::storage;

class IndexDDLTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/tinydb_index_ddl_test_" + std::to_string(getpid());
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

// 测试创建索引
TEST_F(IndexDDLTest, CreateIndex) {
    // 创建索引
    EXPECT_TRUE(storage_->createIndex("idx_users_id", "users", "id", false));

    // 验证索引存在
    auto index = storage_->getIndex("idx_users_id");
    ASSERT_NE(index, nullptr);
    EXPECT_EQ(index->getMeta().table_name, "users");
    EXPECT_EQ(index->getMeta().column_name, "id");
    EXPECT_FALSE(index->getMeta().is_unique);
}

// 测试创建唯一索引
TEST_F(IndexDDLTest, CreateUniqueIndex) {
    EXPECT_TRUE(storage_->createIndex("idx_users_id_unique", "users", "id", true));

    auto index = storage_->getIndex("idx_users_id_unique");
    ASSERT_NE(index, nullptr);
    EXPECT_TRUE(index->getMeta().is_unique);
}

// 测试创建索引后数据自动建立
TEST_F(IndexDDLTest, CreateIndexWithDataBuild) {
    // 创建索引前插入数据
    EXPECT_TRUE(storage_->createIndex("idx_users_id", "users", "id", false));

    // 验证索引可以使用
    auto tids = storage_->indexLookup("users", "id", IndexKey(42));
    EXPECT_EQ(tids.size(), 1);
}

// 测试索引查找
TEST_F(IndexDDLTest, IndexLookup) {
    EXPECT_TRUE(storage_->createIndex("idx_users_id", "users", "id", false));

    // 查找特定值
    auto tids = storage_->indexLookup("users", "id", IndexKey(50));
    ASSERT_EQ(tids.size(), 1);

    // 获取数据验证
    auto tuple = storage_->get("users", tids[0]);
    EXPECT_EQ(tuple.getField(0).getInt32(), 50);
}

// 测试索引范围查询
TEST_F(IndexDDLTest, IndexRangeQuery) {
    EXPECT_TRUE(storage_->createIndex("idx_users_id", "users", "id", false));

    // 范围查询
    auto tids = storage_->indexRangeQuery("users", "id", IndexKey(10), IndexKey(20));
    EXPECT_EQ(tids.size(), 11);  // 10-20共11个
}

// 测试删除索引
TEST_F(IndexDDLTest, DropIndex) {
    // 创建索引
    EXPECT_TRUE(storage_->createIndex("idx_users_id", "users", "id", false));
    EXPECT_NE(storage_->getIndex("idx_users_id"), nullptr);

    // 删除索引
    EXPECT_TRUE(storage_->dropIndex("idx_users_id"));
    EXPECT_EQ(storage_->getIndex("idx_users_id"), nullptr);
}

// 测试在不存在表上创建索引
TEST_F(IndexDDLTest, CreateIndexOnNonExistentTable) {
    EXPECT_FALSE(storage_->createIndex("idx_test", "non_existent_table", "id", false));
}

// 测试在不存在列上创建索引
TEST_F(IndexDDLTest, CreateIndexOnNonExistentColumn) {
    // 尝试在不存在的列上创建索引，应该失败
    EXPECT_FALSE(storage_->createIndex("idx_test", "users", "non_existent_column", false));

    // 验证索引确实没有被创建
    EXPECT_EQ(storage_->getIndex("idx_test"), nullptr);
}

// 测试多列索引（目前不支持，应创建多个单列索引）
TEST_F(IndexDDLTest, MultipleIndexesOnSameTable) {
    EXPECT_TRUE(storage_->createIndex("idx_users_id", "users", "id", false));
    EXPECT_TRUE(storage_->createIndex("idx_users_age", "users", "age", false));

    auto index1 = storage_->getIndex("idx_users_id");
    auto index2 = storage_->getIndex("idx_users_age");

    EXPECT_NE(index1, nullptr);
    EXPECT_NE(index2, nullptr);
}

// 测试重复创建同名索引
TEST_F(IndexDDLTest, CreateDuplicateIndex) {
    EXPECT_TRUE(storage_->createIndex("idx_test", "users", "id", false));
    // 第二次创建应该失败
    EXPECT_FALSE(storage_->createIndex("idx_test", "users", "id", false));
}

// 主函数
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
