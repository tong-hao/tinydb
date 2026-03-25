#include <gtest/gtest.h>
#include "storage/storage_engine.h"
#include "sql/executor/executor.h"
#include <filesystem>
#include <memory>

using namespace tinydb;
using namespace tinydb::storage;
using namespace tinydb::engine;

class JoinTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/tinydb_join_test_" + std::to_string(getpid());
        std::filesystem::create_directories(test_dir_);

        StorageConfig config;
        config.db_file_path = test_dir_ + "/test.db";
        config.wal_file_path = test_dir_ + "/test.wal";

        storage_ = std::make_unique<StorageEngine>(config);
        ASSERT_TRUE(storage_->initialize());

        // 创建用户表
        Schema user_schema;
        user_schema.addColumn("user_id", DataType::INT32, sizeof(int32_t));
        user_schema.addColumn("name", DataType::VARCHAR, 64);
        user_schema.addColumn("age", DataType::INT32, sizeof(int32_t));

        ASSERT_TRUE(storage_->createTable("users", user_schema));

        // 创建订单表
        Schema order_schema;
        order_schema.addColumn("order_id", DataType::INT32, sizeof(int32_t));
        order_schema.addColumn("user_id", DataType::INT32, sizeof(int32_t));
        order_schema.addColumn("amount", DataType::INT32, sizeof(int32_t));

        ASSERT_TRUE(storage_->createTable("orders", order_schema));

        // 插入用户数据
        auto users_table = storage_->getTable("users");
        for (int i = 1; i <= 10; ++i) {
            Tuple tuple(&users_table->schema);
            tuple.addField(Field(static_cast<int32_t>(i)));
            tuple.addField(Field("user_" + std::to_string(i), DataType::VARCHAR));
            tuple.addField(Field(static_cast<int32_t>(20 + i)));

            auto tid = storage_->insert("users", tuple);
            EXPECT_TRUE(tid.isValid());
        }

        // 插入订单数据（每个用户有2-3个订单）
        auto orders_table = storage_->getTable("orders");
        int order_id = 1;
        for (int user_id = 1; user_id <= 10; ++user_id) {
            int num_orders = 2 + (user_id % 2);  // 2或3个订单
            for (int j = 0; j < num_orders; ++j) {
                Tuple tuple(&orders_table->schema);
                tuple.addField(Field(static_cast<int32_t>(order_id)));
                tuple.addField(Field(static_cast<int32_t>(user_id)));
                tuple.addField(Field(static_cast<int32_t>(100 * order_id)));

                auto tid = storage_->insert("orders", tuple);
                EXPECT_TRUE(tid.isValid());
                order_id++;
            }
        }

        // 创建执行器
        executor_ = std::make_unique<Executor>();
        executor_->initialize(storage_.get());
    }

    void TearDown() override {
        executor_.reset();
        storage_->shutdown();
        storage_.reset();

        std::filesystem::remove_all(test_dir_);
    }

    std::string test_dir_;
    std::unique_ptr<StorageEngine> storage_;
    std::unique_ptr<Executor> executor_;
};

// 测试Nested Loop Join
TEST_F(JoinTest, NestedLoopJoin) {
    // 构建JOIN查询
    sql::SelectStmt stmt;
    stmt.addSelectColumn("users.name");
    stmt.addSelectColumn("orders.amount");
    stmt.setFromTable("users");

    // 添加JOIN表
    auto join_table = std::make_unique<sql::JoinTable>("orders", sql::JoinType::INNER);

    // 设置JOIN条件: users.user_id = orders.user_id
    auto left = std::make_unique<sql::ColumnRefExpr>("users.user_id");
    auto right = std::make_unique<sql::ColumnRefExpr>("orders.user_id");
    auto join_condition = std::make_unique<sql::ComparisonExpr>(sql::OpType::EQ,
        std::move(left), std::move(right));
    join_table->setJoinCondition(std::move(join_condition));

    stmt.addJoinTable(std::move(join_table));

    // 执行查询
    sql::AST ast;
    ast.setStatement(std::make_unique<sql::SelectStmt>(std::move(stmt)));
    auto result = executor_->execute(ast);

    EXPECT_TRUE(result.success());

    // 验证结果
    std::string result_str = result.message();
    EXPECT_NE(result_str.find("JOIN Result"), std::string::npos);
}

// 测试JOIN的WHERE条件过滤
TEST_F(JoinTest, JoinWithWhereCondition) {
    sql::SelectStmt stmt;
    stmt.addSelectColumn("users.name");
    stmt.addSelectColumn("orders.amount");
    stmt.setFromTable("users");

    // 添加JOIN表
    auto join_table = std::make_unique<sql::JoinTable>("orders", sql::JoinType::INNER);
    stmt.addJoinTable(std::move(join_table));

    // 设置WHERE条件: users.user_id = 5
    auto left = std::make_unique<sql::ColumnRefExpr>("user_id");
    auto right = std::make_unique<sql::LiteralExpr>("5");
    auto where_condition = std::make_unique<sql::ComparisonExpr>(sql::OpType::EQ,
        std::move(left), std::move(right));
    stmt.setWhereCondition(std::move(where_condition));

    sql::AST ast;
    ast.setStatement(std::make_unique<sql::SelectStmt>(std::move(stmt)));
    auto result = executor_->execute(ast);

    EXPECT_TRUE(result.success());
}

// 测试JOIN执行器方法
TEST_F(JoinTest, ExecuteJoinMethod) {
    sql::SelectStmt stmt;
    stmt.addSelectColumn("*");
    stmt.setFromTable("users");

    // 添加JOIN表
    auto join_table = std::make_unique<sql::JoinTable>("orders", sql::JoinType::INNER);
    stmt.addJoinTable(std::move(join_table));

    sql::AST ast;
    ast.setStatement(std::make_unique<sql::SelectStmt>(std::move(stmt)));
    auto result = executor_->execute(ast);

    EXPECT_TRUE(result.success());
}

// 测试多表JOIN（三个表）
TEST_F(JoinTest, MultiTableJoin) {
    // 创建第三个表
    Schema product_schema;
    product_schema.addColumn("product_id", DataType::INT32, sizeof(int32_t));
    product_schema.addColumn("name", DataType::VARCHAR, 64);
    ASSERT_TRUE(storage_->createTable("products", product_schema));

    // 插入产品数据
    auto product_table = storage_->getTable("products");
    for (int i = 1; i <= 5; ++i) {
        Tuple tuple(&product_table->schema);
        tuple.addField(Field(static_cast<int32_t>(i)));
        tuple.addField(Field("product_" + std::to_string(i), DataType::VARCHAR));
        auto tid = storage_->insert("products", tuple);
        EXPECT_TRUE(tid.isValid());
    }

    // 构建三表JOIN查询
    sql::SelectStmt stmt;
    stmt.addSelectColumn("*");
    stmt.setFromTable("users");

    auto join1 = std::make_unique<sql::JoinTable>("orders", sql::JoinType::INNER);
    stmt.addJoinTable(std::move(join1));

    auto join2 = std::make_unique<sql::JoinTable>("products", sql::JoinType::INNER);
    stmt.addJoinTable(std::move(join2));

    sql::AST ast;
    ast.setStatement(std::make_unique<sql::SelectStmt>(std::move(stmt)));
    auto result = executor_->execute(ast);

    EXPECT_TRUE(result.success());
}

// 测试JOIN WHERE条件评估
TEST_F(JoinTest, JoinWhereConditionEvaluation) {
    // 构建带有复杂WHERE条件的JOIN
    sql::SelectStmt stmt;
    stmt.addSelectColumn("*");
    stmt.setFromTable("users");

    auto join_table = std::make_unique<sql::JoinTable>("orders", sql::JoinType::INNER);
    stmt.addJoinTable(std::move(join_table));

    // 设置复杂WHERE条件: users.age > 25 AND orders.amount > 200
    auto age_left = std::make_unique<sql::ColumnRefExpr>("age");
    auto age_right = std::make_unique<sql::LiteralExpr>("25");
    auto age_cond = std::make_unique<sql::ComparisonExpr>(sql::OpType::GT,
        std::move(age_left), std::move(age_right));

    auto amt_left = std::make_unique<sql::ColumnRefExpr>("amount");
    auto amt_right = std::make_unique<sql::LiteralExpr>("200");
    auto amt_cond = std::make_unique<sql::ComparisonExpr>(sql::OpType::GT,
        std::move(amt_left), std::move(amt_right));

    auto combined = std::make_unique<sql::LogicalExpr>(sql::OpType::AND,
        std::move(age_cond), std::move(amt_cond));
    stmt.setWhereCondition(std::move(combined));

    sql::AST ast;
    ast.setStatement(std::make_unique<sql::SelectStmt>(std::move(stmt)));
    auto result = executor_->execute(ast);

    EXPECT_TRUE(result.success());
}

// 主函数
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
