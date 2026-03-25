#include <gtest/gtest.h>
#include "sql/parser/parser.h"
#include "sql/parser/ast.h"
#include "sql/executor/executor.h"
#include "storage/storage_engine.h"

using namespace tinydb;
using namespace tinydb::sql;
using namespace tinydb::engine;
using namespace tinydb::storage;

class UpdateDeleteTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 创建存储配置
        StorageConfig config;
        config.db_file_path = "test_db.db";
        config.wal_file_path = "test_db.wal";
        config.buffer_pool_size = 1024;

        storage_engine_ = std::make_unique<StorageEngine>(config);
        storage_engine_->initialize();

        executor_ = std::make_unique<Executor>();
        executor_->initialize(storage_engine_.get());

        // 创建测试表
        createTestTable();
        insertTestData();
    }

    void TearDown() override {
        executor_.reset();
        storage_engine_->shutdown();
        storage_engine_.reset();

        // 清理测试文件
        std::remove("test_db.db");
        std::remove("test_db.wal");
    }

    void createTestTable() {
        // 使用 DOUBLE 代替 DECIMAL
        std::string sql = "CREATE TABLE users (id INT, name VARCHAR(50), age INT, salary DOUBLE)";
        auto result = executor_->execute(sql);
        EXPECT_TRUE(result.success()) << "Failed to create table: " << result.message();
    }

    void insertTestData() {
        // 插入一些测试数据
        std::vector<std::string> inserts = {
            "INSERT INTO users VALUES (1, 'Alice', 25, 50000.00)",
            "INSERT INTO users VALUES (2, 'Bob', 30, 60000.00)",
            "INSERT INTO users VALUES (3, 'Charlie', 35, 75000.00)"
        };

        for (const auto& sql : inserts) {
            auto result = executor_->execute(sql);
            EXPECT_TRUE(result.success()) << "Failed to insert: " << result.message();
        }
    }

    // 辅助函数：解析 SQL 并返回 AST
    std::unique_ptr<AST> parseSQL(const std::string& sql) {
        Parser parser(sql);
        return parser.parse();
    }

    std::unique_ptr<StorageEngine> storage_engine_;
    std::unique_ptr<Executor> executor_;
};

// 测试UPDATE语句解析
TEST_F(UpdateDeleteTest, UpdateStmtParsing) {
    std::string sql = "UPDATE users SET age = 30 WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    EXPECT_EQ(update_stmt->table(), "users");
    EXPECT_EQ(update_stmt->assignments().size(), 1);
    EXPECT_EQ(update_stmt->assignments()[0].first, "age");
    EXPECT_NE(update_stmt->whereCondition(), nullptr);
}

// 测试DELETE语句解析
TEST_F(UpdateDeleteTest, DeleteStmtParsing) {
    std::string sql = "DELETE FROM users WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* delete_stmt = dynamic_cast<DeleteStmt*>(ast->statement());
    ASSERT_NE(delete_stmt, nullptr);

    EXPECT_EQ(delete_stmt->table(), "users");
    EXPECT_NE(delete_stmt->whereCondition(), nullptr);
}

// 测试UPDATE多列设置
TEST_F(UpdateDeleteTest, UpdateMultipleColumns) {
    std::string sql = "UPDATE users SET age = 40, salary = 80000.00 WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    EXPECT_EQ(update_stmt->assignments().size(), 2);
    EXPECT_EQ(update_stmt->assignments()[0].first, "age");
    EXPECT_EQ(update_stmt->assignments()[1].first, "salary");
}

// 测试DELETE没有WHERE条件
TEST_F(UpdateDeleteTest, DeleteWithoutWhere) {
    std::string sql = "DELETE FROM users";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* delete_stmt = dynamic_cast<DeleteStmt*>(ast->statement());
    ASSERT_NE(delete_stmt, nullptr);

    EXPECT_EQ(delete_stmt->table(), "users");
    EXPECT_EQ(delete_stmt->whereCondition(), nullptr);
}

// 测试UPDATE没有WHERE条件
TEST_F(UpdateDeleteTest, UpdateWithoutWhere) {
    std::string sql = "UPDATE users SET age = 50";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    EXPECT_EQ(update_stmt->table(), "users");
    EXPECT_EQ(update_stmt->whereCondition(), nullptr);
    EXPECT_EQ(update_stmt->assignments().size(), 1);
}

// 测试UPDATE语句执行并验证数据持久化
TEST_F(UpdateDeleteTest, UpdateExecution) {
    // 先查询原始数据
    auto result = executor_->execute("SELECT * FROM users WHERE id = 1");
    EXPECT_TRUE(result.success());
    std::string before = result.message();
    EXPECT_NE(before.find("25"), std::string::npos);  // Alice 初始 age 是 25

    // 执行 UPDATE
    result = executor_->execute("UPDATE users SET age = 100 WHERE id = 1");
    EXPECT_TRUE(result.success());

    // 验证更新后的数据
    result = executor_->execute("SELECT * FROM users WHERE id = 1");
    EXPECT_TRUE(result.success());
    std::string after = result.message();
    EXPECT_NE(after.find("100"), std::string::npos);  // age 应该变为 100
    EXPECT_EQ(after.find("25"), std::string::npos);   // 不应该再看到 25
}

// 测试DELETE语句执行
TEST_F(UpdateDeleteTest, DeleteExecution) {
    std::string sql = "DELETE FROM users WHERE id = 1";

    auto result = executor_->execute(sql);
    EXPECT_TRUE(result.success());
}

// 测试UPDATE带有复杂WHERE条件
TEST_F(UpdateDeleteTest, UpdateWithComplexWhere) {
    // 使用简单的赋值和复杂的 WHERE 条件
    std::string sql = "UPDATE users SET salary = 55000 WHERE age > 25";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    EXPECT_EQ(update_stmt->table(), "users");
    EXPECT_NE(update_stmt->whereCondition(), nullptr);
    EXPECT_EQ(update_stmt->assignments().size(), 1);
}

// 测试DELETE带有复杂WHERE条件
TEST_F(UpdateDeleteTest, DeleteWithComplexWhere) {
    std::string sql = "DELETE FROM users WHERE age >= 30 AND salary < 70000";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* delete_stmt = dynamic_cast<DeleteStmt*>(ast->statement());
    ASSERT_NE(delete_stmt, nullptr);

    EXPECT_EQ(delete_stmt->table(), "users");
    EXPECT_NE(delete_stmt->whereCondition(), nullptr);
}

// 测试UPDATE返回字符串表示
TEST_F(UpdateDeleteTest, UpdateToString) {
    std::string sql = "UPDATE users SET age = 30, name = 'Updated' WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    std::string str = update_stmt->toString();
    EXPECT_NE(str.find("UPDATE"), std::string::npos);
    EXPECT_NE(str.find("users"), std::string::npos);
    EXPECT_NE(str.find("age"), std::string::npos);
    EXPECT_NE(str.find("WHERE"), std::string::npos);
}

// 测试DELETE返回字符串表示
TEST_F(UpdateDeleteTest, DeleteToString) {
    std::string sql = "DELETE FROM users WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* delete_stmt = dynamic_cast<DeleteStmt*>(ast->statement());
    ASSERT_NE(delete_stmt, nullptr);

    std::string str = delete_stmt->toString();
    EXPECT_NE(str.find("DELETE FROM"), std::string::npos);
    EXPECT_NE(str.find("users"), std::string::npos);
    EXPECT_NE(str.find("WHERE"), std::string::npos);
}

// 测试UPDATE不存在的表
TEST_F(UpdateDeleteTest, UpdateNonExistentTable) {
    std::string sql = "UPDATE nonexistent SET age = 30 WHERE id = 1";

    auto result = executor_->execute(sql);
    // 应该失败或报告0行受影响
    EXPECT_FALSE(result.success());
}

// 测试DELETE不存在的表
TEST_F(UpdateDeleteTest, DeleteNonExistentTable) {
    std::string sql = "DELETE FROM nonexistent WHERE id = 1";

    auto result = executor_->execute(sql);
    // 应该失败或报告0行受影响
    EXPECT_FALSE(result.success());
}

// 测试UPDATE不存在的行
TEST_F(UpdateDeleteTest, UpdateNonExistentRow) {
    std::string sql = "UPDATE users SET age = 30 WHERE id = 999";

    auto result = executor_->execute(sql);
    EXPECT_TRUE(result.success());
}

// 测试DELETE不存在的行
TEST_F(UpdateDeleteTest, DeleteNonExistentRow) {
    std::string sql = "DELETE FROM users WHERE id = 999";

    auto result = executor_->execute(sql);
    EXPECT_TRUE(result.success());
}

// 测试UPDATE带有字符串值
TEST_F(UpdateDeleteTest, UpdateWithStringValue) {
    std::string sql = "UPDATE users SET name = 'NewName' WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    EXPECT_EQ(update_stmt->assignments().size(), 1);
    EXPECT_EQ(update_stmt->assignments()[0].first, "name");
}

// 测试UPDATE带有算术表达式
TEST_F(UpdateDeleteTest, UpdateWithArithmeticExpression) {
    std::string sql = "UPDATE users SET age = age + 1 WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    EXPECT_EQ(update_stmt->assignments().size(), 1);
    EXPECT_EQ(update_stmt->assignments()[0].first, "age");
}

// 测试事务中的UPDATE
TEST_F(UpdateDeleteTest, UpdateInTransaction) {
    // 开始事务
    auto result = executor_->execute("BEGIN");
    EXPECT_TRUE(result.success());

    // 在事务中执行UPDATE
    result = executor_->execute("UPDATE users SET age = 99 WHERE id = 1");
    EXPECT_TRUE(result.success());

    // 提交事务
    result = executor_->execute("COMMIT");
    EXPECT_TRUE(result.success());
}

// 测试事务中的DELETE
TEST_F(UpdateDeleteTest, DeleteInTransaction) {
    // 开始事务
    auto result = executor_->execute("BEGIN");
    EXPECT_TRUE(result.success());

    // 在事务中执行DELETE
    result = executor_->execute("DELETE FROM users WHERE id = 2");
    EXPECT_TRUE(result.success());

    // 回滚事务
    result = executor_->execute("ROLLBACK");
    EXPECT_TRUE(result.success());
}

// 测试UPDATE语句类型（通过 dynamic_cast）
TEST_F(UpdateDeleteTest, UpdateStmtType) {
    std::string sql = "UPDATE users SET age = 30 WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    // 通过 dynamic_cast 验证类型
    auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
    ASSERT_NE(update_stmt, nullptr);

    // 验证不是其他类型
    EXPECT_EQ(dynamic_cast<DeleteStmt*>(ast->statement()), nullptr);
    EXPECT_EQ(dynamic_cast<SelectStmt*>(ast->statement()), nullptr);
}

// 测试DELETE语句类型（通过 dynamic_cast）
TEST_F(UpdateDeleteTest, DeleteStmtType) {
    std::string sql = "DELETE FROM users WHERE id = 1";

    auto ast = parseSQL(sql);
    ASSERT_NE(ast, nullptr);

    // 通过 dynamic_cast 验证类型
    auto* delete_stmt = dynamic_cast<DeleteStmt*>(ast->statement());
    ASSERT_NE(delete_stmt, nullptr);

    // 验证不是其他类型
    EXPECT_EQ(dynamic_cast<UpdateStmt*>(ast->statement()), nullptr);
    EXPECT_EQ(dynamic_cast<SelectStmt*>(ast->statement()), nullptr);
}

// 测试带有比较条件的UPDATE
TEST_F(UpdateDeleteTest, UpdateWithComparison) {
    std::vector<std::string> conditions = {
        "UPDATE users SET age = 30 WHERE id = 1",
        "UPDATE users SET age = 30 WHERE id > 1",
        "UPDATE users SET age = 30 WHERE id < 3",
        "UPDATE users SET age = 30 WHERE id >= 1",
        "UPDATE users SET age = 30 WHERE id <= 2",
        "UPDATE users SET age = 30 WHERE id != 1"
    };

    for (const auto& sql : conditions) {
        auto ast = parseSQL(sql);
        EXPECT_NE(ast, nullptr) << "Failed to parse: " << sql;

        auto* update_stmt = dynamic_cast<UpdateStmt*>(ast->statement());
        ASSERT_NE(update_stmt, nullptr);
        EXPECT_NE(update_stmt->whereCondition(), nullptr);
    }
}

// 测试带有逻辑条件的DELETE
TEST_F(UpdateDeleteTest, DeleteWithLogicalConditions) {
    std::vector<std::string> conditions = {
        "DELETE FROM users WHERE id = 1",
        "DELETE FROM users WHERE id > 1 AND age < 35",
        "DELETE FROM users WHERE age >= 25 OR salary > 55000",
        "DELETE FROM users WHERE NOT id = 1"
    };

    for (const auto& sql : conditions) {
        auto ast = parseSQL(sql);
        EXPECT_NE(ast, nullptr) << "Failed to parse: " << sql;

        auto* delete_stmt = dynamic_cast<DeleteStmt*>(ast->statement());
        ASSERT_NE(delete_stmt, nullptr);
    }
}
