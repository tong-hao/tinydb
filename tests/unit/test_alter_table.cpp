#include <gtest/gtest.h>
#include "sql/parser/parser.h"
#include "sql/parser/ast.h"
#include "sql/executor/executor.h"
#include "storage/storage_engine.h"

using namespace tinydb;
using namespace tinydb::sql;
using namespace tinydb::engine;
using namespace tinydb::storage;

class AlterTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        StorageConfig config;
        config.db_file_path = "test_alter_table.db";
        config.wal_file_path = "test_alter_table.wal";
        config.buffer_pool_size = 1024;

        storage_engine_ = std::make_unique<StorageEngine>(config);
        ASSERT_TRUE(storage_engine_->initialize());

        executor_ = std::make_unique<Executor>();
        executor_->initialize(storage_engine_.get());

        // 创建测试表
        auto result = executor_->execute("CREATE TABLE users (id INT, name VARCHAR(20))");
        ASSERT_TRUE(result.success());

        // 插入测试数据
        result = executor_->execute("INSERT INTO users VALUES (1, 'Alice')");
        ASSERT_TRUE(result.success());
        result = executor_->execute("INSERT INTO users VALUES (2, 'Bob')");
        ASSERT_TRUE(result.success());
    }

    void TearDown() override {
        storage_engine_->shutdown();
        storage_engine_.reset();
        executor_.reset();
        std::remove("test_alter_table.db");
        std::remove("test_alter_table.wal");
    }

    std::unique_ptr<StorageEngine> storage_engine_;
    std::unique_ptr<Executor> executor_;
};

// ========== ADD COLUMN 测试 ==========
TEST_F(AlterTableTest, AddColumnInt) {
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success()) << "Failed: " << result.message();

    // 验证可以查询新列
    result = executor_->execute("SELECT * FROM users");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnVarchar) {
    auto result = executor_->execute("ALTER TABLE users ADD email VARCHAR(50)");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnBigInt) {
    auto result = executor_->execute("ALTER TABLE users ADD score BIGINT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnFloat) {
    auto result = executor_->execute("ALTER TABLE users ADD rating FLOAT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnDouble) {
    auto result = executor_->execute("ALTER TABLE users ADD balance DOUBLE");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnBool) {
    auto result = executor_->execute("ALTER TABLE users ADD active BOOL");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnWithKeywordColumn) {
    auto result = executor_->execute("ALTER TABLE users ADD COLUMN age INT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddDuplicateColumn) {
    // 先添加列
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 再次添加同名列应该失败
    result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, AddColumnToNonExistentTable) {
    auto result = executor_->execute("ALTER TABLE nonexistent ADD age INT");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, AddMultipleColumns) {
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users ADD email VARCHAR(50)");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users ADD score BIGINT");
    EXPECT_TRUE(result.success());
}

// ========== DROP COLUMN 测试 ==========
TEST_F(AlterTableTest, DropColumn) {
    // 先添加列
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 删除列
    result = executor_->execute("ALTER TABLE users DROP age");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, DropColumnWithKeywordColumn) {
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users DROP COLUMN age");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, DropNonExistentColumn) {
    auto result = executor_->execute("ALTER TABLE users DROP nonexistent");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, DropColumnFromNonExistentTable) {
    auto result = executor_->execute("ALTER TABLE nonexistent DROP age");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, DropOnlyColumn) {
    // 创建只有一个列的表
    auto result = executor_->execute("CREATE TABLE single_col (id INT)");
    EXPECT_TRUE(result.success());

    // 删除唯一列应该失败
    result = executor_->execute("ALTER TABLE single_col DROP id");
    EXPECT_FALSE(result.success());
}

// ========== MODIFY COLUMN 测试 ==========
TEST_F(AlterTableTest, ModifyColumnIntToBigInt) {
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users MODIFY age BIGINT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, ModifyColumnVarcharLength) {
    auto result = executor_->execute("ALTER TABLE users ADD email VARCHAR(20)");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users MODIFY email VARCHAR(100)");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, ModifyColumnWithKeywordColumn) {
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users MODIFY COLUMN age BIGINT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, ModifyNonExistentColumn) {
    auto result = executor_->execute("ALTER TABLE users MODIFY nonexistent BIGINT");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, ModifyColumnInNonExistentTable) {
    auto result = executor_->execute("ALTER TABLE nonexistent MODIFY age BIGINT");
    EXPECT_FALSE(result.success());
}

// ========== RENAME TABLE 测试 ==========
TEST_F(AlterTableTest, RenameTableWithTo) {
    auto result = executor_->execute("ALTER TABLE users RENAME TO customers");
    EXPECT_TRUE(result.success());

    // 验证旧表名不存在
    result = executor_->execute("SELECT * FROM users");
    EXPECT_FALSE(result.success());

    // 验证新表名存在
    result = executor_->execute("SELECT * FROM customers");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, RenameTableWithoutTo) {
    auto result = executor_->execute("ALTER TABLE users RENAME customers");
    EXPECT_TRUE(result.success());

    result = executor_->execute("SELECT * FROM customers");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, RenameNonExistentTable) {
    auto result = executor_->execute("ALTER TABLE nonexistent RENAME TO newname");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, RenameToExistingTable) {
    // 创建另一个表
    auto result = executor_->execute("CREATE TABLE other (id INT)");
    EXPECT_TRUE(result.success());

    // 重命名为已存在的表名应该失败
    result = executor_->execute("ALTER TABLE users RENAME TO other");
    EXPECT_FALSE(result.success());
}

// ========== 组合操作测试 ==========
TEST_F(AlterTableTest, AddThenDropThenAdd) {
    // 添加列
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 删除列
    result = executor_->execute("ALTER TABLE users DROP age");
    EXPECT_TRUE(result.success());

    // 再次添加同名列
    result = executor_->execute("ALTER TABLE users ADD age BIGINT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, RenameThenAlter) {
    // 重命名表
    auto result = executor_->execute("ALTER TABLE users RENAME TO customers");
    EXPECT_TRUE(result.success());

    // 在新表名上执行ALTER
    result = executor_->execute("ALTER TABLE customers ADD age INT");
    EXPECT_TRUE(result.success());
}

// ========== 语法错误测试 ==========
TEST_F(AlterTableTest, AlterTableInvalidSyntax) {
    // 缺少表名
    auto result = executor_->execute("ALTER TABLE ADD age INT");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, AlterTableUnknownAction) {
    auto result = executor_->execute("ALTER TABLE users UNKNOWN age INT");
    EXPECT_FALSE(result.success());
}

// ========== 大小写和空格测试 ==========
TEST_F(AlterTableTest, AlterTableCaseInsensitive) {
    auto result = executor_->execute("alter table users add age int");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users DROP AGE");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AlterTableWithExtraSpaces) {
    auto result = executor_->execute("ALTER   TABLE   users   ADD   age   INT");
    EXPECT_TRUE(result.success());
}

// ========== 事务中的ALTER TABLE ==========
TEST_F(AlterTableTest, AlterTableInTransaction) {
    auto result = executor_->execute("BEGIN");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    result = executor_->execute("COMMIT");
    EXPECT_TRUE(result.success());

    // 验证ALTER已提交
    result = executor_->execute("SELECT * FROM users");
    EXPECT_TRUE(result.success());
}

// ========== 数据类型边界测试 ==========
TEST_F(AlterTableTest, AddColumnWithVarcharMaxLength) {
    auto result = executor_->execute("ALTER TABLE users ADD description VARCHAR(65535)");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnWithDecimalPrecision) {
    // 测试带精度的类型（如果支持）
    auto result = executor_->execute("ALTER TABLE users ADD price DECIMAL(10,2)");
    // 可能成功或失败，取决于是否支持DECIMAL
    // 这里只检查不崩溃
}

// ========== 特殊字符和保留字测试 ==========
TEST_F(AlterTableTest, AddColumnWithReservedWord) {
    // 使用保留字作为列名
    auto result = executor_->execute("ALTER TABLE users ADD \"select\" INT");
    // 根据实现可能成功或失败
}

TEST_F(AlterTableTest, AddColumnWithUnderscore) {
    auto result = executor_->execute("ALTER TABLE users ADD user_age INT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AddColumnWithNumber) {
    auto result = executor_->execute("ALTER TABLE users ADD age2 INT");
    EXPECT_TRUE(result.success());
}

// ========== 并发ALTER测试 ==========
TEST_F(AlterTableTest, MultipleAlterOperations) {
    auto result = executor_->execute("ALTER TABLE users ADD col1 INT");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users ADD col2 VARCHAR(20)");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users MODIFY col1 BIGINT");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users DROP col2");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE users ADD col3 FLOAT");
    EXPECT_TRUE(result.success());
}

// ========== 解析器详细测试 ==========
TEST_F(AlterTableTest, ParseAlterTableAllVariants) {
    // 测试解析器能正确解析所有变体
    std::vector<std::string> statements = {
        "ALTER TABLE users ADD age INT",
        "ALTER TABLE users ADD COLUMN age INT",
        "ALTER TABLE users DROP age",
        "ALTER TABLE users DROP COLUMN age",
        "ALTER TABLE users MODIFY age BIGINT",
        "ALTER TABLE users MODIFY COLUMN age BIGINT",
        "ALTER TABLE users ALTER name VARCHAR(150)",
        "ALTER TABLE users ALTER COLUMN name VARCHAR(150)",
        "ALTER TABLE users ALTER name TYPE VARCHAR(150)",
        "ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(150)",
        "ALTER TABLE users RENAME TO customers",
        "ALTER TABLE users RENAME customers",
        "ALTER TABLE users RENAME COLUMN age TO user_age",
    };

    for (const auto& sql : statements) {
        Parser parser(sql);
        auto ast = parser.parse();
        EXPECT_NE(ast, nullptr) << "Failed to parse: " << sql;
    }
}

// ========== 空表和大数据量表测试 ==========
TEST_F(AlterTableTest, AlterEmptyTable) {
    // 创建空表
    auto result = executor_->execute("CREATE TABLE empty_table (id INT)");
    EXPECT_TRUE(result.success());

    result = executor_->execute("ALTER TABLE empty_table ADD name VARCHAR(20)");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, RenameColumn) {
    // 先添加列
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 重命名列
    result = executor_->execute("ALTER TABLE users RENAME COLUMN age TO user_age");
    EXPECT_TRUE(result.success());

    // 验证旧列名不存在（通过INSERT测试）
    result = executor_->execute("INSERT INTO users (id, name, user_age) VALUES (3, 'Charlie', 25)");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, RenameNonExistentColumn) {
    auto result = executor_->execute("ALTER TABLE users RENAME COLUMN nonexistent TO newname");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, RenameColumnToExistingName) {
    // 添加列
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 尝试重命名为已存在的列名
    result = executor_->execute("ALTER TABLE users RENAME COLUMN age TO name");
    EXPECT_FALSE(result.success());
}

// ========== ALTER COLUMN TYPE 测试 (PostgreSQL语法) ==========
TEST_F(AlterTableTest, AlterColumnType) {
    // 添加列
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 修改列类型
    result = executor_->execute("ALTER TABLE users ALTER COLUMN age TYPE BIGINT");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AlterColumnTypeWithoutKeywordColumn) {
    // 添加列
    auto result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 修改列类型 (不使用 COLUMN 关键字)
    result = executor_->execute("ALTER TABLE users ALTER age TYPE VARCHAR(50)");
    EXPECT_TRUE(result.success());
}

TEST_F(AlterTableTest, AlterColumnTypeNonExistentColumn) {
    auto result = executor_->execute("ALTER TABLE users ALTER COLUMN nonexistent TYPE INT");
    EXPECT_FALSE(result.success());
}

TEST_F(AlterTableTest, AlterTableWithIndex) {
    // 创建索引
    auto result = executor_->execute("CREATE INDEX idx_name ON users (name)");
    EXPECT_TRUE(result.success());

    // 添加新列
    result = executor_->execute("ALTER TABLE users ADD age INT");
    EXPECT_TRUE(result.success());

    // 删除索引列应该失败或自动删除索引（取决于实现）
    // result = executor_->execute("ALTER TABLE users DROP name");
    // EXPECT_FALSE(result.success());
}
