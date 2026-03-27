#include <gtest/gtest.h>
#include "sql/parser/ast.h"
#include "sql/parser/parser.h"

using namespace tinydb;
using namespace tinydb::sql;

class LexerTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

    // 辅助函数：解析 SQL 并返回 SQLParseTree
    std::unique_ptr<SQLParseTree> parseSQL(const std::string& sql) {
        Parser parser(sql);
        return parser.parse();
    }
};

TEST_F(LexerTest, ParseSelectStatement) {
    auto ast = parseSQL("SELECT * FROM users");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto select_stmt = dynamic_cast<SelectStmt*>(ast->statement());
    ASSERT_NE(select_stmt, nullptr);
    EXPECT_EQ(select_stmt->fromTable(), "users");
}

TEST_F(LexerTest, ParseSelectWithColumns) {
    auto ast = parseSQL("SELECT id, name FROM users");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto select_stmt = dynamic_cast<SelectStmt*>(ast->statement());
    ASSERT_NE(select_stmt, nullptr);
    EXPECT_EQ(select_stmt->fromTable(), "users");
}

TEST_F(LexerTest, ParseCreateTable) {
    auto ast = parseSQL("CREATE TABLE users (id INT, name VARCHAR(32))");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto create_stmt = dynamic_cast<CreateTableStmt*>(ast->statement());
    ASSERT_NE(create_stmt, nullptr);
    EXPECT_EQ(create_stmt->table(), "users");
}

TEST_F(LexerTest, ParseDropTable) {
    auto ast = parseSQL("DROP TABLE users");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto drop_stmt = dynamic_cast<DropTableStmt*>(ast->statement());
    ASSERT_NE(drop_stmt, nullptr);
    EXPECT_EQ(drop_stmt->table(), "users");
}

TEST_F(LexerTest, ParseInsert) {
    auto ast = parseSQL("INSERT INTO users VALUES (1, 'Alice')");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto insert_stmt = dynamic_cast<InsertStmt*>(ast->statement());
    ASSERT_NE(insert_stmt, nullptr);
    EXPECT_EQ(insert_stmt->table(), "users");
}

TEST_F(LexerTest, ParseInsertWithColumns) {
    auto ast = parseSQL("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto insert_stmt = dynamic_cast<InsertStmt*>(ast->statement());
    ASSERT_NE(insert_stmt, nullptr);
    EXPECT_EQ(insert_stmt->table(), "users");
}

TEST_F(LexerTest, CaseInsensitiveKeywords) {
    auto ast1 = parseSQL("select * from users");
    ASSERT_NE(ast1, nullptr);

    auto ast2 = parseSQL("SELECT * FROM users");
    ASSERT_NE(ast2, nullptr);

    auto ast3 = parseSQL("Select * From users");
    ASSERT_NE(ast3, nullptr);
}

TEST_F(LexerTest, SQLWithSemicolon) {
    auto ast = parseSQL("SELECT * FROM users;");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);
}

// ALTER TABLE 测试
TEST_F(LexerTest, ParseAlterTableAddColumn) {
    auto ast = parseSQL("ALTER TABLE users ADD age INT");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto alter_stmt = dynamic_cast<AlterTableStmt*>(ast->statement());
    ASSERT_NE(alter_stmt, nullptr);
    EXPECT_EQ(alter_stmt->table(), "users");
    EXPECT_EQ(alter_stmt->action(), AlterTableStmt::ActionType::ADD_COLUMN);
    EXPECT_EQ(alter_stmt->columnName(), "age");
}

TEST_F(LexerTest, ParseAlterTableDropColumn) {
    auto ast = parseSQL("ALTER TABLE users DROP age");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto alter_stmt = dynamic_cast<AlterTableStmt*>(ast->statement());
    ASSERT_NE(alter_stmt, nullptr);
    EXPECT_EQ(alter_stmt->table(), "users");
    EXPECT_EQ(alter_stmt->action(), AlterTableStmt::ActionType::DROP_COLUMN);
    EXPECT_EQ(alter_stmt->columnName(), "age");
}

TEST_F(LexerTest, ParseAlterTableModifyColumn) {
    auto ast = parseSQL("ALTER TABLE users MODIFY age BIGINT");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto alter_stmt = dynamic_cast<AlterTableStmt*>(ast->statement());
    ASSERT_NE(alter_stmt, nullptr);
    EXPECT_EQ(alter_stmt->table(), "users");
    EXPECT_EQ(alter_stmt->action(), AlterTableStmt::ActionType::MODIFY_COLUMN);
    EXPECT_EQ(alter_stmt->columnName(), "age");
}

TEST_F(LexerTest, ParseAlterTableRenameTable) {
    auto ast = parseSQL("ALTER TABLE users RENAME TO customers");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto alter_stmt = dynamic_cast<AlterTableStmt*>(ast->statement());
    ASSERT_NE(alter_stmt, nullptr);
    EXPECT_EQ(alter_stmt->table(), "users");
    EXPECT_EQ(alter_stmt->action(), AlterTableStmt::ActionType::RENAME_TABLE);
    EXPECT_EQ(alter_stmt->newTableName(), "customers");
}

TEST_F(LexerTest, ParseAlterTableAlterColumnType) {
    auto ast = parseSQL("ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(150)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto alter_stmt = dynamic_cast<AlterTableStmt*>(ast->statement());
    ASSERT_NE(alter_stmt, nullptr);
    EXPECT_EQ(alter_stmt->table(), "users");
    EXPECT_EQ(alter_stmt->action(), AlterTableStmt::ActionType::ALTER_COLUMN_TYPE);
    EXPECT_EQ(alter_stmt->columnName(), "name");
    EXPECT_EQ(alter_stmt->columnType(), "VARCHAR(150)");
}

TEST_F(LexerTest, ParseAlterTableRenameColumn) {
    auto ast = parseSQL("ALTER TABLE users RENAME COLUMN age TO user_age");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto alter_stmt = dynamic_cast<AlterTableStmt*>(ast->statement());
    ASSERT_NE(alter_stmt, nullptr);
    EXPECT_EQ(alter_stmt->table(), "users");
    EXPECT_EQ(alter_stmt->action(), AlterTableStmt::ActionType::RENAME_COLUMN);
    EXPECT_EQ(alter_stmt->columnName(), "age");
    EXPECT_EQ(alter_stmt->newColumnName(), "user_age");
}

// CREATE INDEX 测试
TEST_F(LexerTest, ParseCreateIndex) {
    auto ast = parseSQL("CREATE INDEX idx_name ON users (name)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto index_stmt = dynamic_cast<CreateIndexStmt*>(ast->statement());
    ASSERT_NE(index_stmt, nullptr);
    EXPECT_EQ(index_stmt->indexName(), "idx_name");
    EXPECT_EQ(index_stmt->tableName(), "users");
    EXPECT_EQ(index_stmt->columnName(), "name");
    EXPECT_FALSE(index_stmt->isUnique());
}

TEST_F(LexerTest, ParseCreateUniqueIndex) {
    auto ast = parseSQL("CREATE UNIQUE INDEX idx_name ON users (name)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto index_stmt = dynamic_cast<CreateIndexStmt*>(ast->statement());
    ASSERT_NE(index_stmt, nullptr);
    EXPECT_EQ(index_stmt->indexName(), "idx_name");
    EXPECT_TRUE(index_stmt->isUnique());
}

// DROP INDEX 测试
TEST_F(LexerTest, ParseDropIndex) {
    auto ast = parseSQL("DROP INDEX idx_name");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto drop_stmt = dynamic_cast<DropIndexStmt*>(ast->statement());
    ASSERT_NE(drop_stmt, nullptr);
    EXPECT_EQ(drop_stmt->indexName(), "idx_name");
}

// BEGIN/COMMIT/ROLLBACK 测试
TEST_F(LexerTest, ParseBegin) {
    auto ast = parseSQL("BEGIN");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto begin_stmt = dynamic_cast<BeginStmt*>(ast->statement());
    ASSERT_NE(begin_stmt, nullptr);
}

TEST_F(LexerTest, ParseCommit) {
    auto ast = parseSQL("COMMIT");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto commit_stmt = dynamic_cast<CommitStmt*>(ast->statement());
    ASSERT_NE(commit_stmt, nullptr);
}

TEST_F(LexerTest, ParseRollback) {
    auto ast = parseSQL("ROLLBACK");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto rollback_stmt = dynamic_cast<RollbackStmt*>(ast->statement());
    ASSERT_NE(rollback_stmt, nullptr);
}

// ANALYZE 测试
TEST_F(LexerTest, ParseAnalyze) {
    auto ast = parseSQL("ANALYZE users");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto analyze_stmt = dynamic_cast<AnalyzeStmt*>(ast->statement());
    ASSERT_NE(analyze_stmt, nullptr);
    EXPECT_EQ(analyze_stmt->tableName(), "users");
}

// EXPLAIN 测试
TEST_F(LexerTest, ParseExplain) {
    auto ast = parseSQL("EXPLAIN SELECT * FROM users");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto explain_stmt = dynamic_cast<ExplainStmt*>(ast->statement());
    ASSERT_NE(explain_stmt, nullptr);

    // Verify inner statement is properly set
    auto inner_stmt = explain_stmt->innerStatement();
    ASSERT_NE(inner_stmt, nullptr);

    auto select_stmt = dynamic_cast<SelectStmt*>(inner_stmt);
    ASSERT_NE(select_stmt, nullptr);
    EXPECT_EQ(select_stmt->fromTable(), "users");
}

// CREATE VIEW 测试
TEST_F(LexerTest, ParseCreateView) {
    auto ast = parseSQL("CREATE VIEW v_sales_summary AS SELECT product, quantity FROM sales");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto create_view_stmt = dynamic_cast<CreateViewStmt*>(ast->statement());
    ASSERT_NE(create_view_stmt, nullptr);
    EXPECT_EQ(create_view_stmt->viewName(), "v_sales_summary");
    ASSERT_NE(create_view_stmt->selectStmt(), nullptr);
    EXPECT_EQ(create_view_stmt->selectStmt()->fromTable(), "sales");
}

TEST_F(LexerTest, ParseCreateViewWithExpression) {
    // Test CREATE VIEW with multiplication expression (quantity * price)
    auto ast = parseSQL("CREATE VIEW v_sales_summary AS SELECT product, quantity, quantity * price as total_amount FROM sales");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto create_view_stmt = dynamic_cast<CreateViewStmt*>(ast->statement());
    ASSERT_NE(create_view_stmt, nullptr);
    EXPECT_EQ(create_view_stmt->viewName(), "v_sales_summary");
    ASSERT_NE(create_view_stmt->selectStmt(), nullptr);
}

// Test multi-row INSERT with NULL values
TEST_F(LexerTest, ParseMultiRowInsertWithNull) {
    auto ast = parseSQL("INSERT INTO test_expr VALUES (1, 10), (2, 20), (3, NULL)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto insert_stmt = dynamic_cast<InsertStmt*>(ast->statement());
    ASSERT_NE(insert_stmt, nullptr);
    EXPECT_EQ(insert_stmt->table(), "test_expr");
}

TEST_F(LexerTest, ParseSingleRowInsertWithNull) {
    auto ast = parseSQL("INSERT INTO test_expr VALUES (1, NULL)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto insert_stmt = dynamic_cast<InsertStmt*>(ast->statement());
    ASSERT_NE(insert_stmt, nullptr);
    EXPECT_EQ(insert_stmt->table(), "test_expr");
}

TEST_F(LexerTest, ParseSelectDistinct) {
    auto ast = parseSQL("SELECT DISTINCT department FROM employees");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto select_stmt = dynamic_cast<SelectStmt*>(ast->statement());
    ASSERT_NE(select_stmt, nullptr);
    EXPECT_TRUE(select_stmt->isDistinct());
    EXPECT_EQ(select_stmt->fromTable(), "employees");
}

TEST_F(LexerTest, ParseSelectDistinctStar) {
    auto ast = parseSQL("SELECT DISTINCT * FROM employees");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto select_stmt = dynamic_cast<SelectStmt*>(ast->statement());
    ASSERT_NE(select_stmt, nullptr);
    EXPECT_TRUE(select_stmt->isDistinct());
    EXPECT_EQ(select_stmt->fromTable(), "employees");
}

TEST_F(LexerTest, ParseSelectWithoutDistinct) {
    auto ast = parseSQL("SELECT department FROM employees");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto select_stmt = dynamic_cast<SelectStmt*>(ast->statement());
    ASSERT_NE(select_stmt, nullptr);
    EXPECT_FALSE(select_stmt->isDistinct());
    EXPECT_EQ(select_stmt->fromTable(), "employees");
}

TEST_F(LexerTest, ParseEmptyString) {
    auto ast = parseSQL("INSERT INTO large_values VALUES (3, '', -2147483648)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto insert_stmt = dynamic_cast<InsertStmt*>(ast->statement());
    ASSERT_NE(insert_stmt, nullptr);
    EXPECT_EQ(insert_stmt->table(), "large_values");
}

// Test CREATE TABLE with DEFAULT constraint
TEST_F(LexerTest, ParseCreateTableWithDefault) {
    auto ast = parseSQL("CREATE TABLE test (id INT PRIMARY KEY, age INT DEFAULT 0)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto create_stmt = dynamic_cast<CreateTableStmt*>(ast->statement());
    ASSERT_NE(create_stmt, nullptr);
    EXPECT_EQ(create_stmt->table(), "test");
}

// Test CREATE TABLE with UNIQUE constraint
TEST_F(LexerTest, ParseCreateTableWithUnique) {
    auto ast = parseSQL("CREATE TABLE test (id INT PRIMARY KEY, username VARCHAR(50) UNIQUE NOT NULL)");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto create_stmt = dynamic_cast<CreateTableStmt*>(ast->statement());
    ASSERT_NE(create_stmt, nullptr);
    EXPECT_EQ(create_stmt->table(), "test");
}

// Test CREATE TABLE with CHECK constraint
TEST_F(LexerTest, ParseCreateTableWithCheck) {
    auto ast = parseSQL("CREATE TABLE test (id INT PRIMARY KEY, age INT CHECK (age >= 0))");
    ASSERT_NE(ast, nullptr);
    ASSERT_NE(ast->statement(), nullptr);

    auto create_stmt = dynamic_cast<CreateTableStmt*>(ast->statement());
    ASSERT_NE(create_stmt, nullptr);
    EXPECT_EQ(create_stmt->table(), "test");
}

