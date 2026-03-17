#include <gtest/gtest.h>
#include "sql/ast.h"
#include "sql/lexer.h"
#include "sql/parser.hpp"

using namespace tinydb;
using namespace tinydb::sql;

class LexerTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

    // 辅助函数：解析 SQL 并返回 AST
    std::unique_ptr<AST> parseSQL(const std::string& sql) {
        yyscan_t scanner;
        yylex_init(&scanner);

        YY_BUFFER_STATE buffer = yy_scan_string(sql.c_str(), scanner);

        AST* ast = nullptr;
        yyparse(scanner, &ast);

        yy_delete_buffer(buffer, scanner);
        yylex_destroy(scanner);

        return std::unique_ptr<AST>(ast);
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
